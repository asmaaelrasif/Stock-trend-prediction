import sys
import os
import hashlib
import boto3
import psycopg2

from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, when, avg, stddev, year, month, to_timestamp, lit
)

# Load .env file (it should be mounted or copied inside the container)
load_dotenv(dotenv_path="/opt/project/.env")  # adjust path if needed


# -----------------------------
# 1️⃣ Configuration from environment
# -----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("BUCKET_NAME")
RAW_PREFIX = os.getenv("RAW_PREFIX")

POSTGRES_HOST = os.getenv("POSTGRES_WAREHOUSE_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_WAREHOUSE_PORT"))
POSTGRES_DB = os.getenv("POSTGRES_WAREHOUSE_DB")
POSTGRES_USER = os.getenv("POSTGRES_WAREHOUSE_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_WAREHOUSE_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_WAREHOUSE_TABLE")
TRANSFORM_TRACK_TABLE = os.getenv("POSTGRES_WAREHOUSE_TRANSFORM_TRACK_TABLE")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# how many historical rows per ticker to fetch
LOOKBACK_ROWS = 20

# -----------------------------
# 2️⃣ Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("AAPL_Monthly_Transform_With_Lookback") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# -----------------------------
# 3️⃣ List available files in MinIO
# -----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)
objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX)

if "Contents" not in objects:
    print("❌ No files found in raw/ folder.")
    spark.stop()
    sys.exit(0)

raw_files = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".csv")]

# -----------------------------
# 4️⃣ Load already processed files (transform tracker)
# -----------------------------
conn = psycopg2.connect(
    host=POSTGRES_HOST, port=POSTGRES_PORT,
    dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
)
cur = conn.cursor()
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TRANSFORM_TRACK_TABLE} (
        file_name TEXT PRIMARY KEY,
        checksum TEXT NOT NULL,
        processed_date TIMESTAMP NOT NULL DEFAULT NOW()
    );
""")
conn.commit()

cur.execute(f"SELECT file_name, checksum FROM {TRANSFORM_TRACK_TABLE}")
processed = dict(cur.fetchall())
cur.close()
conn.close()

# -----------------------------
# 5️⃣ Helper: fetch lookback rows from Postgres for given tickers
# -----------------------------
def fetch_lookback_df_for_tickers(ticker_list):
    """
    Returns a Spark DataFrame with columns: ticker, date, close (and any other columns present).
    Uses a PostgreSQL query that picks the last LOOKBACK_ROWS rows per ticker.
    If ticker_list is empty returns None.
    """
    if not ticker_list:
        return None

    # sanitize tickers for SQL IN clause (basic escaping - careful with untrusted inputs)
    sanitized = ",".join("'" + t.replace("'", "''") + "'" for t in ticker_list)

    # Subquery selects last LOOKBACK_ROWS per ticker
    subquery = f"""
    (
      SELECT ticker, date, close
      FROM (
        SELECT ticker, date, close,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM {POSTGRES_TABLE}
        WHERE ticker IN ({sanitized})
      ) t
      WHERE rn <= {LOOKBACK_ROWS}
      ORDER BY ticker, date
    ) AS prev_rows
    """

    try:
        prev_df = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", subquery) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .load()
        # Ensure date column is timestamp type for downstream operations
        if "date" in prev_df.columns:
            prev_df = prev_df.withColumn("date", to_timestamp(col("date")))
        return prev_df
    except Exception as e:
        print(f"⚠️ Warning: failed to read lookback rows from Postgres: {e}")
        return None

# -----------------------------
# 6️⃣ Process each new or changed file (with lookback)
# -----------------------------
for key in raw_files:
    file_name = key.split("/")[-1]
    s3_path = f"s3a://{BUCKET_NAME}/{key}"

    # Compute checksum to detect duplicates
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        data = obj["Body"].read()
        checksum = hashlib.md5(data).hexdigest()
    except Exception as e:
        print(f"❌ Failed to read object {key} from MinIO: {e}")
        continue

    if file_name in processed and processed[file_name] == checksum:
        print(f"✓ {file_name} already processed with same checksum — skipping.")
        continue

    print(f"📂 Processing new/updated file: {file_name}")

    try:
        # -----------------------------
        # Load CSV and keep all columns
        # -----------------------------
        df = spark.read.option("header", True).option("inferSchema", True).csv(s3_path)

        # normalize date column to timestamp (helps with window ops)
        if "Date" in df.columns:
            df = df.withColumnRenamed("Date", "date")
        df = df.withColumn("date", to_timestamp(col("date")))

        # Rename other expected columns for consistency (only if present)
        rename_map = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }
        for old, new in rename_map.items():
            if old in df.columns and new not in df.columns:
                df = df.withColumnRenamed(old, new)

        # Ensure ticker column exists; if not, and file contains single-symbol data, attempt to infer
        if "ticker" not in df.columns:
            # if file_name encodes ticker like AAPL_2025-01.csv, try to infer ticker
            inferred_ticker = file_name.split("_")[0] if "_" in file_name else None
            if inferred_ticker:
                df = df.withColumn("ticker", lit(inferred_ticker))
            else:
                raise ValueError("Input CSV missing 'ticker' column and ticker couldn't be inferred from filename.")

        # Ensure date ordering
        df = df.withColumn("year", year("date")).withColumn("month", month("date"))
        df = df.orderBy("date")

        # Collect distinct tickers in this file (small action)
        tickers = [r[0] for r in df.select("ticker").distinct().collect()]
        # Fetch lookback rows for these tickers
        prev_df = fetch_lookback_df_for_tickers(tickers)

        # If prev_df exists and has rows, tag it as history and union with current data
        if prev_df and len(prev_df.columns) > 0 and prev_df.count() > 0:
            # Tag source for the historical rows and make sure schema aligns with current df
            prev_df = prev_df.withColumn("source_file", lit("__history__"))
            # If prev_df missing some columns present in df, unionByName with allowMissingColumns
            combined = prev_df.unionByName(df.withColumn("source_file", lit(file_name)), allowMissingColumns=True)
        else:
            # No history available, just use current df and mark source_file
            combined = df.withColumn("source_file", lit(file_name))

        # Re-order after union
        combined = combined.orderBy("ticker", "date")

        # -----------------------------
        # Compute rolling metrics on the combined DF
        # -----------------------------
        w = Window.partitionBy("ticker").orderBy("date")

        combined = combined.withColumn("prev_close", lag("close").over(w))
        combined = combined.withColumn("daily_return", ((col("close") - col("prev_close")) / col("prev_close")) * 100)

        combined = combined.withColumn(
            "trend_label",
            when(col("daily_return") > 0.1, "Uptrend")
            .when(col("daily_return") < -0.1, "Downtrend")
            .otherwise("Neutral")
        )

        combined = combined.withColumn("MA10", avg("close").over(w.rowsBetween(-9, 0)))
        combined = combined.withColumn("MA20", avg("close").over(w.rowsBetween(-19, 0)))
        combined = combined.withColumn("volatility_10d", stddev("daily_return").over(w.rowsBetween(-9, 0)))

        # -----------------------------
        # Keep only rows that belong to current file (so we don't write the injected history rows)
        # -----------------------------
        result_current = combined.filter(col("source_file") == file_name)

        # Deduplicate before writing (by date + ticker + source_file)
        result_current = result_current.dropDuplicates(["date", "ticker", "source_file"])

        # -----------------------------
        # Write to PostgreSQL (append only new rows)
        # -----------------------------
        result_current.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        # -----------------------------
        # Record transformation metadata
        # -----------------------------
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {TRANSFORM_TRACK_TABLE} (file_name, checksum, processed_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (file_name) DO UPDATE
            SET checksum = EXCLUDED.checksum,
                processed_date = EXCLUDED.processed_date;
        """, (file_name, checksum, datetime.utcnow()))
        conn.commit()
        cur.close()
        conn.close()

        print(f"✓ Transformation completed and recorded for {file_name}")

    except Exception as e:
        # Log and continue with next file; do NOT mark file as processed
        print(f"❌ Error processing {file_name}: {e}")
        continue

# End loop
spark.stop()
print("🏁 All pending raw files processed and recorded.")
