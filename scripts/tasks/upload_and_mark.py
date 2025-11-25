import os
from datetime import datetime
import psycopg2
from utils.file_helpers import md5_of_file
from utils.s3_helpers import s3_client, object_exists_in_bucket
from config import BUCKET_NAME, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, MONTHLY_INGESTIONS_TABLE

def upload_and_mark_processed_func(ti):
    year_month = ti.xcom_pull(task_ids='extract_next_month_data_chunk', key='year_month')
    chunk_path = ti.xcom_pull(task_ids='extract_next_month_data_chunk', key='chunk_path')

    if not year_month or not chunk_path:
        print("✓ Nothing to upload/mark (no next month).")
        return

    if not os.path.exists(chunk_path):
        raise FileNotFoundError(f"Chunk file not found at {chunk_path}")

    year, month = map(int, year_month.split('-'))
    checksum = md5_of_file(chunk_path)

    # ===== DB connection with context managers =====
    with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD) as conn:
        with conn.cursor() as cur:
            # Check existing checksum
            cur.execute(
                f"SELECT checksum FROM {MONTHLY_INGESTIONS_TABLE} WHERE year=%s AND month=%s",
                (year, month)
            )
            existing_checksum = cur.fetchone()

            if existing_checksum and existing_checksum[0] == checksum:
                print(f"✓ Month {year}-{month:02d} already processed with same checksum — skipping DB update and upload.")
                return

            # Insert/update processed record
            now = datetime.utcnow()
            cur.execute(f"""
                INSERT INTO {MONTHLY_INGESTIONS_TABLE} (year, month, checksum, processed_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (year, month) DO UPDATE
                SET checksum = EXCLUDED.checksum,
                    processed_date = EXCLUDED.processed_date
            """, (year, month, checksum, now))
    # Connection and cursor are automatically closed/committed here

    # ===== S3 upload logic (unchanged) =====
    s3 = s3_client()
    existing_buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
    if BUCKET_NAME not in existing_buckets:
        print(f"→ Creating bucket {BUCKET_NAME}")
        s3.create_bucket(Bucket=BUCKET_NAME)

    key = f"raw/{year_month}.csv"

    if object_exists_in_bucket(s3, BUCKET_NAME, key):
        print(f"✓ Object {key} already exists — skipping upload.")
        uploaded = False
    else:
        print(f"→ Uploading {chunk_path} to s3://{BUCKET_NAME}/{key} ...")
        s3.upload_file(chunk_path, BUCKET_NAME, key)
        uploaded = True
        print(f"✓ Upload successful: s3://{BUCKET_NAME}/{key}")

    print(f"✓ Marked {year_month} as processed. Uploaded={uploaded}")
