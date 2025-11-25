import os
import pandas as pd
import hashlib
import psycopg2
import time
from config import CSV_FILE, CHUNK_DIR, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, MONTHLY_INGESTIONS_TABLE

def extract_next_month_chunk_func(ti, max_retries=3, retry_delay=5):
    # ===== Retry mechanism for reading CSV =====
    attempt = 0
    while attempt < max_retries:
        try:
            if not os.path.exists(CSV_FILE):
                raise FileNotFoundError(f"CSV file not found at {CSV_FILE}. Run download_full_csv first.")
            df = pd.read_csv(CSV_FILE, dtype={'Date': str})
            break
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                raise e
            print(f"⚠️ Failed to read CSV (attempt {attempt}/{max_retries}): {e}, retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', utc=True)
    df = df.dropna(subset=['Date'])
    df['year'] = df['Date'].dt.year
    df['month'] = df['Date'].dt.month

    # ===== Fetch processed months from DB =====
    with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT year, month FROM {MONTHLY_INGESTIONS_TABLE} ORDER BY year, month")
            processed = cur.fetchall()

    # Determine next month to process
    if not processed:
        next_row = df.sort_values(['year', 'month']).iloc[0]
    else:
        last_year, last_month = processed[-1]
        remaining = df[(df['year'] > last_year) | ((df['year'] == last_year) & (df['month'] > last_month))]
        if remaining.empty:
            print("✓ All months already processed — nothing to do.")
            ti.xcom_push(key='year_month', value=None)
            return None
        next_row = remaining.sort_values(['year', 'month']).iloc[0]

    year, month = int(next_row['year']), int(next_row['month'])
    os.makedirs(CHUNK_DIR, exist_ok=True)
    chunk_filename = f"AAPL_{year}-{month:02d}.csv"
    output_file = os.path.join(CHUNK_DIR, chunk_filename)

    # Skip if chunk already exists
    if os.path.exists(output_file):
        print(f"✓ Chunk file already exists: {output_file} — skipping extraction.")
        ti.xcom_push(key='chunk_path', value=output_file)
        ti.xcom_push(key='year_month', value=f"{year}-{month:02d}")
        return output_file

    chunk = df[(df['year'] == year) & (df['month'] == month)].copy()
    if chunk.empty:
        print(f"⚠️ No rows found for {year}-{month:02d} — skipping.")
        ti.xcom_push(key='year_month', value=None)
        return None

    checksum = hashlib.md5(chunk.to_csv(index=False).encode('utf-8')).hexdigest()

    # ===== Check existing checksum in DB =====
    with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT checksum FROM {MONTHLY_INGESTIONS_TABLE} WHERE year=%s AND month=%s", (year, month))
            existing_checksum = cur.fetchone()

    if existing_checksum and existing_checksum[0] == checksum:
        print(f"✓ Month {year}-{month:02d} already processed with same checksum — skipping extraction.")
        ti.xcom_push(key='year_month', value=None)
        return None

    # Save chunk to file
    chunk.to_csv(output_file, index=False)
    print(f"✓ Saved CSV chunk: {output_file}")
    ti.xcom_push(key='chunk_path', value=output_file)
    ti.xcom_push(key='year_month', value=f"{year}-{month:02d}")
    return output_file
