# config/config.py

# CSV / paths
CSV_FILE = "/opt/airflow/work-dir/data/historical-data/AAPL.csv"
CSV_URL = "https://raw.githubusercontent.com/md1509/Apple_historical_data/main/AAPL.csv"
CHUNK_DIR = "/opt/airflow/work-dir/data/monthly-chunks/"

# MinIO / S3
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "aapl-datalake"

# PostgreSQL
DB_HOST = "postgres-warehouse"
DB_NAME = "stockdb"
DB_USER = "stock"
DB_PASSWORD = "stock"
MONTHLY_INGESTIONS_TABLE= "monthly_ingestions"
