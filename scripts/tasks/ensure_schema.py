import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, MONTHLY_INGESTIONS_TABLE

def ensure_db_table():
    """Ensure monthly_ingestions table exists with checksum column."""
    with psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {MONTHLY_INGESTIONS_TABLE} (
                    year INT NOT NULL,
                    month INT NOT NULL,
                    processed_date TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY(year, month)
                );
            """)
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='monthly_ingestions' AND column_name='checksum'
                    ) THEN
                        ALTER TABLE {MONTHLY_INGESTIONS_TABLE} ADD COLUMN checksum TEXT;
                    END IF;
                END$$;
            """)
    print("✓ Schema verified (idempotent check).")
