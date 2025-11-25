import os
import pandas as pd
from config import CSV_FILE, CSV_URL

def download_full_csv_func():
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    if os.path.exists(CSV_FILE):
        print(f"✓ Local CSV already exists at {CSV_FILE} — skipping download.")
        return CSV_FILE
    
    try:
        df = pd.read_csv(CSV_URL)
    except Exception as e:
        print(f"⚠️ Failed to download CSV: {e}")
        raise
    
    df.to_csv(CSV_FILE, index=False)
    print(f"✓ Downloaded and saved to {CSV_FILE}")
    return CSV_FILE
