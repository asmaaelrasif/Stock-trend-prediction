-- ===============================================
-- Initialization script for Stock Data Pipeline
-- ===============================================

-- Track which monthly CSVs were ingested (Airflow)
CREATE TABLE IF NOT EXISTS monthly_ingestions (
    year INT NOT NULL,
    month INT NOT NULL,
    checksum TEXT,      -- ensures idempotency per period
    processed_date TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (year, month)
);

-- Track which raw files were transformed (Spark)
CREATE TABLE IF NOT EXISTS processed_files (
    file_name TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    processed_date TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Main transformed stock data table (all original CSV columns + metrics)
CREATE TABLE IF NOT EXISTS stock_data (
    date TIMESTAMP NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    ticker TEXT,
    name TEXT,
    prev_close FLOAT,
    daily_return FLOAT,
    trend_label TEXT,
    ma10 FLOAT,
    ma20 FLOAT,
    volatility_10d FLOAT,
    year INT,
    month INT,
    source_file TEXT,
    PRIMARY KEY (date, ticker, source_file)
);

-- Indexes for faster analytics
CREATE INDEX IF NOT EXISTS idx_stock_data_date ON stock_data (date);
CREATE INDEX IF NOT EXISTS idx_stock_data_year_month ON stock_data (year, month);
CREATE INDEX IF NOT EXISTS idx_stock_data_trend ON stock_data (trend_label);