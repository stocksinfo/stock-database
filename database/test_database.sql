CREATE TABLE IF NOT EXISTS stocks (
    ticker VARCHAR(20) NOT NULL,
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    isin VARCHAR(12) UNIQUE NOT NULL,
    wkn VARCHAR(10),
    name VARCHAR(255) NOT NULL,
    exchange_id INTEGER NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap_tier VARCHAR(10),
    currency VARCHAR(3),
    active BOOLEAN DEFAULT 1,
    data_source VARCHAR(50) DEFAULT 'yahoo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (exchange_id) REFERENCES exchanges (id),
    UNIQUE(ticker, exchange_id)
)
