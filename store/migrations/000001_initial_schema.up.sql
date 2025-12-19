-- Transaction tracking with current status
CREATE TABLE IF NOT EXISTS transactions (
    txid TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    timestamp DATETIME NOT NULL,
    block_hash TEXT,
    block_height INTEGER,
    merkle_path TEXT,
    extra_info TEXT,
    competing_txs TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);

-- Client submissions and subscriptions
CREATE TABLE IF NOT EXISTS submissions (
    submission_id TEXT PRIMARY KEY,
    txid TEXT NOT NULL,
    callback_url TEXT,
    callback_token TEXT,
    full_status_updates INTEGER DEFAULT 0,
    last_delivered_status TEXT,
    retry_count INTEGER DEFAULT 0,
    next_retry_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_submissions_txid ON submissions(txid);
CREATE INDEX IF NOT EXISTS idx_submissions_callback_token ON submissions(callback_token);
CREATE INDEX IF NOT EXISTS idx_next_retry ON submissions(next_retry_at);

-- Network state tracking
CREATE TABLE IF NOT EXISTS network_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_height INTEGER NOT NULL,
    last_block_hash TEXT NOT NULL,
    last_block_time DATETIME NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
