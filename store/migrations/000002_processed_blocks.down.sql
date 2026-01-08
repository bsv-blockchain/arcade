DROP TABLE IF EXISTS processed_blocks;

-- Recreate network_state if rolling back
CREATE TABLE IF NOT EXISTS network_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_height INTEGER NOT NULL,
    last_block_hash TEXT NOT NULL,
    last_block_time DATETIME NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
