CREATE TABLE IF NOT EXISTS stumps (
    txid TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    subtree_index INTEGER NOT NULL,
    stump_data BLOB NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (txid, block_hash)
);
CREATE INDEX IF NOT EXISTS idx_stumps_block_hash ON stumps(block_hash);
