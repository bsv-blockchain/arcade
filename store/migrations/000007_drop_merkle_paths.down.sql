CREATE TABLE IF NOT EXISTS merkle_paths (
    txid TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_height INTEGER NOT NULL,
    merkle_path BLOB NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (txid, block_hash)
);
CREATE INDEX IF NOT EXISTS idx_merkle_paths_block_hash ON merkle_paths(block_hash);
