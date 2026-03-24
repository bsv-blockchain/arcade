CREATE TABLE IF NOT EXISTS bumps (
    block_hash TEXT PRIMARY KEY,
    block_height INTEGER NOT NULL,
    bump_data BLOB NOT NULL,
    created_at DATETIME NOT NULL
);
