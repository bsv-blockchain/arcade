DROP TABLE IF EXISTS stumps;
CREATE TABLE stumps (
    block_hash TEXT NOT NULL,
    subtree_index INTEGER NOT NULL,
    stump_data TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (block_hash, subtree_index)
);
