-- Track processed blocks and canonical chain state
CREATE TABLE IF NOT EXISTS processed_blocks (
    block_hash TEXT PRIMARY KEY,
    block_height INTEGER NOT NULL,
    on_chain INTEGER NOT NULL DEFAULT 1,  -- 1 = part of canonical chain, 0 = orphaned/disconnected
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processed_blocks_height ON processed_blocks(block_height);
CREATE INDEX IF NOT EXISTS idx_processed_blocks_on_chain_height ON processed_blocks(on_chain, block_height);

-- Drop unused network_state table (replaced by processed_blocks)
DROP TABLE IF EXISTS network_state;
