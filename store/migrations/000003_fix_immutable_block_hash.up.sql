-- Ensure merkle_paths exists before the fix-up query.
-- On existing installs this is a no-op. On fresh installs it creates the table.
CREATE TABLE IF NOT EXISTS merkle_paths (
    txid TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_height INTEGER NOT NULL,
    merkle_path BLOB NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (txid, block_hash)
);
CREATE INDEX IF NOT EXISTS idx_merkle_paths_block_hash ON merkle_paths(block_hash);

-- Repopulate block_hash on transactions that were wiped when transitioning to IMMUTABLE.
-- Joins through processed_blocks to ensure we use the canonical chain's block hash.
-- On fresh installs both tables are empty so this is a no-op.
UPDATE transactions
SET block_hash = mp.block_hash
FROM merkle_paths mp
JOIN processed_blocks pb ON mp.block_hash = pb.block_hash AND pb.on_chain = 1
WHERE transactions.txid = mp.txid
  AND transactions.block_hash IS NULL;
