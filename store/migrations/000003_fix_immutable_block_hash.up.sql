-- Repopulate block_hash on transactions that were wiped when transitioning to IMMUTABLE.
-- Joins through processed_blocks to ensure we use the canonical chain's block hash.
UPDATE transactions
SET block_hash = mp.block_hash
FROM merkle_paths mp
JOIN processed_blocks pb ON mp.block_hash = pb.block_hash AND pb.on_chain = 1
WHERE transactions.txid = mp.txid
  AND transactions.block_hash IS NULL;
