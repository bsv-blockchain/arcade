-- Remove legacy columns that were moved to the merkle_paths table.
ALTER TABLE transactions DROP COLUMN block_height;
ALTER TABLE transactions DROP COLUMN merkle_path;
