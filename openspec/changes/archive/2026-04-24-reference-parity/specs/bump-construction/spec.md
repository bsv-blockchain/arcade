## ADDED Requirements

### Requirement: Assemble full BUMP from STUMP with subtree offset shifting
The system SHALL implement `assembleFullPath(stumpData, subtreeIndex, subtreeHashes, coinbaseBUMP)` that:
1. Parses the BRC-74 binary STUMP via `transaction.NewMerklePathFromBinary`
2. For subtree 0 with coinbase BUMP available, replaces the coinbase placeholder via `applyCoinbaseToSTUMP`
3. For single-subtree blocks, returns the STUMP directly as the full BUMP
4. For multi-subtree blocks, shifts all STUMP offsets from local to global using `offset += subtreeIndex << (internalHeight - level)`
5. Appends subtree root hash layer at height `ceil(log2(numSubtrees))`
6. Calls `ComputeMissingHashes()` to fill in intermediate nodes

#### Scenario: Multi-subtree STUMP assembly
- **WHEN** `assembleFullPath` is called with a STUMP in subtree index 2 of a 4-subtree block
- **THEN** level-0 offsets SHALL be shifted by `2 << internalHeight` and subtree root hashes for subtrees 0, 1, 3 SHALL be added at the subtree root layer

#### Scenario: Single-subtree block
- **WHEN** `assembleFullPath` is called with subtreeHashes containing only 1 entry
- **THEN** the STUMP SHALL be returned directly as the full path without offset shifting or subtree layer

### Requirement: Coinbase placeholder replacement
The system SHALL implement `applyCoinbaseToSTUMP` that replaces the placeholder hash at level 0 offset 0 with the real coinbase TXID, handling both full STUMPs (all level-0 leaves present) and minimal STUMPs (only tracked tx path).

#### Scenario: Full STUMP coinbase replacement
- **WHEN** the STUMP includes level-0 offset 0 and a coinbase BUMP is available
- **THEN** offset 0 hash SHALL be replaced with the real coinbase TXID and stale higher-level offset-0 hashes SHALL be cleared for recomputation

#### Scenario: Minimal STUMP coinbase replacement
- **WHEN** the STUMP does NOT include level-0 offset 0 but higher levels have offset-0 hashes
- **THEN** the system SHALL walk the coinbase BUMP to compute correct intermediate hashes and replace stale offset-0 entries at each level

### Requirement: Build compound BUMP from multiple STUMPs
The system SHALL implement `BuildCompoundBUMP(stumps, subtreeHashes, coinbaseBUMP, tracker)` that merges all STUMPs into a single compound MerklePath, deduplicating path elements by `(level, offset)`, and uses the TxTracker to discover tracked transaction IDs from level-0 hashes.

#### Scenario: Merge two STUMPs from different subtrees
- **WHEN** `BuildCompoundBUMP` is called with STUMPs from subtree 0 and subtree 3
- **THEN** the compound BUMP SHALL contain all path elements from both assembled paths, deduplicated by (level, offset), and return the tracked TXIDs found in both STUMPs

### Requirement: Extract minimal path for single transaction
The system SHALL implement `ExtractMinimalPath(fullPath, txOffset)` that extracts only the nodes needed to verify a single transaction: the leaf at txOffset and the sibling at each level (offset XOR 1).

#### Scenario: Extract minimal path
- **WHEN** `ExtractMinimalPath` is called with a compound BUMP and txOffset 5
- **THEN** the result SHALL contain the leaf at offset 5 and siblings at offsets 4, 3, 0 (walking up the tree with XOR 1 and right-shift at each level)

### Requirement: Construct BUMPs for block
The system SHALL implement the full BUMP construction pipeline: fetch STUMPs from store, fetch subtree hashes and coinbase BUMP from datahub, call BuildCompoundBUMP, store the compound BUMP as BRC-74 binary, set tracked transactions to MINED status, and prune STUMPs.

#### Scenario: Full BUMP construction pipeline
- **WHEN** BLOCK_PROCESSED is received for a block with 3 stored STUMPs containing 500 tracked transactions
- **THEN** the system SHALL build a compound BUMP, store it as binary, update all 500 transactions to MINED with the block hash, and delete the STUMPs

### Requirement: Fetch block data for BUMP construction
The system SHALL fetch subtree hashes and coinbase BUMP from datahub, trying JSON endpoint first (which includes coinbase_bump field), falling back to binary endpoint.

#### Scenario: JSON endpoint returns subtree data
- **WHEN** the datahub JSON endpoint returns `{subtrees: ["hash1", "hash2"], coinbase_bump: "hexdata"}`
- **THEN** the system SHALL parse subtree hashes as chainhash.Hash values and decode coinbase_bump from hex
