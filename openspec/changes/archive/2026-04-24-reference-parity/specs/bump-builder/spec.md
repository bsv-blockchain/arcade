## MODIFIED Requirements

### Requirement: BUMP construction using go-sdk MerklePath
The BUMP builder SHALL use `transaction.MerklePath` from go-sdk for all merkle path operations instead of custom JSON PathNode structs. STUMPs SHALL be parsed from BRC-74 binary format via `transaction.NewMerklePathFromBinary`. Constructed BUMPs SHALL be stored as BRC-74 binary via `MerklePath.Bytes()`.

#### Scenario: Parse binary STUMP
- **WHEN** a STUMP callback delivers hex-encoded BRC-74 binary data
- **THEN** the builder SHALL decode from hex and parse via `NewMerklePathFromBinary` to obtain a `*transaction.MerklePath`

### Requirement: Compound BUMP construction pipeline
The BUMP builder service SHALL: (1) consume BLOCK_PROCESSED messages from Kafka, (2) retrieve all STUMPs for the block from the store, (3) fetch subtree hashes and coinbase BUMP from datahub, (4) call `BuildCompoundBUMP` to merge all STUMPs, (5) store the compound BUMP as binary, (6) set tracked transactions to MINED status, (7) delete the STUMPs.

#### Scenario: End-to-end BUMP construction
- **WHEN** a BLOCK_PROCESSED message is consumed for a block with 5 stored STUMPs
- **THEN** the builder SHALL fetch block data from datahub, build a compound BUMP, store it, update transaction statuses to MINED, and prune the STUMPs

### Requirement: STUMP consumer stores by subtreeIndex
The STUMP consumer SHALL store incoming STUMPs keyed by `(blockHash, subtreeIndex)` with raw binary stump data, NOT by individual TXID.

#### Scenario: Store STUMP from callback
- **WHEN** a STUMP Kafka message is consumed with blockHash "abc" and subtreeIndex 2
- **THEN** the store SHALL receive an `InsertStump` call with `{BlockHash: "abc", SubtreeIndex: 2, StumpData: <binary>}`
