# bump-builder Specification

## Purpose
TBD - created by archiving change arcade-microservice-scaffold. Update Purpose after archive.
## Requirements
### Requirement: Build BUMP from STUMPs
The BUMP builder SHALL consume messages from the `block_processed` Kafka topic, retrieve all stored STUMPs for that block, obtain the coinbase transaction merkle path, and construct a full BUMP (Bitcoin Unified Merkle Path) per BRC-0074.

#### Scenario: Block processed with STUMPs available
- **WHEN** a `block_processed` message is received for block hash `abc123` and STUMPs for 500 registered transactions exist in Aerospike
- **THEN** the builder SHALL retrieve all STUMPs for that block, fetch the coinbase merkle path from the stored block, construct the full BUMP, and store individual merkle proofs for each registered transaction

#### Scenario: No STUMPs for block
- **WHEN** a `block_processed` message is received for a block with no registered transactions (no STUMPs)
- **THEN** the builder SHALL acknowledge the message and take no further action

#### Scenario: STUMPs arrive after BLOCK_PROCESSED
- **WHEN** a `block_processed` message is received but some STUMPs have not yet been stored
- **THEN** the builder SHALL process available STUMPs and re-queue a check for any transactions still missing proofs

### Requirement: Store merkle proofs per transaction
The BUMP builder SHALL store the computed merkle proof for each registered transaction in Aerospike using batched writes and update the transaction state accordingly.

#### Scenario: Store proofs for registered transactions
- **WHEN** BUMPs are constructed for 10,000 transactions in a block
- **THEN** the builder SHALL batch-write all merkle proofs to Aerospike and update each transaction's state to include its proof

### Requirement: Prune STUMPs after a retention window
The BUMP builder SHALL retain a block's STUMP rows for `bump_builder.stump_retention_minutes` after the compound BUMP was built, and SHALL delete them afterwards. STUMP rows are content-addressed (`block_hash`, `subtree_index`, `sha256(stump_data)`) so several merkle-services can deliver the same subtree: identical bytes are one row, divergent variants coexist.

#### Scenario: Retention window elapsed
- **WHEN** the BUMP for block `abc123` was built more than the retention window ago
- **THEN** the janitor SHALL delete all STUMP records for that block

#### Scenario: Within the retention window
- **WHEN** the BUMP for block `abc123` was built less than the retention window ago
- **THEN** the builder SHALL keep the STUMP records so a later `block_processed` delivery can be compared against the stored BUMP

### Requirement: Idempotent processing of repeated block_processed messages
With several merkle-services configured every block's `block_processed` arrives once per service. The BUMP builder SHALL process every delivery and SHALL rebuild the compound BUMP only when the retained STUMP set contains level-0 hashes the stored BUMP lacks.

#### Scenario: Redelivery adds nothing
- **WHEN** a `block_processed` message arrives for a block whose stored BUMP already covers every level-0 hash in the retained STUMPs
- **THEN** the builder SHALL re-mine the stored BUMP's transactions without publishing `MINED` for rows already mined against that block, re-stamp `processed_at`, and record outcome `short_circuited`

#### Scenario: Redelivery adds leaves
- **WHEN** a `block_processed` message arrives and the retained STUMPs (merged per subtree) cover transactions the stored BUMP does not
- **THEN** the builder SHALL rebuild the compound from the full STUMP set, overwrite the stored BUMP, publish `MINED` only for the newly covered transactions, and record outcome `rebuilt`

#### Scenario: Later delivery is incomplete for an already-finalized block
- **WHEN** a `block_processed` message for a block with `processed_at` set is deferred (expected STUMPs missing after the grace window, or a rebuild would drop leaves of the stored BUMP)
- **THEN** the builder SHALL clear `processed_at` so the watchdog re-drives the block via `/reprocess` to every merkle-service

