## MODIFIED Requirements

### Requirement: Store interface with status-based operations
The store interface SHALL provide `GetOrInsertStatus(ctx, status)` for atomic upsert, `UpdateStatus(ctx, status)` for state transitions, `GetStatus(ctx, txid)`, and `SetMinedByTxIDs(ctx, blockHash, txids)` for bulk mined status updates. All methods SHALL accept `context.Context`.

#### Scenario: Atomic get-or-insert
- **WHEN** `GetOrInsertStatus` is called for a new TXID
- **THEN** the status SHALL be inserted and `(status, true, nil)` returned

#### Scenario: Duplicate insert returns existing
- **WHEN** `GetOrInsertStatus` is called for an existing TXID
- **THEN** the existing status SHALL be returned with `(existing, false, nil)`

#### Scenario: Bulk set mined
- **WHEN** `SetMinedByTxIDs(ctx, "blockhash", ["tx1", "tx2"])` is called
- **THEN** both transactions SHALL be updated to MINED status with the block hash, and full status objects returned

### Requirement: STUMP storage by subtreeIndex
STUMPs SHALL be stored with a composite key of `(blockHash, subtreeIndex)` and binary stump data. The store SHALL provide `InsertStump(ctx, stump)`, `GetStumpsByBlockHash(ctx, blockHash)`, and `DeleteStumpsByBlockHash(ctx, blockHash)`.

#### Scenario: Store and retrieve STUMPs
- **WHEN** 3 STUMPs for subtrees 0, 1, 2 of block "abc" are stored
- **THEN** `GetStumpsByBlockHash(ctx, "abc")` SHALL return all 3 STUMPs with their subtreeIndex and binary data

#### Scenario: Delete STUMPs for block
- **WHEN** `DeleteStumpsByBlockHash(ctx, "abc")` is called after BUMP construction
- **THEN** all STUMP records for block "abc" SHALL be deleted

### Requirement: BUMP storage as binary
BUMPs SHALL be stored as BRC-74 binary data (not JSON). The store SHALL provide `InsertBUMP(ctx, blockHash, blockHeight, bumpData)` and `GetBUMP(ctx, blockHash)` returning `(blockHeight, bumpData, err)`.

#### Scenario: Store and retrieve binary BUMP
- **WHEN** a compound BUMP is stored as binary bytes
- **THEN** `GetBUMP` SHALL return the exact same bytes

### Requirement: Submission tracking
The store SHALL track client submissions with `InsertSubmission(ctx, sub)`, `GetSubmissionsByTxID(ctx, txid)`, and `GetSubmissionsByToken(ctx, token)` for webhook delivery.

#### Scenario: Store and retrieve submission
- **WHEN** a submission with callbackURL is stored for txid "tx1"
- **THEN** `GetSubmissionsByTxID(ctx, "tx1")` SHALL return the submission record
