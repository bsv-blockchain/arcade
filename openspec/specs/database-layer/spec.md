# database-layer Specification

## Purpose
TBD - created by archiving change arcade-microservice-scaffold. Update Purpose after archive.
## Requirements
### Requirement: Aerospike client with batch operations
The database layer SHALL provide an Aerospike client wrapper that enforces batched read/write operations for all data access. Individual record operations SHALL only be used for single-record lookups (e.g., GET by TXID).

#### Scenario: Batch write 10,000 transaction records
- **WHEN** the TX validator needs to store 10,000 validated transactions
- **THEN** the store layer SHALL write them in configurable batch sizes (e.g., 500 per batch) using Aerospike batch write operations

#### Scenario: Batch read transactions by TXIDs
- **WHEN** the BUMP builder needs to look up 5,000 transactions for a block
- **THEN** the store layer SHALL read them using Aerospike batch read operations

### Requirement: Transaction storage
The database layer SHALL store transaction records with: TXID (key), raw transaction data, current state, merkle proof (when available), timestamps (created, last updated), and rejection reason (if applicable).

#### Scenario: Store new transaction
- **WHEN** a validated transaction is stored
- **THEN** the record SHALL contain the TXID, raw data, initial state, creation timestamp, and no proof data

#### Scenario: Update transaction state
- **WHEN** a transaction state changes from SEEN_ON_NETWORK to SEEN_MULTIPLE_NODES
- **THEN** the store layer SHALL update only the state and last_updated fields

#### Scenario: Attach merkle proof to transaction
- **WHEN** the BUMP builder produces a merkle proof for a transaction
- **THEN** the store layer SHALL update the transaction record with the proof data and set state to include proof availability

### Requirement: STUMP storage and pruning
The database layer SHALL store STUMP records indexed by block hash and TXID, and support bulk deletion (pruning) of all STUMPs for a given block.

#### Scenario: Store STUMP for transaction
- **WHEN** a STUMP callback is received for TXID `tx123` in block `blk456`
- **THEN** the store layer SHALL store the STUMP data keyed by block hash + TXID

#### Scenario: Retrieve all STUMPs for a block
- **WHEN** the BUMP builder requests STUMPs for block `blk456`
- **THEN** the store layer SHALL return all STUMP records associated with that block hash using a batch read or secondary index query

#### Scenario: Prune STUMPs for a block
- **WHEN** the BUMP builder completes BUMP construction for block `blk456`
- **THEN** the store layer SHALL delete all STUMP records for that block using batched delete operations

### Requirement: Block storage
The database layer SHALL store block records using the Teranode block model definition, including block header, coinbase transaction, and merkle root.

#### Scenario: Store full block
- **WHEN** the block processor stores a fetched block
- **THEN** the record SHALL contain the block hash (key), block header, coinbase transaction data, and block height

#### Scenario: Retrieve block by hash
- **WHEN** the BUMP builder needs the coinbase merkle path for block `blk456`
- **THEN** the store layer SHALL return the block record with coinbase transaction data

### Requirement: BUMP storage
The database layer SHALL store completed BUMP records indexed by block hash.

#### Scenario: Store completed BUMP
- **WHEN** the BUMP builder constructs a full BUMP for block `blk456`
- **THEN** the store layer SHALL store the BUMP data keyed by block hash

### Requirement: Connection pooling and health checks
The Aerospike client SHALL use connection pooling with configurable pool size and expose health check methods for service readiness probes.

#### Scenario: Health check
- **WHEN** a service readiness probe queries the database health
- **THEN** the client SHALL return healthy if Aerospike is reachable and the connection pool is active

