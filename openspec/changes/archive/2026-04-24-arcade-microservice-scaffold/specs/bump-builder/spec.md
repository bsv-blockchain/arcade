## ADDED Requirements

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

### Requirement: Prune STUMPs after BUMP construction
The BUMP builder SHALL delete STUMPs from Aerospike after the full BUMP has been successfully constructed and all merkle proofs stored.

#### Scenario: Successful BUMP construction
- **WHEN** the BUMP for block `abc123` is fully constructed and all proofs are stored
- **THEN** the builder SHALL delete all STUMP records for that block from Aerospike

#### Scenario: Partial BUMP construction failure
- **WHEN** BUMP construction fails for some transactions in a block
- **THEN** the builder SHALL retain the STUMPs for failed transactions and route a retry message to Kafka
