## ADDED Requirements

### Requirement: Validate transactions
The TX validator SHALL consume messages from the `transaction` Kafka topic (for new submissions), validate the transaction data, and route valid transactions to propagation or reject invalid ones.

#### Scenario: Valid transaction
- **WHEN** a new transaction message is consumed and the transaction passes all validation checks
- **THEN** the validator SHALL publish the transaction to the propagation Kafka topic and store the transaction in Aerospike with initial state

#### Scenario: Invalid transaction
- **WHEN** a new transaction message is consumed and the transaction fails validation
- **THEN** the validator SHALL update the transaction state to REJECTED in Aerospike with the rejection reason and SHALL NOT propagate it

#### Scenario: Duplicate transaction
- **WHEN** a transaction is submitted that already exists in Aerospike
- **THEN** the validator SHALL return the existing transaction state without re-processing

### Requirement: Transaction state updates
The TX validator SHALL process state update messages (from merkle-service callbacks) and update transaction records in Aerospike.

#### Scenario: State transition SEEN_ON_NETWORK
- **WHEN** a state update message with state `SEEN_ON_NETWORK` is consumed for an existing transaction
- **THEN** the validator SHALL update the transaction state in Aerospike to `SEEN_ON_NETWORK`

#### Scenario: State transition SEEN_MULTIPLE_NODES
- **WHEN** a state update message with state `SEEN_MULTIPLE_NODES` is consumed
- **THEN** the validator SHALL update the transaction state in Aerospike to `SEEN_MULTIPLE_NODES`

### Requirement: Batch validation
The TX validator SHALL support validating batches of transactions efficiently, processing them in configurable batch sizes.

#### Scenario: Batch of 1000 transactions
- **WHEN** 1000 transaction messages are available in the Kafka topic
- **THEN** the validator SHALL consume and validate them in batches, using batched Aerospike operations for storage
