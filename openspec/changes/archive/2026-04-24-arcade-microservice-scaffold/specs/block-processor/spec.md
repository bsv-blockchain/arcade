## ADDED Requirements

### Requirement: Fetch and store blocks
The block processor SHALL consume messages from the `block` Kafka topic, fetch the full block data from configured datahub URLs, and store the complete block model in Aerospike.

#### Scenario: New block message received
- **WHEN** a message with block hash `abc123` is consumed from the `block` Kafka topic
- **THEN** the processor SHALL fetch the full block from a configured datahub URL and store the block model (using Teranode block definition) in Aerospike

#### Scenario: Block already stored
- **WHEN** a block message is received for a block that already exists in Aerospike
- **THEN** the processor SHALL skip the fetch and storage, and acknowledge the Kafka message

#### Scenario: Datahub fetch failure
- **WHEN** the processor fails to fetch a block from all configured datahub URLs
- **THEN** the processor SHALL retry with exponential backoff and, after max retries, route the message to a dead-letter topic

### Requirement: Batch storage operations
The block processor SHALL use batched Aerospike operations when storing block data, following the Teranode blockassembler pattern.

#### Scenario: Store block with transaction index
- **WHEN** a block containing 1 million transactions is fetched
- **THEN** the processor SHALL store the block header and transaction index using batched Aerospike writes, processing transactions in configurable batch sizes

### Requirement: Datahub URL failover
The block processor SHALL support multiple configured datahub URLs and attempt each in sequence if the primary fails.

#### Scenario: Primary datahub unavailable
- **WHEN** the primary datahub URL returns an error or times out
- **THEN** the processor SHALL attempt the next configured datahub URL before triggering retry logic
