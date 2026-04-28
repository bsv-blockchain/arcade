## ADDED Requirements

### Requirement: Kafka batch publish
The kafka Producer SHALL provide a `SendBatch` method that accepts a topic and a slice of key/value pairs, and publishes all messages in a single `SendMessages` call to the sync producer. The method SHALL return an error if any message in the batch fails to publish.

#### Scenario: Successful batch publish
- **WHEN** `SendBatch` is called with 100 messages
- **THEN** all 100 messages are published to the specified topic in a single batch round-trip
- **THEN** the method returns nil

#### Scenario: Batch publish failure
- **WHEN** `SendBatch` is called and the broker rejects the batch
- **THEN** the method returns an error wrapping the Sarama error
- **THEN** no partial delivery guarantee is made (Sarama's `SendMessages` is all-or-nothing per partition)

### Requirement: Concurrent merkle registration
The merkle service client SHALL provide a `RegisterBatch` method that accepts a slice of `Registration` structs (txid + callbackURL) and registers them concurrently with bounded parallelism. The method SHALL return on first error (fail-fast).

#### Scenario: All registrations succeed
- **WHEN** `RegisterBatch` is called with 100 registrations and the merkle service returns 200 for all
- **THEN** all 100 txids are registered
- **THEN** the method returns nil

#### Scenario: One registration fails
- **WHEN** `RegisterBatch` is called with 100 registrations and the merkle service returns 500 for the 50th
- **THEN** the method returns an error containing the failed txid
- **THEN** in-flight registrations are cancelled via context

#### Scenario: Concurrency is bounded
- **WHEN** `RegisterBatch` is called with 100 registrations and max concurrency is 10
- **THEN** at most 10 HTTP requests to the merkle service are in-flight at any time

### Requirement: Propagation micro-batching
The propagator SHALL accumulate incoming Kafka messages into micro-batches bounded by a configurable `BatchSize` (default 100) and `FlushInterval` (default 50ms). When either limit is reached, the accumulated batch SHALL be processed together.

#### Scenario: Batch fills before timeout
- **WHEN** 100 messages arrive within the flush interval
- **THEN** the batch is processed immediately when the 100th message arrives
- **THEN** all 100 transactions are registered with merkle service and broadcast together

#### Scenario: Timeout triggers partial batch
- **WHEN** 30 messages arrive and no more arrive within the flush interval
- **THEN** the batch of 30 is processed when the flush interval expires

#### Scenario: Single message latency
- **WHEN** 1 message arrives and no more arrive within the flush interval
- **THEN** the single message is processed when the flush interval expires (max 50ms added latency)

### Requirement: Batch broadcast to teranode
After successful merkle registration of all txids in a batch, the propagator SHALL concatenate all raw transaction bytes and call `SubmitTransactions` (single `POST /txs` per endpoint) instead of individual `SubmitTransaction` calls. The concurrent multi-endpoint fan-out SHALL be preserved.

#### Scenario: Batch broadcast to multiple endpoints
- **WHEN** a batch of 100 transactions is ready to broadcast and 3 teranode endpoints are configured
- **THEN** one `POST /txs` call is made to each of the 3 endpoints concurrently
- **THEN** each call contains all 100 concatenated raw transactions

#### Scenario: Endpoint failure in batch
- **WHEN** a batch broadcast to endpoint A returns 200 and endpoint B returns 500
- **THEN** the best status (AcceptedByNetwork from endpoint A) is used
- **THEN** UpdateStatus is called once per transaction in the batch

### Requirement: Propagation batch config
The config SHALL include a `Propagation` section with `BatchSize` (int, default 100), `FlushIntervalMs` (int, default 50), and `MerkleConcurrency` (int, default 10).

#### Scenario: Custom config values
- **WHEN** config.yaml contains `propagation.batch_size: 50`, `propagation.flush_interval_ms: 100`, `propagation.merkle_concurrency: 5`
- **THEN** the propagator uses batch size 50, flush interval 100ms, and merkle concurrency 5

#### Scenario: Default config values
- **WHEN** no propagation config is specified
- **THEN** the propagator uses batch size 100, flush interval 50ms, and merkle concurrency 10
