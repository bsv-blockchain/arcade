# kafka-messaging Specification

## Purpose
TBD - created by archiving change arcade-microservice-scaffold. Update Purpose after archive.
## Requirements
### Requirement: Kafka topic definitions
The system SHALL define the following Kafka topics for inter-service communication: `block`, `stump`, `block_processed`, `transaction`, and `propagation`. Each topic SHALL have configurable partition counts and replication factors.

#### Scenario: All topics available on startup
- **WHEN** any service starts and connects to Kafka
- **THEN** all required topics SHALL exist (auto-created or pre-provisioned) and the service SHALL log successful connection

### Requirement: Shared Kafka producer
The system SHALL provide a shared Kafka producer package that services use to publish messages. The producer SHALL support synchronous and asynchronous publishing modes.

#### Scenario: Publish message to topic
- **WHEN** the API server publishes a callback message to the `stump` topic
- **THEN** the producer SHALL serialize the message, send it to the correct topic, and return a confirmation or error

#### Scenario: Kafka broker unavailable
- **WHEN** a service attempts to publish and the Kafka broker is unreachable
- **THEN** the producer SHALL retry with exponential backoff, buffer messages up to a configurable limit, and return an error if the buffer is exhausted

### Requirement: Consumer group management
Each service type SHALL use a dedicated Kafka consumer group. Multiple instances of the same service SHALL share the workload via consumer group partitioning.

#### Scenario: Two BUMP builder instances
- **WHEN** two BUMP builder instances are running in the same consumer group
- **THEN** Kafka SHALL distribute partitions of the `block_processed` topic across both instances so each processes a subset of messages

#### Scenario: Consumer instance failure
- **WHEN** one instance in a consumer group crashes
- **THEN** Kafka SHALL rebalance partitions to remaining instances and no messages SHALL be lost

### Requirement: Dead-letter topic support
The system SHALL route messages that fail processing after max retries to a dead-letter topic (`<topic>.dlq`) for each main topic.

#### Scenario: Message fails max retries
- **WHEN** a `block` message fails processing 5 times (configurable max)
- **THEN** the consumer SHALL publish the failed message to `block.dlq` with error metadata and continue processing other messages

### Requirement: Message serialization
All Kafka messages SHALL use a consistent serialization format (Protocol Buffers or JSON) with versioned schemas to support forward compatibility.

#### Scenario: Produce and consume message
- **WHEN** the API server produces a JSON-encoded callback message
- **THEN** the downstream consumer SHALL deserialize the message using the same schema and process it correctly

