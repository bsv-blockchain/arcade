# propagation Specification

## Purpose
TBD - created by archiving change arcade-microservice-scaffold. Update Purpose after archive.
## Requirements
### Requirement: Broadcast transactions to datahub
The propagation service SHALL consume validated transactions and broadcast them to all configured datahub URLs.

#### Scenario: Propagate single transaction
- **WHEN** a validated transaction message is consumed from the propagation Kafka topic
- **THEN** the service SHALL send the raw transaction to all configured datahub URLs concurrently

#### Scenario: Propagate batch of transactions
- **WHEN** multiple validated transaction messages are available
- **THEN** the service SHALL batch transactions and broadcast them to each datahub URL efficiently

#### Scenario: Datahub rejects transaction
- **WHEN** a datahub URL responds with an error for a transaction
- **THEN** the service SHALL log the rejection, retry with backoff, and after max retries route to a dead-letter topic

### Requirement: Register transaction with merkle-service
After successful propagation, the propagation service SHALL register the transaction with the merkle-service server, providing the TXID and the arcade callback URL.

#### Scenario: Successful registration
- **WHEN** a transaction has been propagated to at least one datahub successfully
- **THEN** the service SHALL register the TXID and callback URL with the merkle-service

#### Scenario: Registration failure
- **WHEN** merkle-service registration fails
- **THEN** the service SHALL retry with exponential backoff and log the failure

### Requirement: Concurrent datahub broadcasting
The propagation service SHALL broadcast to multiple datahub URLs concurrently, not sequentially, to minimize latency.

#### Scenario: Three datahub URLs configured
- **WHEN** a transaction is ready for propagation and three datahub URLs are configured
- **THEN** the service SHALL send the transaction to all three URLs concurrently and track individual success/failure for each

