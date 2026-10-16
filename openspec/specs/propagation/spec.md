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
Before broadcasting, the propagation service SHALL register the transaction with every configured merkle-service endpoint, providing the TXID and the arcade callback URL. A transaction counts as registered once at least one endpoint accepted it.

#### Scenario: Successful registration
- **WHEN** a transaction is ready for broadcast and at least one merkle-service endpoint accepts its `/watch`
- **THEN** the service SHALL broadcast the transaction and stamp `merkle_registered_at`

#### Scenario: Registration failure on every endpoint
- **WHEN** every merkle-service endpoint refuses or cannot be reached
- **THEN** the service SHALL NOT broadcast the transaction, SHALL requeue it, and SHALL log the failure

#### Scenario: Endpoint outage
- **WHEN** one endpoint fails three consecutive requests while others remain healthy
- **THEN** the service SHALL skip that endpoint until its `/health` answers, keep registering with the healthy endpoints, and on recovery re-register the non-terminal transactions the endpoint missed

### Requirement: Concurrent datahub broadcasting
The propagation service SHALL broadcast to multiple datahub URLs concurrently, not sequentially, to minimize latency.

#### Scenario: Three datahub URLs configured
- **WHEN** a transaction is ready for propagation and three datahub URLs are configured
- **THEN** the service SHALL send the transaction to all three URLs concurrently and track individual success/failure for each

