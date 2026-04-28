# api-server Specification

## Purpose
TBD - created by archiving change arcade-microservice-scaffold. Update Purpose after archive.
## Requirements
### Requirement: Merkle-service callback endpoint
The API server SHALL expose a POST `/callback` endpoint that accepts merkle-service callbacks. The endpoint SHALL accept callbacks with the following states: REJECTED, SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, STUMP, BLOCK_PROCESSED. The endpoint SHALL validate the callback payload, publish a Kafka message to the appropriate topic, and return a success response. The endpoint SHALL NOT perform any heavy processing inline.

#### Scenario: Receive SEEN_ON_NETWORK callback
- **WHEN** merkle-service sends a POST to `/callback` with state `SEEN_ON_NETWORK` and a valid TXID
- **THEN** the server SHALL publish a message to the `transaction` Kafka topic with the TXID and new state, and return HTTP 200

#### Scenario: Receive STUMP callback
- **WHEN** merkle-service sends a POST to `/callback` with state `STUMP` including a TXID, block hash, and STUMP data
- **THEN** the server SHALL publish a message to the `stump` Kafka topic with the full STUMP payload, and return HTTP 200

#### Scenario: Receive BLOCK_PROCESSED callback
- **WHEN** merkle-service sends a POST to `/callback` with state `BLOCK_PROCESSED` and a block hash
- **THEN** the server SHALL publish a message to the `block_processed` Kafka topic with the block hash, and return HTTP 200

#### Scenario: Receive REJECTED callback
- **WHEN** merkle-service sends a POST to `/callback` with state `REJECTED` and a TXID with rejection reason
- **THEN** the server SHALL publish a message to the `transaction` Kafka topic with the TXID, REJECTED state, and reason, and return HTTP 200

#### Scenario: Invalid callback payload
- **WHEN** merkle-service sends a POST to `/callback` with a malformed or missing required fields
- **THEN** the server SHALL return HTTP 400 with an error description and SHALL NOT publish any Kafka message

### Requirement: Get transaction endpoint
The API server SHALL expose a GET `/tx/:txid` endpoint that retrieves the current state and merkle proof for a transaction from Aerospike.

#### Scenario: Transaction exists with BUMP
- **WHEN** a client sends GET `/tx/:txid` for a transaction that has a completed BUMP
- **THEN** the server SHALL return HTTP 200 with the transaction state, BUMP data, and merkle proof

#### Scenario: Transaction exists without BUMP
- **WHEN** a client sends GET `/tx/:txid` for a transaction in SEEN_ON_NETWORK state
- **THEN** the server SHALL return HTTP 200 with the current transaction state and no proof data

#### Scenario: Transaction not found
- **WHEN** a client sends GET `/tx/:txid` for a TXID that does not exist
- **THEN** the server SHALL return HTTP 404

### Requirement: Submit transaction endpoint
The API server SHALL expose a POST `/tx` endpoint that accepts one or more transactions for validation and propagation. The endpoint SHALL publish transactions to the `transaction` Kafka topic for downstream processing.

#### Scenario: Submit single valid transaction
- **WHEN** a client sends POST `/tx` with a valid raw transaction
- **THEN** the server SHALL publish the transaction to the `transaction` Kafka topic and return HTTP 202 with the TXID

#### Scenario: Submit batch of transactions
- **WHEN** a client sends POST `/tx` with multiple raw transactions
- **THEN** the server SHALL publish each transaction to the `transaction` Kafka topic and return HTTP 202 with all TXIDs

#### Scenario: Submit malformed transaction
- **WHEN** a client sends POST `/tx` with data that cannot be parsed as a transaction
- **THEN** the server SHALL return HTTP 400 with an error description

### Requirement: High-throughput callback handling
The API server SHALL handle thousands of concurrent callback requests without degradation. It SHALL NOT perform database writes, BUMP construction, or transaction propagation inline with callback processing.

#### Scenario: Concurrent callback burst
- **WHEN** the server receives 10,000 callbacks within 1 second
- **THEN** all callbacks SHALL be acknowledged with HTTP 200 and corresponding Kafka messages SHALL be published without request timeouts or dropped connections

