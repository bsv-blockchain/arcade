## MODIFIED Requirements

### Requirement: Merkle-service callback endpoint
The callback endpoint SHALL accept `CallbackMessage` format with a `type` field (SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, STUMP, BLOCK_PROCESSED) instead of the previous `CallbackPayload` with `state` field. The endpoint SHALL support bearer token authentication. The endpoint path SHALL be `/api/v1/merkle-service/callback`.

#### Scenario: Receive STUMP callback
- **WHEN** merkle-service sends a STUMP callback with `{type: "STUMP", blockHash: "...", subtreeIndex: 2, stump: "<hex>"}`
- **THEN** the handler SHALL parse the BRC-74 binary STUMP, extract level-0 hashes, filter for tracked transactions via TxTracker, update tracked transactions to STUMP_PROCESSING status, store the STUMP keyed by (blockHash, subtreeIndex), and publish a Kafka message to the stump topic

#### Scenario: Receive BLOCK_PROCESSED callback
- **WHEN** merkle-service sends `{type: "BLOCK_PROCESSED", blockHash: "..."}`
- **THEN** the handler SHALL publish a Kafka message to the block_processed topic with the block hash

#### Scenario: Receive SEEN_ON_NETWORK callback with batched TxIDs
- **WHEN** merkle-service sends `{type: "SEEN_ON_NETWORK", txids: ["tx1", "tx2", "tx3"]}`
- **THEN** the handler SHALL update each transaction's status to SEEN_ON_NETWORK and publish status events

#### Scenario: Bearer token validation
- **WHEN** a callback request arrives without a valid `Authorization: Bearer <token>` header and a callback token is configured
- **THEN** the handler SHALL return HTTP 401

### Requirement: Transaction submission with multiple content types
The POST `/tx` endpoint SHALL accept raw transaction bytes as `application/octet-stream`, hex-encoded string as `text/plain`, or JSON body. It SHALL parse BEEF format first, falling back to raw bytes.

#### Scenario: Submit octet-stream transaction
- **WHEN** a client POSTs raw bytes with `Content-Type: application/octet-stream`
- **THEN** the server SHALL parse the transaction, compute TXID, validate, and submit

#### Scenario: Submit hex-encoded transaction
- **WHEN** a client POSTs a hex string with `Content-Type: text/plain`
- **THEN** the server SHALL hex-decode the bytes and process as raw transaction

### Requirement: ARC error responses
All error responses from the API server SHALL use the `ErrorFields` format with `type`, `title`, `status`, `detail`, and optional `extraInfo` fields.

#### Scenario: Validation error response
- **WHEN** a transaction fails validation with ArcError StatusFees (465)
- **THEN** the API SHALL return HTTP 465 with body `{type: "...", title: "Fee too low", status: 465, detail: "Transaction fee is too low"}`
