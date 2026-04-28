## ADDED Requirements

### Requirement: Transaction submission via octet-stream
The teranode client SHALL submit transactions to datahub endpoints via POST to `/tx` (single) and `/txs` (batch) using `application/octet-stream` content type with raw transaction bytes.

#### Scenario: Submit single transaction
- **WHEN** `SubmitTransaction(ctx, endpoint, rawTx)` is called
- **THEN** it SHALL POST raw bytes to `{endpoint}/tx` with `Content-Type: application/octet-stream` and return the HTTP status code (200=accepted, 202=queued)

#### Scenario: Submit batch transactions
- **WHEN** `SubmitTransactions(ctx, endpoint, rawTxs)` is called with multiple transactions
- **THEN** it SHALL concatenate all raw transaction bytes and POST to `{endpoint}/txs` as a single body

### Requirement: Bearer token authentication
The teranode client SHALL include an `Authorization: Bearer {token}` header on all requests when an auth token is configured.

#### Scenario: Authenticated request
- **WHEN** a client is created with authToken "mytoken" and submits a transaction
- **THEN** the request SHALL include header `Authorization: Bearer mytoken`

### Requirement: Multi-endpoint support
The teranode client SHALL expose `GetEndpoints()` returning all configured datahub URLs for concurrent broadcast by the propagation service.

#### Scenario: Get endpoints
- **WHEN** a client is created with 3 endpoint URLs
- **THEN** `GetEndpoints()` SHALL return all 3 URLs
