## MODIFIED Requirements

### Requirement: Broadcast via teranode client with octet-stream
The propagation service SHALL use the teranode client to broadcast raw transaction bytes (not JSON) to all configured datahub endpoints concurrently. Single transactions use POST `/tx`, batches use POST `/txs`. Content type SHALL be `application/octet-stream`.

#### Scenario: Concurrent broadcast to 3 endpoints
- **WHEN** a validated transaction is ready for propagation
- **THEN** the service SHALL call `SubmitTransaction` on the teranode client for each endpoint concurrently, accepting if at least one succeeds

### Requirement: Register with merkle-service via /watch
The propagation service SHALL register transactions with the merkle-service by calling `Register(ctx, txid, callbackURL)` which POSTs to the `/watch` endpoint. Registration SHALL happen before broadcast (best-effort, non-blocking).

#### Scenario: Register before broadcast
- **WHEN** a transaction is ready for propagation
- **THEN** the service SHALL register with merkle-service first, then broadcast to datahubs

#### Scenario: Registration failure is non-fatal
- **WHEN** merkle-service registration fails
- **THEN** the service SHALL log the error but proceed with broadcasting

### Requirement: Status priority for concurrent results
When broadcasting to multiple endpoints, the propagation service SHALL aggregate results using status priority: AcceptedByNetwork (3) > SentToNetwork (2) > Rejected (1), returning the highest-priority status.

#### Scenario: Mixed broadcast results
- **WHEN** endpoint A returns 200 (AcceptedByNetwork) and endpoint B returns 202 (SentToNetwork)
- **THEN** the propagation service SHALL report AcceptedByNetwork as the result
