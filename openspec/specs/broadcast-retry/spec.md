## ADDED Requirements

### Requirement: Retryable error classification
The propagation service SHALL classify broadcast failures as either retryable or permanent based on the Teranode endpoint error response. Errors containing "missing inputs", "txn-mempool-conflict", or "failed to validate" SHALL be classified as retryable. All other broadcast errors SHALL be classified as permanent.

#### Scenario: Missing inputs error is retryable
- **WHEN** a transaction broadcast returns an error containing "missing inputs"
- **THEN** the system SHALL classify the failure as retryable

#### Scenario: Mempool conflict error is retryable
- **WHEN** a transaction broadcast returns an error containing "txn-mempool-conflict"
- **THEN** the system SHALL classify the failure as retryable

#### Scenario: Failed to validate error is retryable
- **WHEN** a transaction broadcast returns an error containing "failed to validate" (e.g., Teranode batch endpoint wrapping a missing-inputs error)
- **THEN** the system SHALL classify the failure as retryable

#### Scenario: Unknown error is permanent
- **WHEN** a transaction broadcast returns an error not matching any retryable pattern
- **THEN** the system SHALL classify the failure as permanent and set the transaction status to `REJECTED`

### Requirement: Configurable retry limits
The system SHALL support configurable retry behavior via `Propagation.RetryMaxAttempts` (default: 5) and `Propagation.RetryBackoffMs` (default: 500ms base interval). The retry buffer SHALL have a configurable maximum size via `Propagation.RetryBufferSize` (default: 10000).

#### Scenario: Default configuration
- **WHEN** no retry configuration is provided
- **THEN** the system SHALL use max attempts of 5, base backoff of 500ms, and retry buffer size of 10000

#### Scenario: Custom configuration
- **WHEN** `Propagation.RetryMaxAttempts` is set to 10 and `Propagation.RetryBackoffMs` is set to 1000
- **THEN** the system SHALL retry up to 10 times with 1000ms base backoff

### Requirement: Exponential backoff with jitter
The system SHALL apply exponential backoff between retry attempts using the formula `baseInterval * 2^attempt` with ±25% random jitter, capped at 30 seconds.

#### Scenario: First retry delay
- **WHEN** a transaction fails its first broadcast attempt with a retryable error
- **THEN** the system SHALL wait approximately `baseInterval` (±25% jitter) before the next attempt

#### Scenario: Backoff cap reached
- **WHEN** the computed backoff exceeds 30 seconds
- **THEN** the system SHALL cap the delay at 30 seconds (±25% jitter)

### Requirement: Retry status tracking
The system SHALL set transactions to `PENDING_RETRY` status when a retryable broadcast failure occurs. The system SHALL store and increment a `retry_count` on the transaction record for each retry attempt.

#### Scenario: First retryable failure
- **WHEN** a transaction broadcast fails with a retryable error for the first time
- **THEN** the system SHALL set the transaction status to `PENDING_RETRY` and set `retry_count` to 1

#### Scenario: Subsequent retryable failure
- **WHEN** a transaction in `PENDING_RETRY` status fails again with a retryable error
- **THEN** the system SHALL increment `retry_count` by 1

#### Scenario: API returns pending retry status
- **WHEN** a client queries `GET /tx/:txid` for a transaction in `PENDING_RETRY` status
- **THEN** the response SHALL include `"txStatus": "PENDING_RETRY"`

### Requirement: Max retry exhaustion
The system SHALL transition transactions to `REJECTED` status when the retry count reaches `RetryMaxAttempts`. The `ExtraInfo` field SHALL indicate that retries were exhausted.

#### Scenario: Retries exhausted
- **WHEN** a transaction has been retried `RetryMaxAttempts` times and the last attempt fails
- **THEN** the system SHALL set the transaction status to `REJECTED` with `ExtraInfo` containing "broadcast retries exhausted"

### Requirement: Successful retry
The system SHALL transition transactions from `PENDING_RETRY` to the appropriate success status when a retry broadcast succeeds.

#### Scenario: Retry succeeds after previous failures
- **WHEN** a transaction in `PENDING_RETRY` status is successfully broadcast
- **THEN** the system SHALL update the transaction status to `ACCEPTED_BY_NETWORK`

### Requirement: In-memory retry buffer
The propagation service SHALL maintain an in-memory buffer of transactions awaiting retry. Transactions SHALL be re-attempted during the propagation consumer's flush cycle when their backoff period has elapsed.

#### Scenario: Buffer accepts retryable transaction
- **WHEN** a transaction broadcast fails with a retryable error and the buffer is not full
- **THEN** the system SHALL add the transaction to the retry buffer with its next retry time

#### Scenario: Buffer full
- **WHEN** a transaction broadcast fails with a retryable error but the retry buffer is at capacity
- **THEN** the system SHALL immediately set the transaction to `REJECTED` with `ExtraInfo` containing "retry buffer full"

### Requirement: Batch broadcast fallback to single-tx on retry
When retrying transactions, the system SHALL broadcast them individually (not as a batch) so that per-transaction error classification is possible.

#### Scenario: Retry uses single-tx broadcast
- **WHEN** a transaction from a failed batch broadcast is scheduled for retry
- **THEN** the system SHALL broadcast it individually via the single-transaction endpoint

### Requirement: Startup recovery of pending retries
On service startup, the propagation service SHALL query the store for transactions with `PENDING_RETRY` status and transition them to `REJECTED` status since raw transaction data is not persisted across restarts.

#### Scenario: Service restart with pending retries
- **WHEN** the propagation service starts and the store contains transactions with `PENDING_RETRY` status
- **THEN** the system SHALL set those transactions to `REJECTED` with `ExtraInfo` containing "broadcast retries interrupted by service restart"

### Requirement: Secondary index for status queries
The Aerospike store SHALL create a secondary index on the `status` bin of the transactions set to support efficient queries for transactions by status (e.g., `PENDING_RETRY` recovery on startup).

#### Scenario: Index created on startup
- **WHEN** the store initializes and calls `EnsureIndexes`
- **THEN** a string secondary index on the `status` bin SHALL be created (or confirmed to already exist)
