## 1. Models & Config

- [x] 1.1 Add `StatusPendingRetry` to `models/transaction.go` status constants and update `ValidTransitions()` to allow transitions: any → PENDING_RETRY, PENDING_RETRY → ACCEPTED_BY_NETWORK, PENDING_RETRY → REJECTED
- [x] 1.2 Add retry config fields to `config/config.go`: `PropagationConfig.RetryMaxAttempts` (default 5), `PropagationConfig.RetryBackoffMs` (default 500), `PropagationConfig.RetryBufferSize` (default 10000)

## 2. Store Layer

- [x] 2.1 Add `IncrementRetryCount(ctx, txid) (int, error)` and `GetPendingRetryTxs(ctx) ([]*TransactionStatus, error)` to the `Store` interface in `store/store.go`
- [x] 2.2 Implement `IncrementRetryCount` in `store/aerospike.go` — atomically increment `retry_count` bin and return new value
- [x] 2.3 Implement `GetPendingRetryTxs` in `store/aerospike.go` — query transactions set where `status = "PENDING_RETRY"`, return with raw_tx data needed for rebroadcast

## 3. Retryable Error Classification

- [x] 3.1 Add `IsRetryableError(errMsg string) bool` function in `services/propagation/` that matches "missing inputs" and "txn-mempool-conflict" patterns in broadcast error responses
- [x] 3.2 Add unit tests for `IsRetryableError` covering retryable patterns, permanent errors, empty strings, and case variations

## 4. Retry Buffer

- [x] 4.1 Create `services/propagation/retry_buffer.go` with an in-memory retry buffer struct holding pending retry entries (txid, raw_tx, attempt count, next retry time). Implement `Add()`, `Ready()` (returns entries whose backoff has elapsed), `Remove()`, `IsFull()`, and `Len()`
- [x] 4.2 Implement exponential backoff calculation: `baseInterval * 2^attempt` with ±25% jitter, capped at 30 seconds
- [x] 4.3 Add unit tests for retry buffer: add/remove, ready filtering by time, capacity limits, backoff calculation

## 5. Propagation Service Integration

- [x] 5.1 Update `propagator.go` to initialize the retry buffer on construction using config values
- [x] 5.2 Update `broadcastSingleToEndpoints` to return the error message string (not just status) so the caller can classify retryable vs permanent
- [x] 5.3 Update `processBatch` to classify broadcast failures: retryable errors → add to retry buffer + set `StatusPendingRetry` + increment retry count; permanent errors → `StatusRejected` (existing behavior)
- [x] 5.4 Update the flush cycle to also process ready retry entries from the buffer: dequeue entries whose backoff has elapsed, broadcast individually, handle success (update to ACCEPTED_BY_NETWORK) or re-enqueue (if still retryable and under max attempts) or reject (if max attempts exhausted with ExtraInfo "broadcast retries exhausted")
- [x] 5.5 Handle retry buffer overflow: when buffer is full, immediately reject with ExtraInfo "retry buffer full"
- [x] 5.6 On batch broadcast failure during retry classification, fall back to single-tx broadcast per transaction so each gets individual error classification

## 6. Startup Recovery

- [x] 6.1 On propagation service startup, call `GetPendingRetryTxs()` and reject stale entries (raw_tx not persisted, so retries cannot continue after restart)
- [x] 6.2 Add integration test: set transactions to PENDING_RETRY in mock store, start propagator, verify they are rejected on startup

## 7. End-to-End Tests

- [x] 7.1 Add test: broadcast fails with "missing inputs" → tx enters PENDING_RETRY → retry succeeds → tx becomes ACCEPTED_BY_NETWORK
- [x] 7.2 Add test: broadcast fails with "missing inputs" repeatedly → retries exhausted → tx becomes REJECTED with "broadcast retries exhausted"
- [x] 7.3 Add test: broadcast fails with permanent error → tx immediately REJECTED (no retry)
- [x] 7.4 Add test: retry buffer full → immediate REJECTED with "retry buffer full"
- [x] 7.5 Verify `GET /tx/:txid` returns `PENDING_RETRY` status during retry window
