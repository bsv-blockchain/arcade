## Why

When transactions are submitted in batches via Kafka, dependent transactions (child spending parent's output) may arrive at the propagation service before their parents have been broadcast and accepted by the network. The current system immediately rejects these with `StatusRejected`, requiring manual resubmission. This is a fundamental problem with partitioned Kafka consumption — transaction ordering across partitions is not guaranteed.

## What Changes

- Add a configurable retry mechanism to the propagation service that re-attempts broadcasting transactions that fail due to missing-input errors (parent not yet confirmed on network)
- Introduce a `StatusPendingRetry` intermediate status so these transactions are distinguishable from permanently rejected ones
- Add retry configuration: max attempts, backoff interval, and which rejection reasons are retryable
- After exhausting retries, transition to `StatusRejected` as today (no behavioral change for permanent failures)

## Capabilities

### New Capabilities
- `broadcast-retry`: Retry logic for transaction broadcasting failures caused by missing dependencies (parent txs not yet visible to the network). Covers retry scheduling, backoff strategy, max attempt limits, retryable error classification, and status tracking during retries.

### Modified Capabilities
<!-- No existing specs to modify -->

## Impact

- **`services/propagation/propagator.go`**: Core retry loop added to broadcast flow
- **`models/transaction.go`**: New `StatusPendingRetry` status and updated transition rules
- **`config/config.go`**: New `Propagation.RetryMaxAttempts`, `Propagation.RetryBackoff` fields
- **`store/` (interface + aerospike)**: Query for pending-retry transactions; track attempt count
- **Kafka topics**: No new topics — retries happen within the propagation consumer's flush cycle
- **API responses**: `GET /tx/:txid` may return `"txStatus": "PENDING_RETRY"` during retry window
