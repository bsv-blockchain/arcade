## Context

Arcade uses Kafka for transaction processing with three stages: API ingestion → validation → propagation. Each stage is a separate consumer group. When clients submit batches of dependent transactions (e.g., parent + child), Kafka partitioning provides no ordering guarantee across partitions. A child transaction may reach the propagation service and be broadcast to Teranode before its parent has been accepted, causing a "missing inputs" rejection.

Currently, the propagation service (`propagator.go`) broadcasts transactions and maps any endpoint failure directly to `StatusRejected`. This is correct for permanently invalid transactions but wrong for ordering-dependent failures that would succeed moments later.

The original ARC implementation handled this implicitly by processing transactions sequentially through SQLite. The refactored Kafka-based architecture introduces parallelism that exposes this ordering problem.

## Goals / Non-Goals

**Goals:**
- Automatically retry broadcast failures that are likely transient (missing inputs / parent not yet confirmed)
- Make retry behavior configurable (max attempts, backoff)
- Preserve existing behavior for permanently invalid transactions (immediate rejection)
- Track retry state so API consumers can distinguish "still trying" from "permanently failed"

**Non-Goals:**
- Transaction dependency graph analysis or topological sorting before broadcast
- Guaranteed ordering of dependent transactions across Kafka partitions
- Retry of validation failures (policy violations are permanent)
- Custom per-transaction retry policies

## Decisions

### 1. Retry within the propagation consumer's flush cycle

**Decision**: Implement retries by re-enqueueing failed transactions into a retry buffer within the propagation service, not via Kafka re-publishing.

**Rationale**: Re-publishing to Kafka adds latency (consumer lag, partition rebalancing) and complicates offset management. An in-memory retry buffer with backoff is simpler and faster. If the service restarts, pending retries are lost — but these transactions are still in the store with `StatusPendingRetry` and can be recovered on startup.

**Alternative considered**: Kafka retry topic (`arcade.propagation.retry`) with delayed consumption. Rejected because Kafka doesn't natively support delayed delivery, requiring either external scheduling or busy-polling with timestamp checks.

### 2. Classify retryable vs permanent failures by Teranode error response

**Decision**: Parse the Teranode endpoint error response to determine retryability. "Missing inputs" and "mempool conflict" errors are retryable; all others are permanent rejections.

**Rationale**: Teranode returns structured error messages. Matching on known transient error patterns (e.g., `"missing inputs"`, `"txn-mempool-conflict"`) is reliable and avoids retrying genuinely invalid transactions.

**Alternative considered**: Retry all broadcast failures unconditionally. Rejected because it would waste resources on permanently invalid transactions and delay their rejection.

### 3. Exponential backoff with jitter

**Decision**: Use exponential backoff (`baseInterval * 2^attempt`) with ±25% jitter, capped at 30 seconds.

**Rationale**: Prevents thundering herd on retry waves. Jitter prevents correlated retries from landing at the same time. Cap prevents excessively long waits.

### 4. New `StatusPendingRetry` status

**Decision**: Add `StatusPendingRetry` as a transient status between broadcast failure and final rejection. Transitions: `StatusReceived → StatusPendingRetry → StatusRejected` (on exhaust) or `StatusPendingRetry → StatusAcceptedByNetwork` (on success).

**Rationale**: Gives API consumers visibility into retry state. Without this, transactions appear as either "received" (misleading) or "rejected" (premature).

### 5. Attempt counter stored in Aerospike

**Decision**: Store `retry_count` as a bin on the transaction record, incremented on each retry attempt.

**Rationale**: Enables accurate max-attempt enforcement across flush cycles and service restarts. Also provides observability (how many retries did this tx need?).

## Risks / Trade-offs

- **[Memory pressure from retry buffer]** → Mitigation: Cap the retry buffer size (configurable). When full, excess retries go directly to `StatusRejected`. Monitor buffer size via logging.

- **[Retry storms on sustained Teranode unavailability]** → Mitigation: Exponential backoff with cap. If all endpoints fail for all transactions in a batch, that's a connectivity issue, not a dependency ordering issue — the existing Kafka consumer retry (DLQ after max retries) handles this.

- **[Lost retries on service restart]** → Mitigation: On startup, query store for transactions with `StatusPendingRetry` and re-enqueue them. This is best-effort recovery, not guaranteed exactly-once.

- **[Batch broadcast complicates per-tx retry classification]** → Mitigation: When batch broadcast fails, fall back to single-tx broadcast for retry attempts so we can classify each transaction's error individually.
