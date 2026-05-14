# Dependency-Aware Dispatch

Plan to add parent-child dependency awareness to Arcade's broadcast pipeline so that transactions in the queue with parent-child relationships are sequenced correctly when sent to Teranode. The redesign also simplifies the pipeline: validation moves to intake, the `tx_validator` service is removed, PENDING_RETRY status and the reaper are removed, and retry state lives in the dispatcher's memory backed by Kafka replay for durability.

## Problem

Today's pipeline batches and broadcasts transactions to Teranode without awareness of parent-child relationships between in-flight transactions. With `MaxConcurrentBatches=4`, a parent transaction can be in one batch while its child is in another batch broadcasting concurrently. Teranode handles dependency ordering correctly within a single `/txs` POST, but it has no visibility across separate concurrent batches. The result is "missing inputs" rejections for children whose parents are still being processed in a sibling batch.

The current mitigation routes those rejections through PENDING_RETRY and the reaper, which adds latency, database churn, and operational complexity for a class of failure that doesn't need to happen.

## Goal

Eliminate the parent-child race within Arcade by tracking which in-flight transactions are parents of other in-flight transactions, and gating the broadcast of children until their parents have reached a terminal state (`ACCEPTED_BY_NETWORK` or `REJECTED`).

Scope is limited to parent-child relationships where both transactions came through Arcade. Children whose parents are broadcast through other paths are out of scope — those continue to follow the current "try and let Teranode decide" behavior.

## Architecture

### Current pipeline

```
HTTP submit → TopicTransaction → tx_validator → TopicPropagation → propagator → Teranode
                                  (parse, dedup,                    (parallel
                                   validate, publish)                broadcast)
```

### New pipeline

```
HTTP submit (parse + validate + dedup + publish)
    → TopicPropagation (new topic, single partition)
        → dispatcher (single goroutine, dep index, retry queue)
            → broadcast workers (parallel)
                → Teranode
```

The `tx_validator` service is removed. Intake performs parse, script/fee validation, and dedup synchronously, then publishes directly to the propagation topic.

### Dispatcher

The dispatcher is a single goroutine reading from a single-partition Kafka topic. It maintains in-memory state:

- `inFlight` — set of txids currently being processed
- `waiters` — map from parent txid to list of child txids waiting on it
- `pendingParents` — map from child txid to set of parent txids it's still waiting on
- `retryQueue` — map from txid to next-retry-time for txs awaiting backoff
- `offsetHeap` — min-heap of Kafka offsets for in-flight txs

For each incoming message:
1. Look up each `input_txid` in `inFlight`
2. If any parent is in-flight, register the child as a waiter on each unmet parent and hold
3. Otherwise add the child to the current pending batch

On terminal status flips (received via the dispatcher's input channel from broadcast workers and the merkle-service callback handler):
- `ACCEPTED_BY_NETWORK`: pop the txid's waiters; for each waiter, decrement `pendingParents`. If empty, release the waiter to the pending batch.
- `REJECTED`: pop the txid's waiters; mark each waiter REJECTED with a parent-rejected reason; recurse using each waiter as a new parent for further cascade.

On retryable failures (genuine transient infra errors, network failures, or `ErrTxMissingParent` from Teranode):
- Add the txid to `retryQueue` with `nextRetryAt = now + backoff`
- A timer goroutine in the dispatcher wakes when the soonest retry is due and re-dispatches the tx to broadcast workers

### Batching

Batching remains. The dispatcher accumulates eligible transactions into a pending batch and flushes when batch size reaches `teranodeBatchCap` or a short timer expires. Within a flushed batch, parent and child can coexist — Teranode serializes their processing internally. The dispatcher only holds back children whose parents are in a different in-flight batch.

Broadcast workers consume flushed batches and post to Teranode in parallel. The existing per-tx fallback (when a `/txs` batch returns all-rejected, fall back to per-tx `/tx` calls) is preserved — it's how the dispatcher gets per-tx status outcomes that a batch response can't provide.

### Single partition

The propagation topic is recreated with one partition. This gives total ordering at the topic level, allows a single consumer goroutine to own all state without locking, and supports failover via consumer groups (one active consumer, others on standby).

Throughput is bounded by what a single goroutine can do for dispatch decisions. With map lookups on 32-byte keys, the bottleneck is JSON decode cost (~5-10μs per message). Single-pod sustained throughput is in the tens of thousands of messages per second, sufficient for current load with significant headroom.

### Offset commit policy

The dispatcher tracks Kafka offsets for in-flight transactions in a min-heap. When a transaction reaches terminal status, its offset is popped. The Kafka commit offset is the current minimum, meaning we never advance past unfinished work. Commits are batched every few hundred milliseconds rather than per-message to keep API overhead bounded.

On restart, replay starts from the last committed offset and the dep index rebuilds as messages flow through the dispatcher again — same code path as live operation. The retry queue rebuilds the same way: a tx in the in-flight set that hasn't terminalized will re-enter via replay.

### Retry handling

PENDING_RETRY status and the reaper go away. Retries live in the dispatcher's memory:

- Retryable broadcast failure → add to `retryQueue` with backoff
- Timer wakes when next retry is due → re-dispatch to broadcast workers
- Each retry bumps an attempt counter; once it exceeds the max, the tx flips to terminal REJECTED with reason "broadcast retries exhausted"

Durability comes from Kafka replay: a tx in retry has not committed its offset, so a restart re-processes it. Worst case, retry state (attempt count, next-retry-time) resets to zero on restart, which means a tx might get more retry attempts than intended after a crash. Acceptable trade-off for losing the entire PENDING_RETRY apparatus.

### Mining and merkle-service callbacks

The merkle-service callback path is unchanged. `SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`, `STUMP`, and `BLOCK_PROCESSED` continue to land at the existing HTTP callback endpoint and write directly to the status store. The callback handler additionally pushes status flips onto the dispatcher's input channel so the dep index can release or cascade waiters as appropriate.

### Retry classification

Arcade's retry classification is driven by Teranode's HTTP status code, not by string-matching the error body:

- 422 (Unprocessable Entity, emitted for `ErrTxMissingParent`) → retryable, add to retry queue
- 500 → terminal REJECTED (real infra failure that's not recoverable per-tx)
- 400 / 403 / 409 → terminal REJECTED
- 200 → ACCEPTED_BY_NETWORK (includes the duplicate-submit / `ErrTxExists` case, which Teranode returns as 200 since the tx is already in its UTXO store)
- 202 → in-flight, awaiting merkle-service callback
- No response at all (network error, timeout) → retryable, add to retry queue

Mempool-conflict is terminal REJECTED — wallets resolve it, not Arcade.

The status-code audit on the Teranode side (bsv-blockchain/teranode#870) landed the 422 mapping for `ErrTxMissingParent` and the 200 mapping for `ErrTxExists`. The existing string-match in [`IsRetryableError`](../../services/propagation/retryable.go) is removed in favor of a `statusCode`-driven switch in the same call site.

## Message format

The existing JSON message format is extended with an optional `input_txids` field:

```json
{
  "txid": "...",
  "raw_tx": "<base64>",
  "input_txids": ["...", "...", "..."]
}
```

A binary format (protobuf or similar) is a separate optimization, deferred until throughput measurement justifies it.

## Deployment

Single coordinated deploy. The new code does not coexist with the old: there's no backward-compatibility code path.

### Prerequisites

1. **Drain the existing propagation queue.** All in-flight transactions in the current `TopicTransaction` and `TopicPropagation` topics must reach terminal status before deploy. The current code's normal operation handles this — just stop accepting new submissions, wait for the pipeline to clear.

2. **Clear PENDING_RETRY rows.** Either let the existing reaper drain them, or accept them being lost at cutover (the new code has no concept of PENDING_RETRY status).

### Deploy

Single binary deploy with the new code. The new code:

- Creates and uses a new propagation topic (name TBD, e.g. `TopicPropagationV2`) with one partition
- Performs all intake work synchronously, including validation
- Runs the new dispatcher in place of the old propagator's parallel consumer pool
- Maintains retry queue in memory; no reaper, no PENDING_RETRY

The old `TopicTransaction` and original `TopicPropagation` topics can be deleted after deploy.

## Install base note

The current Arcade install base is limited (essentially just GorillaPool's production deployment). The cost of breaking backward compatibility is minimal. Anyone running their own Arcade would need to follow the drain + redeploy sequence above; there is no rolling upgrade path.

## Non-goals

- Handling parent-child relationships where the parent was not submitted through Arcade
- Reordering or holding the merkle-service callback path
- Replacing the existing JSON message format (deferred to a separate optimization)
- Continuing to support the existing `tx_validator` service in any form
- Maintaining backward compatibility with the old `TopicTransaction` / `TopicPropagation` topic layout
- Handling stale `SENT_TO_NETWORK` rows (the status is unused in today's code; a separate effort can address the underlying gap if needed)
