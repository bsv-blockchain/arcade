# Dependency-Aware Dispatch

Plan to add parent-child dependency awareness to Arcade's broadcast pipeline so that transactions in the queue with parent-child relationships are sequenced correctly when sent to Teranode. The redesign also simplifies the pipeline: validation moves to intake, the `tx_validator` service is removed, the `PENDING_RETRY` status is removed (transient infra failures are kept in-memory and requeued through the dispatcher), the reaper is narrowed to rebroadcasting stale `SEEN_ON_NETWORK` rows that the dispatcher's in-flight set can't see, and the dispatcher holds Kafka offsets in flight until each tx terminalizes so a crash replays everything that wasn't done.

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
- `heldMsgs` — map from child txid to the pending propagation message held until its parent terminalizes
- `pendingMsgs` — the broadcast-ready slice that the next flush drains
- `offsetTracker` — min-heap of Kafka offsets for in-flight txs (with lazy deletion)

For each incoming message:
1. Look up each `input_txid` in `inFlight`
2. If any parent is in-flight, register the child as a waiter on each unmet parent and hold in `heldMsgs`
3. Otherwise add to `pendingMsgs` and record the offset on the tracker

On terminal status events (sent via channel from broadcast workers and the merkle-service callback handler):
- `ACCEPTED_BY_NETWORK`: pop the txid from `inFlight`, mark its offset `Done` on the tracker, release any waiters that no longer have an in-flight parent.
- `REJECTED`: pop the txid from `inFlight`, mark its offset `Done`, walk waiters/`heldMsgs` for the cascade subtree, write terminal REJECTED rows for every cascaded descendant, mark their offsets `Done` too.

Infra failures (Teranode 500, no-peer-reachable, merkle-service /watch failure) are NOT terminal status events — the broadcast worker schedules a deferred requeue through the dispatcher (see "Retry handling"). The offset stays alive on the tracker for the entire retry loop, so the Kafka commit watermark cannot advance past a stuck tx.

### Batching

Batching remains. The dispatcher accumulates eligible transactions into `pendingMsgs` and flushes when batch size reaches `teranodeBatchCap` or a short timer expires.

Children of in-flight parents are never co-batched with their parent — held in `heldMsgs` until the parent terminalizes (Teranode is moving to parallel-process `/txs` per [bsv-blockchain/teranode#879](https://github.com/bsv-blockchain/teranode/pull/879), so an in-batch parent-child pair is no longer safe).

Broadcast workers consume flushed batches and post to Teranode in parallel via `/txs`. The per-tx `/tx` fallback is removed — with [bsv-blockchain/teranode#879](https://github.com/bsv-blockchain/teranode/pull/879) and [bsv-blockchain/teranode#881](https://github.com/bsv-blockchain/teranode/pull/881), the `/txs` response itself carries per-slot status (`"OK"` or `"<TERANODE_CODE> (<num>)"` in submission order) and arcade parses those lines for per-tx classification directly.

### Single partition

The propagation topic is recreated with one partition. This gives total ordering at the topic level, allows a single consumer goroutine to own all state without locking, and supports failover via consumer groups (one active consumer, others on standby).

Throughput is bounded by what a single goroutine can do for dispatch decisions. With map lookups on 32-byte keys, the bottleneck is JSON decode cost (~5-10μs per message). Single-pod sustained throughput is in the tens of thousands of messages per second, sufficient for current load with significant headroom.

### Offset commit policy

The dispatcher tracks Kafka offsets for in-flight transactions in an `offsetTracker` (min-heap with lazy deletion). On admission, the offset is added. On terminal status (accepted, real rejection, or cascade rejection), it's marked done.

`kafka/consumer.go` is modified to stop marking each message immediately after the handler returns. Instead a separate goroutine in the propagator ticks every 200ms, asks the dispatcher for the current `LowestUnfinished`, and tells the consumer to mark every pending message at or below that offset. The Kafka commit position therefore never advances past unfinished work; held children, in-flight broadcasts, and retrying-forever txs all pin the watermark to their offset.

On restart, replay starts from the last committed offset and the dep index rebuilds as messages flow through the dispatcher again — same code path as live operation. A tx that was held or in-flight when the process crashed re-enters the same flow on consume.

### Reaper (narrowed)

The reaper is retained but its role is narrowed. The dispatcher's `offsetTracker` covers everything in flight — held children, broadcasting batches, retrying-forever infra failures — but goes blind once a tx reaches `SEEN_ON_NETWORK` or `SEEN_MULTIPLE_NODES`. Those rows aren't in `inFlight` anymore (the dispatcher's tracker `Done`-d their offset when broadcast accepted them), so the dispatcher can't notice when a peer evicts them from its mempool, a `BLOCK_PROCESSED` callback gets dropped, or a fee bump is needed to land them.

The reaper covers that gap. On a fixed interval (`reaperInterval`) the lease-holding replica scans `IterateStatusesSince` for rows at `SEEN_ON_NETWORK` or `SEEN_MULTIPLE_NODES` older than `staleSeenOnNetworkAge` (currently 1h) and rebroadcasts them through the same `registerBatch` + `broadcastInChunks` pipeline as `processBatch`. The scan is bounded by `reaperRebroadcastBatch` (200) per tick to keep a backlog from pinning the reaper into a single multi-minute call. `RECEIVED` rows are intentionally not rebroadcast — those are owned by the submitter, who got an error from intake on Kafka publish failure and decides whether to retry.

Rebroadcasts bypass the dispatcher's admission (these txids are no longer in `inFlight`); resulting terminal statuses flow into `applyTerminalStatuses` as before but the dispatcher-notify side becomes a no-op for offset bookkeeping.

### Retry handling

`PENDING_RETRY` status goes away. Infrastructure failures retry forever through the dispatcher's requeue path, not via a status-store-backed reaper. The merkle-service and Teranode paths share the same retry surface: any tx that didn't terminalize on a given attempt is sent back through `requeueAfterDelay` → `requeueCh`, the dispatcher re-runs dep-aware admission on it, and it lands back in either `heldMsgs` (if a parent is still in flight) or `pendingMsgs` for the next flush. The Kafka offset stays pinned because the original `inFlight` entry is preserved across the requeue.

**Merkle-service `/watch` failure — per-tx requeue.** `registerBatch` calls `merkleClient.RegisterBatchWithResults` and partitions the results into `(registered, failed)` per-tx; `processBatch` sends the failed subset through `requeueAfterDelay` so each tx individually flows back to the dispatcher. `RegisterBatchWithResults` parallelizes its per-tx HTTP calls internally — a single flaky `/watch` call no longer fails the whole batch, and the successful subset proceeds straight to broadcast.

**Teranode `/txs` per-slot infra failures — per-tx requeue, same path.** With #879 + #881, the `/txs` response delivers per-slot Teranode codes. `broadcastInChunks` walks the per-slot lines and partitions them:

- `"OK"` → terminalize as `ACCEPTED_BY_NETWORK`.
- Terminal Teranode code (`TX_INVALID (31)`, `TX_CONFLICTING (36)`, `UTXO_FROZEN (72)`, etc.) → terminalize as `REJECTED`, cascade as before.
- Infra-classified code (e.g. `PROCESSING (4)`) → infra slot.

For infra slots, `processBatch` sends them through `requeueAfterDelay` after a flat `requeueDelay` wait (currently 2s, tunable as a single constant in `propagator.go`). The dispatcher re-runs admission on each — if the tx's parents are still in flight, it goes back to `heldMsgs`; otherwise it goes back to `pendingMsgs`. The offset stays pinned the whole time (the slot never terminalized, so the tracker never marked it `Done`).

A batch-level HTTP 500 with no parseable body is treated as every-slot-infra: every tx in the batch is requeued through the same path.

**No healthy endpoints / no peer reachable** — same as Teranode infra: requeue every tx in the batch.

No attempt counter, no exhaustion, no terminal REJECTED for infra failures. If merkle-service or Teranode is genuinely down, arcade is correctly stuck — broadcasting without `/watch` registration breaks F-024, and there is no useful work the propagator can do until upstream recovers. Each retry attempt is logged so an operator monitoring the service sees the upstream is unhealthy.

The Kafka offset tracker keeps the watermark pinned to the earliest stuck tx so a restart replays everything that wasn't terminalized.

Real per-tx rejections from Teranode are terminal — the tx is genuinely invalid and retrying won't change that. `ErrTxExists` (returned as 200 OK by Teranode) is success. Cascade-rejection of children of a real-rejected parent continues as before.

API resubmission of a tx whose row already exists falls through the intake's dedup CAS and returns the current status (no special handling for retry-stuck txs).

### Mining and merkle-service callbacks

The merkle-service callback path is unchanged. `SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`, `STUMP`, and `BLOCK_PROCESSED` continue to land at the existing HTTP callback endpoint and write directly to the status store. The callback handler additionally pushes status flips onto the dispatcher's input channel so the dep index can release or cascade waiters as appropriate.

### Result classification

With #879 + #881, Teranode `/txs` returns one of:

- HTTP 200 + body `"OK"` → every tx in the batch was accepted. Every slot is `ACCEPTED_BY_NETWORK`.
- HTTP 500 + body containing per-slot lines → at least one tx had an error. Each line is either `"OK"` (slot succeeded) or `"<NAME> (<num>)"` (slot failed with that Teranode error code).
- HTTP 500 + no body (or unparseable body) → batch-level server failure. Retry forever, infra failure.

Per-slot Teranode codes are bucketed:

- `ACCEPTED_BY_NETWORK` — slot returned `"OK"` (including `ErrTxExists`/`TX_EXISTS (33)`, which Teranode treats as success).
- **Terminal REJECTED** — `TX_INVALID (31)`, `TX_INVALID_DOUBLE_SPEND (32)`, `TX_CONFLICTING (36)`, `TX_LOCKED (37)`, `TX_LOCK_TIME (35)`, `TX_POLICY (39)`, `TX_COINBASE_IMMATURE (38)`, `TX_MISSING_PARENT (34)`, `UTXO_FROZEN (72)`, `UTXO_SPENT (70)`, `UTXO_NON_FINAL (71)`, `UTXO_INVALID_SIZE (...)`, `INVALID_ARGUMENT (1)`. These are real per-tx rejections; cascade walks waiters.
- **Infra retry** — batch-level 500 with no per-slot info, network/timeout, or any per-slot line that classifies as infra (e.g. `PROCESSING (4)` from the default branch).

`TX_MISSING_PARENT (34)` is terminal in this design — dep-aware dispatch already gated children on in-flight parents, so a missing-parent rejection from Teranode means a parent that arcade doesn't know about. Wallets resolve it.

Single-tx `/tx` returns the same Teranode code in the response body when there's an error, with the HTTP status mirroring the classification (400/403/409/422/500). Arcade keeps the per-tx `/tx` path for non-batch use cases but the `/txs` failure path no longer falls back to it.

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

- Uses a single-partition propagation topic
- Performs all intake work synchronously, including validation
- Runs the dispatcher inside the propagator (single state-owning goroutine, channel-fed) in place of the old parallel consumer pool
- Defers Kafka offset commits to a watermark driven by the dispatcher's `offsetTracker`
- Retries infrastructure failures by requeuing through the dispatcher with a short flat delay (`requeueDelay`, currently 2s); no `PENDING_RETRY` status
- Keeps the reaper, narrowed to rebroadcasting stale `SEEN_ON_NETWORK`/`SEEN_MULTIPLE_NODES` rows past `staleSeenOnNetworkAge` (1h)
- Parses Teranode `/txs` per-slot response for per-tx classification (no per-tx `/tx` fallback)

The old `TopicTransaction` and any prior PENDING_RETRY rows can be cleaned up after deploy.

### Dependencies

This plan depends on two upstream Teranode PRs:

- [bsv-blockchain/teranode#879](https://github.com/bsv-blockchain/teranode/pull/879) — `/txs` processes the batch concurrently with per-submission slots.
- [bsv-blockchain/teranode#881](https://github.com/bsv-blockchain/teranode/pull/881) — `/txs` response body emits per-slot lines in submission order so arcade can attribute results to specific txids. Also unifies the single-tx and batch error format on Teranode error codes (`"NAME (num)"`).

The arcade-side parsing in `teranode/client.go` is written against the post-#881 format. If #881 isn't merged at deploy time, the parsing falls back to treating the batch result as binary (any 500 → retry), which is degraded but not broken.

## Install base note

The current Arcade install base is limited (essentially just GorillaPool's production deployment). The cost of breaking backward compatibility is minimal. Anyone running their own Arcade would need to follow the drain + redeploy sequence above; there is no rolling upgrade path.

## Non-goals

- Handling parent-child relationships where the parent was not submitted through Arcade
- Reordering or holding the merkle-service callback path
- Replacing the existing JSON message format (deferred to a separate optimization)
- Continuing to support the existing `tx_validator` service in any form
- Maintaining backward compatibility with the old `TopicTransaction` / `TopicPropagation` topic layout
- Handling stale `SENT_TO_NETWORK` rows (the status is unused in today's code; a separate effort can address the underlying gap if needed)
