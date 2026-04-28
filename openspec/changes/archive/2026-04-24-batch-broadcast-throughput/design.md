## Context

The current batch endpoint (`POST /txs`) parses concatenated raw transactions and publishes each one individually to Kafka via `producer.Send()` (synchronous, WaitForAll acks). Each message flows through the tx-validator consumer (one at a time), gets re-published to the propagation topic (again one at a time), then the propagation consumer registers with merkle service and broadcasts to teranode — all serially. For 100 transactions this means ~200 synchronous Kafka round-trips, ~100 sequential merkle HTTP calls, and ~100 sequential teranode HTTP calls per endpoint.

The teranode client already has a `SubmitTransactions` method that posts to `/txs` with concatenated bytes in a single HTTP call. The Sarama producer has `SendMessages` for batch publishing. These existing capabilities are unused in the batch path.

## Goals / Non-Goals

**Goals:**
- Minimize wall-clock time from `POST /txs` with 100 transactions to all 100 being broadcast on the network
- Batch Kafka publishes at both the API→validator and validator→propagation boundaries using `SendMessages`
- Register multiple txids with merkle service concurrently (bounded parallelism)
- Use the existing teranode `POST /txs` batch endpoint to broadcast all transactions in one HTTP call per endpoint
- Maintain the mandatory merkle registration guarantee (fail before broadcast)
- Keep backward compatibility: single-tx flow (`POST /tx`) unchanged, individual propagation messages still work

**Non-Goals:**
- Changing the Kafka topic structure or consumer group architecture
- Adding a new API endpoint — the existing `POST /txs` contract is unchanged
- Async/fire-and-forget semantics — we still want synchronous ack from Kafka
- Optimizing the tx-validator's parsing/validation logic itself
- Changing the merkle service or teranode server-side behavior

## Decisions

### 1. Batch Kafka publish via `SendBatch`

**Decision:** Add `SendBatch(topic string, msgs []KeyValue) error` to `kafka.Producer` using `syncProducer.SendMessages`.

**Rationale:** Sarama's `SendMessages` batches messages into fewer network round-trips based on `Producer.Flush.Messages` and `Producer.Flush.Bytes`. For 100 messages this reduces ~100 round-trips to ~1-2. The sync producer is kept (not async) because the API handler and validator both need to know if the publish succeeded before responding/proceeding.

**Alternative considered:** Using the async producer with a completion channel — rejected because error handling is complex and we need guaranteed delivery before returning 200 to the client.

### 2. Micro-batching in the propagation consumer

**Decision:** Add a batch-aware consumption mode to the propagator. Instead of processing one message at a time, accumulate messages up to `BatchSize` (default 100) or `FlushInterval` (default 50ms), then process the batch together: register all with merkle concurrently, then broadcast all to teranode in one call.

**Rationale:** The Kafka consumer receives messages one at a time via `ConsumeClaim`. Rather than changing the consumer framework, the propagator accumulates a micro-batch internally. The flush interval ensures low-latency for sparse traffic while the batch size caps memory usage.

**Alternative considered:** Changing the Kafka consumer to support batch handlers — rejected because it's a larger framework change that affects all consumers, and micro-batching at the handler level achieves the same throughput benefit with less blast radius.

### 3. Concurrent merkle registration with `RegisterBatch`

**Decision:** Add `RegisterBatch(ctx context.Context, registrations []Registration) error` to the merkle client. Uses a bounded goroutine pool (default concurrency: 10) to register txids in parallel. Returns on first error (fail-fast).

**Rationale:** The merkle service `/watch` endpoint accepts one txid per call — there's no batch endpoint. Concurrent registration with bounded parallelism is the best we can do without server-side changes. The fail-fast behavior preserves the mandatory registration guarantee: if any registration fails, no transactions in the batch are broadcast, and the entire batch is retried by Kafka.

**Alternative considered:** Sequential registration in a loop — rejected because 100 sequential HTTP calls at ~5ms each is 500ms, while 10-way concurrency brings it to ~50ms.

### 4. Batch broadcast via existing `SubmitTransactions`

**Decision:** After successful merkle registration of all txids in a batch, concatenate all raw transaction bytes and call `SubmitTransactions` (single `POST /txs` per endpoint) instead of individual `SubmitTransaction` calls.

**Rationale:** The teranode client already supports this. One HTTP call with 100 concatenated transactions is dramatically faster than 100 individual calls. The concurrent multi-endpoint fan-out is preserved.

### 5. Batch publish in tx-validator

**Decision:** The tx-validator continues to process messages one at a time (validation is CPU-bound and benefits from isolation), but accumulates validated propagation messages and publishes them in a batch using `SendBatch` when the current claim batch is processed.

**Rationale:** Validation failures are per-tx decisions that shouldn't block other transactions. But the Kafka publish at the end can be batched. This is a smaller change that avoids restructuring the validator's error handling while still eliminating per-tx Kafka round-trips for the validator→propagation hop.

**Alternative considered:** Full batch validation — rejected because a single bad transaction in the batch would complicate error reporting and retry logic.

## Risks / Trade-offs

- **[Batch failure atomicity]** If merkle registration succeeds for 50 of 100 txids and then fails, all 100 are retried — the 50 already-registered txids get re-registered. → **Mitigation:** Merkle service registration is idempotent (re-registering the same txid+callback is a no-op), so retries are safe.

- **[Increased memory per batch]** Accumulating 100 raw transactions in memory before broadcast. → **Mitigation:** At ~1KB average per transaction, 100 txs is ~100KB — negligible. The `BatchSize` config caps the maximum.

- **[Micro-batch latency for single transactions]** A single transaction sent via `POST /tx` flows through the propagation consumer and may wait up to `FlushInterval` (50ms) before being flushed. → **Mitigation:** The flush interval is short (50ms default) and configurable. Single-tx messages bypass batching when no other messages are pending (batch of 1 is still valid).

- **[Partial teranode broadcast failure]** If the batch POST to one endpoint fails, it's treated as a full failure for that endpoint (same as today's per-tx approach). → **Mitigation:** The multi-endpoint fan-out means success on any endpoint is sufficient. The "best status" aggregation logic is unchanged.
