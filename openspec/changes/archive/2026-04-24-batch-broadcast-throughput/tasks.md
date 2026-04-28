## 1. Config and Shared Infrastructure

- [x] 1.1 Add `Propagation` config struct to `config/config.go` with `BatchSize` (default 100), `FlushIntervalMs` (default 50), and `MerkleConcurrency` (default 10). Wire defaults in `setDefaults()`.
- [x] 1.2 Add `SendBatch(topic string, msgs []KeyValue) error` method to `kafka/producer.go` using `syncProducer.SendMessages`. Add `KeyValue` struct with `Key string` and `Value any` fields.
- [x] 1.3 Add tests for `SendBatch` in `kafka/producer_test.go` — verify batch publish success, empty batch returns nil, and error propagation.

## 2. Merkle Service Batch Registration

- [x] 2.1 Add `Registration` struct and `RegisterBatch(ctx context.Context, registrations []Registration, maxConcurrency int) error` to `merkleservice/client.go`. Uses bounded goroutine pool with `errgroup` and semaphore. Returns on first error with context cancellation.
- [x] 2.2 Add tests for `RegisterBatch` in `merkleservice/client_test.go` — verify all succeed, fail-fast on error, concurrency bounded, empty slice returns nil.

## 3. API Server Batch Publish

- [x] 3.1 Refactor `handleSubmitTransactions` in `services/api_server/handlers.go` to parse all transactions first into a slice, then publish all messages in one `producer.SendBatch` call. On parse failure mid-stream, return 400 without publishing. On batch publish failure, return 500.
- [x] 3.2 Add tests for `handleSubmitTransactions` in `services/api_server/handlers_test.go` — verify batch publish of 100 txs, parse failure publishes nothing, Kafka failure returns 500.

## 4. TX Validator Batch Propagation Publish

- [x] 4.1 Add batch accumulation to the tx-validator: collect validated propagation messages during claim processing and publish them via `SendBatch` at the end instead of individual `Send` calls per transaction. Messages that fail validation are still handled individually (status update to REJECTED).
- [x] 4.2 Add tests for batch propagation publish in `services/tx_validator/validator_test.go` — verify 100 validated txs produce one `SendBatch` call, mixed pass/fail batch excludes failures, single tx works.

## 5. Propagation Micro-Batching

- [x] 5.1 Add batch-aware handler to `services/propagation/propagator.go`: replace single-message `handleMessage` with a micro-batch accumulator bounded by `BatchSize` and `FlushInterval`. On flush, call `processBatch` which: (a) registers all txids with merkle via `RegisterBatch`, (b) concatenates raw txs and broadcasts via `SubmitTransactions` per endpoint, (c) calls `UpdateStatus` for each tx.
- [x] 5.2 Preserve the existing single-message `handleMessage` for backward compatibility — micro-batching wraps around it. When `BatchSize` is 1 or config is absent, behavior matches current single-message flow.
- [x] 5.3 Update `propagator_test.go` — add tests for: batch of 100 txs all registered then broadcast in single call, merkle failure aborts entire batch (no broadcast), flush interval triggers partial batch, single message still works, nil merkle client skips registration for batch.

## 6. Integration and Wiring

- [x] 6.1 Wire `Propagation` config into `Propagator.New()` and `Start()` — pass batch size, flush interval, and merkle concurrency from config.
- [x] 6.2 Verify `go build ./...` succeeds and `go test ./...` passes with no regressions.
- [x] 6.3 Add end-to-end batch test: construct 100 raw transactions, submit via `handleSubmitTransactions`, verify all 100 reach the propagation mock servers in batched calls (single Kafka batch, concurrent merkle registration, single teranode POST).
