## Why

The batch transaction endpoint (`POST /txs`) processes 100 transactions serially: each tx is parsed, individually published to Kafka via `SyncProducer`, consumed one-by-one through the validator, published again to the propagation topic, consumed again one-by-one, registered with the merkle service individually, and broadcast to teranode endpoints one at a time. This serial pipeline means 100 transactions require ~200 synchronous Kafka round-trips, ~100 sequential HTTP calls to merkle service, and ~100 sequential HTTP calls per teranode endpoint. For maximum throughput, the batch should flow through the system as a batch — not be shredded into individual messages at the API boundary.

## What Changes

- **Add `SendBatch` to kafka Producer** — accepts a slice of messages and publishes them in a single Kafka batch call using `SendMessages`, eliminating per-message round-trip overhead at both the API→validator and validator→propagation boundaries
- **Batch-publish in `handleSubmitTransactions`** — parse all transactions upfront, then publish the entire batch to `arcade.transaction` in one `SendBatch` call instead of looping with individual `Send` calls
- **Add batch-aware propagation handler** — accumulate messages into micro-batches (configurable window/size), register all txids with merkle service concurrently (bounded goroutine pool), then broadcast all raw transactions to each teranode endpoint using the existing `SubmitTransactions` (`POST /txs`) batch endpoint in a single HTTP call per endpoint
- **Add `RegisterBatch` to merkle service client** — accepts a slice of txid/callback pairs and registers them concurrently with bounded parallelism, returning on first error (fail-fast for mandatory registration)
- **Batch-publish in tx-validator** — after validating transactions, publish all propagation messages in one `SendBatch` call instead of individual `Send` calls per transaction

## Capabilities

### New Capabilities
- `batch-pipeline`: End-to-end batch processing from API ingestion through Kafka, validation, merkle registration, and teranode broadcast — covering the batch Kafka producer, concurrent merkle registration, and batch teranode submission

### Modified Capabilities
- `propagation`: Propagator gains batch-aware message handling with configurable micro-batching (batch size + flush interval), concurrent merkle registration with bounded parallelism, and single-call batch broadcast to teranode `/txs` endpoint
- `api-server`: Batch endpoint switches from per-tx Kafka publishes to single batch publish
- `tx-validator`: Validator batches propagation publishes instead of per-tx Kafka sends

## Impact

- **kafka/producer.go**: New `SendBatch` method using `syncProducer.SendMessages`
- **merkleservice/client.go**: New `RegisterBatch` method with concurrent bounded registration
- **services/api_server/handlers.go**: `handleSubmitTransactions` refactored to batch-publish
- **services/tx_validator/validator.go**: Propagation publishing batched
- **services/propagation/propagator.go**: New batch-aware handler with micro-batching, concurrent merkle registration, batch broadcast via `SubmitTransactions`
- **config/config.go**: New `Propagation` config section for batch size, flush interval, and merkle concurrency
- **No breaking API changes** — the `POST /txs` request/response format is unchanged
- **No new dependencies** — uses existing `sync`, `context`, and sarama batch APIs
