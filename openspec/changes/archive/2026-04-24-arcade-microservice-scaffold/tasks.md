## 1. Project Scaffold & Configuration

- [x] 1.1 Initialize Go module (`go.mod`) with module path and add core dependencies (Kafka client, Aerospike client, Viper, structured logger)
- [x] 1.2 Create project directory structure: `cmd/arcade/`, `config/`, `services/`, `store/`, `kafka/`, `models/`
- [x] 1.3 Implement `config/config.go` — Viper-based configuration with env vars, YAML file, and CLI flag support. Define all config keys (Kafka brokers, Aerospike hosts, datahub URLs, service mode, log level, callback URL)
- [x] 1.4 Implement the common `Service` interface (`Start(ctx) error`, `Stop() error`) in a shared package
- [x] 1.5 Implement `cmd/arcade/main.go` — single binary entrypoint that reads `--mode` flag and starts selected services (or all). Wire graceful shutdown on SIGTERM/SIGINT
- [x] 1.6 Create `Dockerfile` and `docker-compose.yml` with Kafka, Aerospike, and arcade service definitions

## 2. Shared Models

- [x] 2.1 Define `models/transaction.go` — Transaction struct with TXID, raw data, state enum (REJECTED, SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, STUMP, BLOCK_PROCESSED), merkle proof, timestamps, rejection reason
- [x] 2.2 Define `models/block.go` — Block struct following Teranode block definition (hash, header, height, coinbase tx, previous hash, timestamp)
- [x] 2.3 Define `models/stump.go` — STUMP struct (block hash, TXID, subtree merkle path data)
- [x] 2.4 Define `models/bump.go` — BUMP struct per BRC-0074 (block hash, full merkle path)
- [x] 2.5 Define `models/callback.go` — Merkle-service callback request struct (TXID, state, block hash, STUMP data, rejection reason)

## 3. Kafka Messaging Layer

- [x] 3.1 Define topic constants in `kafka/topics.go` — block, stump, block_processed, transaction, propagation, and corresponding DLQ topics
- [x] 3.2 Implement `kafka/producer.go` — shared Kafka producer with sync/async modes, serialization, retry logic, and error handling for broker unavailability
- [x] 3.3 Implement `kafka/consumer.go` — consumer group wrapper with configurable group ID, partition handling, dead-letter routing after max retries, and graceful shutdown

## 4. Database Layer (Aerospike)

- [x] 4.1 Implement `store/aerospike.go` — Aerospike client wrapper with connection pooling, health check method, and configurable batch sizes
- [x] 4.2 Implement `store/transactions.go` — batch CRUD for transactions: Create, GetByTXID, BatchGet, UpdateState, AttachProof
- [x] 4.3 Implement `store/blocks.go` — block storage: Store (with batched tx index writes), GetByHash
- [x] 4.4 Implement `store/stumps.go` — STUMP storage: Store, GetByBlockHash (all STUMPs for a block), PruneByBlockHash (batched delete)
- [x] 4.5 Implement `store/bumps.go` — BUMP storage: Store, GetByBlockHash

## 5. API Server Service

- [x] 5.1 Implement `services/api_server/server.go` — HTTP server setup with health/readiness endpoints
- [x] 5.2 Implement `services/api_server/routes.go` — route definitions for POST `/callback`, GET `/tx/:txid`, POST `/tx`
- [x] 5.3 Implement callback handler — validate payload, publish to appropriate Kafka topic (stump, block_processed, or transaction) based on callback state, return 200/400
- [x] 5.4 Implement GET `/tx/:txid` handler — query Aerospike for transaction by TXID, return state and proof data (200/404)
- [x] 5.5 Implement POST `/tx` handler — parse single or batch transactions, publish each to `transaction` Kafka topic, return 202 with TXIDs

## 6. P2P Client Service

- [x] 6.1 Implement `services/p2p_client/client.go` — connect to BSV network using Teranode libp2p client, listen for block announcements
- [x] 6.2 Publish block notifications to `block` Kafka topic with block hash, height, previous hash, timestamp
- [x] 6.3 Add duplicate block detection (dedup by block hash within a time window)
- [x] 6.4 Add reconnection logic with exponential backoff on P2P connection loss

## 7. Block Processor Service

- [x] 7.1 Implement `services/block_processor/processor.go` — consume from `block` Kafka topic
- [x] 7.2 Fetch full block data from configured datahub URLs with failover (try each URL in sequence)
- [x] 7.3 Store block model in Aerospike using batched writes for transaction index
- [x] 7.4 Add idempotency check — skip blocks already stored in Aerospike
- [x] 7.5 Add retry with exponential backoff and dead-letter routing on persistent fetch failures

## 8. TX Validator Service

- [x] 8.1 Implement `services/tx_validator/validator.go` — consume new transaction messages from `transaction` Kafka topic
- [x] 8.2 Validate transaction data and store valid transactions in Aerospike (batched writes)
- [x] 8.3 Publish valid transactions to `propagation` Kafka topic
- [x] 8.4 Handle invalid transactions — update state to REJECTED in Aerospike with reason
- [x] 8.5 Process state update messages (SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES) — update transaction state in Aerospike
- [x] 8.6 Add duplicate detection — return existing state for already-known transactions

## 9. Propagation Service

- [x] 9.1 Implement `services/propagation/propagator.go` — consume from `propagation` Kafka topic
- [x] 9.2 Broadcast transactions to all configured datahub URLs concurrently
- [x] 9.3 Register transaction with merkle-service (TXID + callback URL) after successful propagation
- [x] 9.4 Add retry with exponential backoff for failed datahub sends and merkle-service registration
- [x] 9.5 Route persistently failed messages to dead-letter topic

## 10. BUMP Builder Service

- [x] 10.1 Implement `services/bump_builder/builder.go` — consume from `block_processed` Kafka topic
- [x] 10.2 Retrieve all stored STUMPs for the block from Aerospike (batched read)
- [x] 10.3 Fetch coinbase merkle path from stored block data
- [x] 10.4 Construct full BUMP from STUMPs + coinbase merkle path per BRC-0074
- [x] 10.5 Batch-write individual merkle proofs to each registered transaction in Aerospike
- [x] 10.6 Prune STUMPs for the block after successful BUMP construction (batched delete)
- [x] 10.7 Handle late-arriving STUMPs — re-queue check for transactions still missing proofs

## 11. Structured Logging & Observability

- [x] 11.1 Set up structured JSON logger with configurable log levels across all services
- [x] 11.2 Add correlation ID propagation (TXID, block hash) to all log entries within processing chains
- [x] 11.3 Add health (`/health`) and readiness (`/ready`) HTTP endpoints to all non-API services

## 12. Integration & Testing

- [x] 12.1 Write unit tests for BUMP construction logic (STUMP + coinbase path → full BUMP)
- [x] 12.2 Write unit tests for Kafka message serialization/deserialization
- [x] 12.3 Write unit tests for Aerospike batch operation wrappers
- [x] 12.4 Write integration tests for API server callback → Kafka → state update flow
- [x] 12.5 Write integration tests for full transaction lifecycle (submit → validate → propagate → register → callbacks → BUMP)
