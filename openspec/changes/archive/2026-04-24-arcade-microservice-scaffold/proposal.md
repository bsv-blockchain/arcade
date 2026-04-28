## Why

The existing Arcade implementation (merkle-service-integration branch) is feature-complete but cannot handle production-scale workloads. Specifically, it fails under the volume of merkle-service callback API responses combined with concurrent transaction propagation. It also lacks proper error handling for failed processing steps, leading to silent data loss. This reimplementation restructures Arcade as a proper microservice architecture — following Teranode patterns — to support blocks with millions of transactions, subtrees with thousands of transactions, and tens of thousands of registered transactions per block.

## What Changes

- **New Go project scaffold** following the Teranode daemon/service pattern with support for running all services in a single binary or independently
- **API Server service** — lightweight HTTP server handling merkle-service callbacks (REJECTED, SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, STUMP, BLOCK_PROCESSED) and transaction GET/POST endpoints; delegates all heavy work to Kafka
- **P2P Client service** — listens for new blocks via Teranode libp2p and publishes block notifications to Kafka
- **Block Processor service** — consumes block notifications from Kafka, fetches full block data from datahub URLs, stores block models
- **BUMP Builder service** — triggered on BLOCK_PROCESSED events, collects STUMPs for a block, constructs full BUMPs using coinbase merkle path, stores per-transaction merkle proofs, prunes STUMPs
- **TX Validator service** — validates incoming transactions, routes valid ones to propagation, rejects invalid ones
- **Propagation service** — broadcasts validated transactions to all configured datahub URLs
- **Kafka messaging layer** — topics for block, stump, block_processed, and transaction events to decouple all services
- **Aerospike database layer** — batched operations for all DB calls (following Teranode blockassembler patterns) storing transactions, states, STUMPs, BUMPs, and blocks
- **Robust error handling** — retry logic, dead-letter queues, and proper failure propagation across all services

## Capabilities

### New Capabilities
- `api-server`: HTTP server with merkle-service callback endpoint and transaction CRUD endpoints
- `p2p-client`: Block listener using Teranode libp2p, publishes to Kafka
- `block-processor`: Fetches and stores full block models from datahub
- `bump-builder`: Constructs full BUMPs from STUMPs and coinbase merkle path
- `tx-validator`: Transaction validation pipeline
- `propagation`: Transaction broadcasting to datahub URLs
- `kafka-messaging`: Kafka topic definitions, producers, and consumers for inter-service communication
- `database-layer`: Aerospike schema, batched operations, and data access patterns
- `service-scaffold`: Project structure, configuration, all-in-one vs microservice deployment modes

### Modified Capabilities
_(none — this is a greenfield reimplementation)_

## Impact

- **Code**: Entirely new Go codebase structured as independent services under a monorepo
- **APIs**: New HTTP callback endpoint for merkle-service; new GET/POST transaction endpoints
- **Dependencies**: Go modules for Kafka (confluent/sarama), Aerospike Go client, Teranode libp2p client, go-p2p-message-bus
- **Infrastructure**: Requires Kafka cluster, Aerospike cluster, datahub endpoints, P2P network access
- **Deployment**: Supports both all-in-one binary and independent microservice containers
