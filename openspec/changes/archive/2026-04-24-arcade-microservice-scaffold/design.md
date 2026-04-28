## Context

Arcade is a transaction management system that registers transactions with a merkle-service, receives state callbacks, and constructs merkle proofs (BUMPs) when transactions are included in blocks. The existing implementation on the `merkle-service-integration` branch is functionally complete but breaks under production load — specifically when processing high volumes of merkle-service callbacks alongside transaction propagation. It also lacks error recovery, meaning failed processing steps cause silent data loss.

This redesign follows Teranode's proven patterns for high-throughput blockchain services: Kafka-driven event pipelines, batched Aerospike operations, and a modular service architecture that can run all-in-one or as independent microservices.

**Key constraints:**
- Blocks contain millions of transactions
- Subtrees contain thousands of transactions
- Arcade may register tens of thousands of transactions per block
- All Aerospike calls must be batched (per Teranode blockassembler patterns)
- Must follow Teranode/Merkle Service daemon patterns

## Goals / Non-Goals

**Goals:**
- Handle production-scale callback volumes without degradation
- Decouple all services via Kafka so each can scale independently
- Batch all database operations for throughput
- Provide robust error handling with retries and dead-letter queues
- Support both all-in-one and microservice deployment modes
- Reuse proven patterns from Teranode codebase

**Non-Goals:**
- Reimplementing the merkle-service itself (it's a separate project)
- Changing the BUMP/STUMP specification format (BRC-0074)
- Building a custom P2P implementation (use existing Teranode libp2p client)
- UI or monitoring dashboards (operational tooling is separate)
- Wallet or key management functionality

## Decisions

### 1. Go with Kafka for inter-service messaging
**Choice:** Apache Kafka with dedicated topics per event type
**Rationale:** Teranode already uses Kafka successfully at scale. Kafka provides durable, ordered event streams with consumer groups for horizontal scaling. Each service becomes a producer or consumer (or both), enabling independent scaling.
**Alternatives considered:**
- Direct gRPC calls between services — rejected because tight coupling prevents independent scaling and creates cascading failures under load (the exact problem we're fixing)
- NATS/Redis streams — viable but Kafka aligns with Teranode patterns and handles the throughput requirements with proven durability

### 2. Aerospike with batched operations for all data persistence
**Choice:** Aerospike with batch read/write for all database calls
**Rationale:** Teranode's blockassembler demonstrates that batching Aerospike operations is critical for handling millions of records per block. Individual calls cannot scale to the required throughput.
**Alternatives considered:**
- PostgreSQL — cannot handle the write throughput for millions of transactions per block
- ScyllaDB/Cassandra — viable but Aerospike is already proven in the Teranode ecosystem with established patterns to reuse

### 3. Monorepo with service-per-directory structure
**Choice:** Single Go module with `cmd/` entrypoints and `services/` packages, following Teranode layout
**Rationale:** Enables code sharing (models, Kafka helpers, Aerospike clients) while keeping services independently deployable. A single `arcade` binary can run all services or a subset via configuration.
**Alternatives considered:**
- Separate repos per service — too much overhead for a small team, harder to share models and utilities
- Go workspaces — unnecessary complexity when a single module works

### 4. Consumer group pattern for Kafka scaling
**Choice:** Each service type uses a dedicated Kafka consumer group. Multiple instances of the same service share work via consumer group partitioning.
**Rationale:** This is how Teranode scales — you add more instances of a service and Kafka automatically distributes partitions. No custom load balancing needed.

### 5. API server as thin gateway
**Choice:** The API server does no heavy processing — it validates incoming requests, publishes Kafka messages, and returns. All actual work happens in downstream services.
**Rationale:** This is the root cause of the current scaling problems. When the API server processes callbacks inline, it bottlenecks under load. By offloading to Kafka, the API server can handle high callback volumes with minimal latency.

### 6. Error handling with retries and dead-letter topics
**Choice:** Failed Kafka message processing retries with exponential backoff, then routes to a dead-letter topic after max retries. Services expose health endpoints for orchestration.
**Rationale:** The current implementation silently drops failed operations. Dead-letter topics ensure nothing is lost and allow manual or automated recovery.

### 7. Configuration via environment variables with config file fallback
**Choice:** Viper-based configuration supporting env vars, config files, and CLI flags — matching Teranode patterns.
**Rationale:** Env vars work naturally in containerized deployments (K8s). Config files work for local development and all-in-one mode.

## Architecture Overview

```
                    ┌─────────────────────┐
                    │   Merkle Service    │
                    │  (external system)  │
                    └─────────┬───────────┘
                              │ callbacks
                              ▼
┌──────────────────────────────────────────────────────────┐
│                      API SERVER                          │
│  POST /callback  →  validate  →  publish to Kafka       │
│  POST /tx        →  validate  →  publish to Kafka       │
│  GET  /tx/:id    →  query Aerospike  →  respond         │
└──────────┬──────────────────────────────┬────────────────┘
           │                              │
           ▼                              ▼
    ┌──────────────┐              ┌──────────────┐
    │ Kafka: stump │              │ Kafka: tx    │
    │ Kafka: block │              │              │
    │   _processed │              │              │
    └──────┬───────┘              └──────┬───────┘
           │                             │
           ▼                             ▼
    ┌──────────────┐              ┌──────────────┐
    │ BUMP Builder │              │ TX Validator  │
    │              │              │              │
    │ collect      │              │ validate     │
    │ STUMPs →     │              │ → propagate  │
    │ build BUMP → │              │ → reject     │
    │ store proofs │              │              │
    └──────────────┘              └──────┬───────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │ Propagation  │
                                  │              │
                                  │ broadcast to │
                                  │ datahub URLs │
                                  └──────────────┘

    ┌──────────────┐
    │  P2P Client  │──── block ────→ Kafka: block
    │ (libp2p)     │
    └──────────────┘
           │
           ▼
    ┌──────────────┐
    │Block Processor│
    │              │
    │ fetch block  │
    │ from datahub │
    │ store model  │
    └──────────────┘

    ┌─────────────────────────────────┐
    │         AEROSPIKE               │
    │  transactions | blocks | stumps │
    │  bumps | merkle_proofs          │
    │  (all batched operations)       │
    └─────────────────────────────────┘
```

## Data Flow

1. **Transaction submission:** Client → API Server → Kafka `transaction` topic → TX Validator → (valid) Propagation → datahub URLs
2. **Transaction registration:** After propagation, register TXID + callback URL with merkle-service
3. **Callback processing:** Merkle-service → API Server callback → Kafka topic per state → downstream service updates Aerospike
4. **Block arrival:** P2P network → P2P Client → Kafka `block` topic → Block Processor → fetch from datahub → store in Aerospike
5. **BUMP construction:** BLOCK_PROCESSED callback → Kafka `block_processed` topic → BUMP Builder → collect STUMPs → build BUMP → store proofs → prune STUMPs

## Project Structure

```
arcade/
├── cmd/
│   └── arcade/
│       └── main.go              # Single binary entrypoint
├── config/
│   └── config.go                # Viper-based configuration
├── services/
│   ├── api_server/
│   │   ├── server.go            # HTTP server setup
│   │   ├── handlers.go          # Route handlers
│   │   └── routes.go            # Route definitions
│   ├── p2p_client/
│   │   └── client.go            # libp2p block listener
│   ├── block_processor/
│   │   └── processor.go         # Block fetch & store
│   ├── bump_builder/
│   │   └── builder.go           # STUMP → BUMP construction
│   ├── tx_validator/
│   │   └── validator.go         # Transaction validation
│   └── propagation/
│       └── propagator.go        # Datahub broadcasting
├── store/
│   ├── aerospike.go             # Aerospike client with batching
│   ├── transactions.go          # Transaction CRUD
│   ├── blocks.go                # Block storage
│   ├── stumps.go                # STUMP storage & pruning
│   └── bumps.go                 # BUMP & merkle proof storage
├── kafka/
│   ├── producer.go              # Shared Kafka producer
│   ├── consumer.go              # Consumer group helpers
│   └── topics.go                # Topic constants
├── models/
│   ├── transaction.go           # Transaction + state model
│   ├── block.go                 # Block model (Teranode format)
│   ├── stump.go                 # STUMP model
│   ├── bump.go                  # BUMP model
│   └── callback.go              # Merkle-service callback model
├── go.mod
├── go.sum
├── Dockerfile
└── docker-compose.yml
```

## Risks / Trade-offs

- **[Kafka dependency]** → All services depend on Kafka availability. Mitigation: Kafka is deployed as a replicated cluster; services implement health checks and reconnection logic.
- **[Aerospike batching complexity]** → Batch operations require careful error handling (partial batch failures). Mitigation: Follow Teranode blockassembler patterns which already handle this; wrap in a store layer that retries failed items.
- **[STUMP ordering for BUMP construction]** → STUMPs may arrive before BLOCK_PROCESSED or vice versa. Mitigation: BUMP Builder collects STUMPs as they arrive and only builds when BLOCK_PROCESSED signals readiness; missing STUMPs trigger a retry/wait.
- **[P2P client reliability]** → libp2p connections can drop. Mitigation: Reconnection logic with backoff; block gaps detected by Block Processor and re-fetched.
- **[All-in-one vs microservice mode]** → Supporting both modes adds code complexity. Mitigation: Each service implements a common `Service` interface with `Start()`/`Stop()`; the main binary selects which to run via config. This is the same pattern Teranode uses.

## Migration Plan

This is a greenfield reimplementation, not an in-place migration. Deployment steps:
1. Deploy infrastructure (Kafka, Aerospike) alongside existing Arcade
2. Deploy new Arcade services pointing at same datahub URLs and merkle-service
3. Run both systems in parallel, comparing outputs
4. Cut over transaction registration to new Arcade
5. Decommission old Arcade after verification

Rollback: Re-point merkle-service callbacks to old Arcade instance.

## Open Questions

- What Kafka client library to use — Sarama vs confluent-kafka-go? (Recommend aligning with whatever Teranode uses)
- Exact Aerospike namespace/set naming conventions — follow Teranode or define new?
- Should the propagation service confirm delivery to datahub, or fire-and-forget with retries?
- What monitoring/metrics framework to use (Prometheus, OpenTelemetry)?
