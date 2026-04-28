## Context

The scaffold has the correct service boundaries and Kafka/Aerospike infrastructure, but the core business logic is placeholder code. The reference implementation on the `merkle-service-integration` branch is the source of truth for how Arcade works — it's feature-complete but doesn't scale. Our job is to port the exact business logic into the scalable microservice architecture.

Key differences between reference and scaffold:
- Reference uses `go-sdk/transaction.MerklePath` for BRC-74 binary encoding; scaffold uses custom JSON structs
- Reference STUMPs are keyed by `(blockHash, subtreeIndex)` with binary data; scaffold keys by `(blockHash, TXID)` with JSON
- Reference has full transaction validation (policy, fees, scripts, SPV); scaffold has a length check stub
- Reference callback handler processes `CallbackMessage` with `Type` field; scaffold uses `CallbackPayload` with `State` field
- Reference uses `TxTracker` for O(1) identification of tracked transactions from STUMP hashes
- Reference fetches subtree hashes + coinbase BUMP from datahub JSON endpoint for BUMP construction
- Reference uses teranode client with `application/octet-stream` and bearer auth; scaffold uses JSON
- Reference has ARC-compatible error codes (460-475); scaffold has generic errors

## Goals / Non-Goals

**Goals:**
- Port the exact BUMP construction algorithm (`assembleFullPath`, `BuildCompoundBUMP`, `ExtractMinimalPath`, `applyCoinbaseToSTUMP`) from the reference
- Port the exact transaction validation logic from the reference validator
- Port the callback handler logic including STUMP processing with tracked tx discovery
- Port the ARC error codes and structured error responses
- Port the teranode datahub client and merkle-service client
- Add TxTracker for in-memory tx hash tracking
- Update store interface to match reference patterns
- Ensure BUMP construction tests match reference behavior exactly
- Keep the Kafka/Aerospike microservice architecture (don't regress to the reference's monolithic SQLite design)

**Non-Goals:**
- Porting the P2P block listener (remains a hook point — the go-teranode-p2p-client integration is a separate concern)
- Porting the webhook/SSE event delivery system (future enhancement)
- Porting the HTML dashboard or Swagger docs
- Porting the chaintracks integration (separate concern)
- Changing the service deployment model

## Decisions

### 1. Add go-sdk as a direct dependency for MerklePath and transaction parsing
**Choice:** Use `github.com/bsv-blockchain/go-sdk` for `transaction.MerklePath`, `transaction.NewTransactionFromBytes`, `chainhash.Hash`, `spv.Verify`, and script validation.
**Rationale:** The reference implementation depends entirely on go-sdk's MerklePath for BRC-74 binary encoding. Our custom PathNode JSON structs cannot interoperate with merkle-service's binary STUMP format. The SDK also provides transaction parsing, TXID computation, and SPV verification that we currently stub.
**Alternatives:** Implement BRC-74 binary encoding ourselves — rejected because go-sdk is the canonical implementation and the reference already uses it.

### 2. Keep Kafka for inter-service communication, port business logic into service handlers
**Choice:** The BUMP construction, validation, and callback processing logic lives in the service packages but uses the same algorithms as the reference. Kafka decouples the services; the business logic within each service matches the reference.
**Rationale:** The whole point of this refactor is scaling via Kafka. The reference's monolithic approach (callback handler directly calls ConstructBUMPsForBlock) is what broke under load. We keep Kafka between services but ensure the logic inside each service is correct.

### 3. Replace STUMP storage model to match reference (keyed by subtreeIndex)
**Choice:** STUMPs stored as `(blockHash, subtreeIndex)` with binary stump data, matching the reference's `models.Stump` struct.
**Rationale:** Merkle-service sends one STUMP per subtree, not per transaction. A single STUMP contains multiple transaction hashes at level 0. The reference uses `TxTracker.FilterTrackedHashes()` to discover which level-0 hashes are tracked. Our current per-TXID model is fundamentally wrong — it would require decomposing each STUMP, losing the subtree structure needed for BUMP construction.

### 4. Add TxTracker as a shared in-memory component
**Choice:** Port `store/tracker.go` — an in-memory concurrent hash map that supports O(1) lookup of tracked transaction hashes. Populated on service startup from the store, updated as transactions are added/status changes.
**Rationale:** BUMP construction needs to identify which level-0 hashes in a STUMP are transactions we care about. Without TxTracker, we'd need to query the database for every hash in every STUMP — infeasible at scale with millions of transactions per block.

### 5. Port reference code directly where possible
**Choice:** Copy the BUMP construction functions (`assembleFullPath`, `BuildCompoundBUMP`, `ExtractMinimalPath`, `applyCoinbaseToSTUMP`, `computeCorrectedSubtreeRoot`), error codes, validator checks, and client code directly from the reference, adapting only for the microservice context (Kafka message handling, Aerospike storage).
**Rationale:** The user owns both codebases. The reference code is tested and correct. Rewriting it risks introducing bugs in complex merkle tree math.

## Risks / Trade-offs

- **[go-sdk version compatibility]** → The reference uses specific go-sdk APIs (`MerklePath.ComputeMissingHashes`, `MerklePath.AddLeaf`, `FindLeafByOffset`). Mitigation: Pin to the same go-sdk version the reference uses.
- **[TxTracker memory usage]** → At tens of thousands of registered transactions, the in-memory tracker is fine. At millions, it could consume significant memory. Mitigation: Same scale as reference; if needed, can shard or use bloom filters later.
- **[STUMP storage migration]** → Changing from per-TXID to per-subtreeIndex storage is a breaking schema change. Mitigation: This is pre-production; no data migration needed.
- **[Callback format change]** → Switching from `CallbackPayload` to `CallbackMessage` changes the API contract. Mitigation: The `CallbackMessage` format is what merkle-service actually sends; our old format was speculative.
