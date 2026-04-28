## Why

The scaffold implementation has the correct microservice architecture, but the core business logic diverges significantly from the working reference implementation (`merkle-service-integration` branch). The BUMP construction algorithm is a naive merge that doesn't handle subtree offset shifting, coinbase placeholder replacement, or BRC-74 binary encoding. Transaction validation is a stub. The callback handler doesn't match the merkle-service callback protocol. The store layer, datahub client, and merkle-service client are all structurally different from the reference. These gaps must be closed before the system can process real transactions.

## What Changes

- **Replace BUMP construction** with the exact algorithm from the reference: `assembleFullPath()` with subtree offset shifting, `BuildCompoundBUMP()` for multi-STUMP merging, `ExtractMinimalPath()` for per-tx proofs, and `applyCoinbaseToSTUMP()` for coinbase placeholder handling. Use `go-sdk/transaction.MerklePath` for BRC-74 binary format instead of custom JSON PathNode structs.
- **Replace STUMP model and storage** — STUMPs are keyed by `(blockHash, subtreeIndex)` with binary stump data, not by `(blockHash, TXID)` with JSON path arrays
- **Add TxTracker** — in-memory O(1) hash lookup to identify which level-0 STUMP hashes are tracked transactions (ported from `store/tracker.go`)
- **Replace callback handler** to use `CallbackMessage` format with `Type` field, bearer token auth, proper STUMP handling (extract level-0 hashes, filter tracked, update status), and direct BUMP construction on BLOCK_PROCESSED
- **Add ARC-compatible error codes** (460-475 range) from `errors/errors.go` for standardized error responses
- **Replace transaction validation** with proper go-sdk validation: policy checks (inputs, outputs, sigops, push data, size), fee validation via SPV verify, script validation, BEEF format parsing
- **Replace datahub client** — use `application/octet-stream` for tx submission to `/tx` and `/txs` endpoints with bearer token auth, matching the teranode client pattern
- **Replace merkle-service client** — POST to `/watch` endpoint with `{txid, callbackUrl}` JSON body
- **Update store interface** to match reference: `GetOrInsertStatus`, `UpdateStatus`, `SetMinedByTxIDs`, `InsertBUMP`/`GetBUMP` with binary data, `InsertStump`/`GetStumpsByBlockHash`/`DeleteStumpsByBlockHash` by subtreeIndex, block tracking methods, submission management
- **Add block data fetching for BUMP construction** — fetch subtree hashes and coinbase BUMP from datahub JSON/binary endpoints
- **Update API handlers** to use proper models, ARC error responses, and content-type handling (octet-stream, hex, JSON for tx submission)
- **Add comprehensive tests** for BUMP construction, callback processing, validation, and the full transaction lifecycle

## Capabilities

### New Capabilities
- `arc-errors`: ARC-compatible error codes and structured error responses
- `tx-tracker`: In-memory transaction hash tracker for O(1) STUMP filtering
- `bump-construction`: Full BUMP construction algorithm matching the reference (subtree offset shifting, coinbase replacement, compound BUMP, minimal path extraction)
- `teranode-client`: DataHub HTTP client for transaction broadcast and block data fetching
- `merkle-client`: Merkle Service HTTP client for transaction registration via `/watch`

### Modified Capabilities
- `api-server`: Update callback handler to use CallbackMessage format, add bearer auth, add ARC error responses, update tx submission to support octet-stream/hex/JSON
- `tx-validator`: Replace stub with proper go-sdk validation (policy, fees, scripts, BEEF parsing)
- `bump-builder`: Replace naive merge with reference BUMP construction using go-sdk MerklePath
- `database-layer`: Restructure store interface to match reference (status-based ops, binary BUMP/STUMP storage, subtreeIndex keying, block tracking, submissions)
- `propagation`: Use teranode client pattern (octet-stream, concurrent multi-endpoint broadcast) and merkle-service `/watch` registration

## Impact

- **Dependencies**: Add `github.com/bsv-blockchain/go-sdk` (chainhash, transaction, spv, script, interpreter, fee_model), remove custom PathNode/BUMP/STUMP JSON models
- **Store schema**: STUMPs change from `(blockHash:TXID)` key to `(blockHash, subtreeIndex)` key with binary data; BUMPs store binary not JSON; add submissions table; add processed_blocks table
- **API contract**: Callback endpoint changes from `CallbackPayload` to `CallbackMessage` format; tx submission supports multiple content types; error responses follow ARC format
- **Code**: Substantial changes to bump_builder, tx_validator, propagation, api_server, and store packages; new errors, tx_tracker packages
