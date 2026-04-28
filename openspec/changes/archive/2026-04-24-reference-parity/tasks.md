## 1. Add go-sdk Dependency & Update Models

- [x] 1.1 Add `github.com/bsv-blockchain/go-sdk` to go.mod (chainhash, transaction, spv, script, interpreter, fee_model packages)
- [x] 1.2 Remove custom `models/stump.go` and `models/bump.go` (PathNode, STUMP, BUMP structs) — these are replaced by `transaction.MerklePath` and `models.Stump`
- [x] 1.3 Update `models/callback.go` to use the `CallbackMessage` format with `Type` (CallbackType), `TxIDs`, `SubtreeIndex`, and `Stump` (HexBytes) fields — the user has already done this, verify it matches the reference
- [x] 1.4 Update `models/transaction.go` to use the full `TransactionStatus`/`Status` model with all states, `DisallowedPreviousStatuses`, `Submission`, `SubmitOptions`, `Policy`, `HexBytes`, and `BlockReorg` — the user has already done this, verify it matches the reference

## 2. ARC Error Codes

- [x] 2.1 Create `errors/errors.go` — port the complete `arcerrors` package from the reference: `StatusCode` type, all ARC codes (460-475), `ArcError` struct with `Error()`, `Unwrap()`, `ToErrorFields()`, `NewArcError`, `NewArcErrorWithInfo`, `GetArcError`, `NewErrorFields` with all status-to-message mappings, `ErrorFields` struct
- [x] 2.2 Write tests for ARC error creation, wrapping, extraction via `errors.As`, and `ErrorFields` generation for each status code

## 3. TxTracker

- [x] 3.1 Create `store/tracker.go` — port the `TxTracker` from the reference: concurrent map of `chainhash.Hash` to `Status`, methods `Add(txid string, status Status)`, `IsTracked(hash chainhash.Hash) bool`, `FilterTrackedHashes(hashes []chainhash.Hash) []chainhash.Hash`, `UpdateStatusHash(hash chainhash.Hash, status Status)`
- [x] 3.2 Write tests for TxTracker: Add/IsTracked, FilterTrackedHashes with mix of tracked and untracked hashes, UpdateStatusHash, concurrent access safety

## 4. Teranode DataHub Client

- [x] 4.1 Create `teranode/client.go` — port the exact teranode client from reference: `NewClient(endpoints, authToken)`, `SubmitTransaction(ctx, endpoint, rawTx) (statusCode, error)` posting `application/octet-stream` to `/tx`, `SubmitTransactions(ctx, endpoint, rawTxs) (statusCode, error)` posting concatenated bytes to `/txs`, `GetEndpoints() []string`, bearer token auth
- [x] 4.2 Write tests for teranode client: verify request content-type, URL construction, auth header, batch concatenation, status code handling

## 5. Merkle Service Client

- [x] 5.1 Create `merkleservice/client.go` — port the exact merkle-service client from reference: `NewClient(baseURL, authToken, timeout)`, `Register(ctx, txid, callbackURL) error` posting JSON `{txid, callbackUrl}` to `/watch`, bearer token auth, configurable timeout with 30s default
- [x] 5.2 Write tests for merkle-service client: verify request URL, body format, auth header, error on non-2xx response

## 6. BUMP Construction (Core Algorithm)

- [x] 6.1 Create `bump/bump.go` — port the complete BUMP construction package from the reference: `assembleFullPath(stumpData, subtreeIndex, subtreeHashes, coinbaseBUMP)`, `AssembleBUMP` (calls assembleFullPath + ExtractMinimalPath), `ExtractMinimalPath(fullPath, txOffset)`, `extractCoinbaseTxID(coinbaseBUMP)`, `applyCoinbaseToSTUMP(stumpPath, coinbaseTxID, coinbaseBUMP)`, `computeCorrectedSubtreeRoot(stumpData, coinbaseTxID)`, `BuildCompoundBUMP(stumps, subtreeHashes, coinbaseBUMP, tracker)`, `extractLevel0Hashes(stumpData)`
- [x] 6.2 Create `bump/datahub.go` — port block data fetching: `FetchBlockDataForBUMP(ctx, datahubURLs, blockHash)` returning `(subtreeHashes, coinbaseBUMP, error)`, trying JSON endpoint (`/block/{hash}?format=json`) first then binary fallback, `parseBlockJSONResponse` parsing subtrees array and coinbase_bump hex field
- [x] 6.3 Port `bump_test.go` tests from reference — test assembleFullPath with single and multi-subtree, coinbase replacement (full and minimal STUMP), ExtractMinimalPath, BuildCompoundBUMP with multiple STUMPs, and edge cases (empty STUMPs, missing coinbase)
- [x] 6.4 Write additional tests for FetchBlockDataForBUMP with mock HTTP server testing JSON and binary fallback

## 7. Transaction Validator

- [x] 7.1 Create `validator/validator.go` — port the full validator from reference: `NewValidator(policy, chainTracker)`, `ValidatePolicy(tx)` checking inputs/outputs/size/sigops/pushdata, `ValidateTransaction(ctx, tx, skipFees, skipScripts)` using `spv.Verify`, `wrapPolicyError` and `wrapSPVError` mapping to ARC error codes
- [x] 7.2 Port all validation helper functions: `checkInputs` (coinbase rejection, satoshi bounds), `checkOutputs` (dust limit, OP_RETURN zero-value, total bounds), `sigOpsCheck` (parsed script sigop counting), `pushDataCheck` (non-empty, push-only unlocking scripts)
- [x] 7.3 Write tests for validator: policy violations (too small, too large, no inputs, no outputs, empty unlocking script, non-push-only, sigops exceeded, bad output values), fee validation, and ARC error code mapping

## 8. Store Interface Refactor

- [x] 8.1 Define new `store/store.go` interface matching reference: `GetOrInsertStatus`, `UpdateStatus`, `GetStatus`, `GetStatusesSince`, `SetStatusByBlockHash`, `InsertBUMP`/`GetBUMP` (binary), `SetMinedByTxIDs`, `InsertSubmission`/`GetSubmissionsByTxID`/`GetSubmissionsByToken`, `UpdateDeliveryStatus`, `IsBlockOnChain`/`MarkBlockProcessed`/`MarkBlockOffChain`/`HasAnyProcessedBlocks`/`GetOnChainBlockAtHeight`, `InsertStump`/`GetStumpsByBlockHash`/`DeleteStumpsByBlockHash`, `Close`, `ErrNotFound`
- [x] 8.2 Update `store/aerospike.go` Aerospike implementation for the new interface: implement `GetOrInsertStatus` (check-and-insert with generation check), `UpdateStatus` (with `TransactionStatus` struct), `SetMinedByTxIDs` (batch update with block hash), binary BUMP storage, STUMP storage by `(blockHash, subtreeIndex)`, block tracking sets, submission storage set
- [x] 8.3 Remove old store methods that no longer match the interface (old `CreateTransaction`, `BatchCreateTransactions`, `UpdateState`, `UpdateStateWithReason`, `AttachProof`, `BatchAttachProofs`, old STUMP/BUMP methods)
- [x] 8.4 Write store interface tests with mock Aerospike or integration tests for key operations

## 9. Callback Handler Update

- [x] 9.1 Update `services/api_server/handlers.go` callback handler to process `CallbackMessage` (not `CallbackPayload`): parse `Type` field, add bearer token validation, implement `handleSeenOnNetwork` (resolve TxIDs, update status, update TxTracker), `handleStump` (extract level-0 hashes, filter tracked via TxTracker, update status, store STUMP by subtreeIndex, publish to Kafka), `handleBlockProcessed` (publish to Kafka)
- [x] 9.2 Update `services/api_server/handlers.go` tx submission handler to support `application/octet-stream`, `text/plain` (hex), and JSON content types; parse via go-sdk `NewTransactionFromBytes`/`ParseBeef`; compute TXID via `tx.TxID().String()`; return ARC error responses
- [x] 9.3 Add TxTracker as a dependency of the API server service (injected via constructor)
- [x] 9.4 Write handler tests: callback with each type (SEEN_ON_NETWORK, STUMP, BLOCK_PROCESSED), bearer token validation, tx submission with each content type, ARC error responses

## 10. BUMP Builder Service Update

- [x] 10.1 Update `services/bump_builder/builder.go` to use the new `bump` package: consume BLOCK_PROCESSED from Kafka, call `store.GetStumpsByBlockHash` (returns `[]*models.Stump` with subtreeIndex), call `bump.FetchBlockDataForBUMP` for subtree hashes + coinbase BUMP, call `bump.BuildCompoundBUMP` with TxTracker, store compound BUMP via `store.InsertBUMP` as binary, call `store.SetMinedByTxIDs` for tracked txids, call `store.DeleteStumpsByBlockHash`
- [x] 10.2 Update `services/bump_builder/stump_consumer.go` to store STUMPs using the new model (`models.Stump` with `SubtreeIndex` and binary `StumpData`) — parse `CallbackMessage` from Kafka, extract blockHash/subtreeIndex/stump bytes, call `store.InsertStump`
- [x] 10.3 Add TxTracker as a dependency of the bump builder service
- [x] 10.4 Write integration tests for the full BUMP construction pipeline with mock store and datahub

## 11. TX Validator Service Update

- [x] 11.1 Update `services/tx_validator/validator.go` to use the new `validator` package: parse transactions via go-sdk, compute TXID via `tx.TxID().String()`, validate via `validator.ValidateTransaction`, handle `GetOrInsertStatus` for duplicate detection
- [x] 11.2 Remove stub `computeTXID` and `validateTransaction` functions
- [x] 11.3 Wire validator with configurable policy and optional chain tracker

## 12. Propagation Service Update

- [x] 12.1 Update `services/propagation/propagator.go` to use the teranode client: submit raw bytes (not JSON) via `SubmitTransaction` to each endpoint concurrently, aggregate results using `statusPriority`, update transaction status based on best result
- [x] 12.2 Update merkle-service registration to use the merkle-service client's `Register` method posting to `/watch`
- [x] 12.3 Register with merkle-service BEFORE broadcasting (best-effort, non-blocking on failure)
- [x] 12.4 Write tests for propagation: concurrent broadcast, status priority aggregation, registration failure handling

## 13. Wire Dependencies in main.go

- [x] 13.1 Update `cmd/arcade/main.go` to create TxTracker, teranode client, merkle-service client, and validator; inject them into the appropriate services
- [x] 13.2 Update config to add teranode auth token, merkle-service auth token, callback URL, validator policy settings
- [x] 13.3 Verify all services start correctly with the new dependencies

## 14. End-to-End Tests

- [x] 14.1 Write BUMP construction end-to-end test: create STUMPs with known binary data, mock datahub returning subtree hashes + coinbase BUMP, verify compound BUMP contains correct merged paths, verify ExtractMinimalPath returns valid proofs
- [x] 14.2 Write callback handler end-to-end test: send STUMP callback, verify STUMP stored by subtreeIndex, send BLOCK_PROCESSED, verify Kafka message published
- [x] 14.3 Write transaction lifecycle test: submit tx → validate → propagate → register → SEEN_ON_NETWORK callback → STUMP callback → BLOCK_PROCESSED → BUMP built → MINED status
