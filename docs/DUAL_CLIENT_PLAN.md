# Arcade Dual-Implementation Client Plan

This document tracks the implementation of a dual-client pattern for Arcade, similar to go-chaintracks, where consumers can use either an embedded implementation or a REST client with the same interface.

## Overview

**Goal:** Create an `ArcadeService` interface that can be satisfied by:
1. **Embedded implementation** - Direct access to Arcade internals (for in-process use)
2. **REST client** - HTTP client for remote Arcade servers
3. **TypeScript client** - NPM package for browser/Node.js consumers

## Status Legend
- [ ] Not started
- [x] Complete
- [~] In progress

---

## Phase 1: Go Interface & Types

### 1.1 Add Types to Models Package
**File:** `models/transaction.go` (extend existing)

- [x] `SubmitOptions` struct
  ```go
  type SubmitOptions struct {
      CallbackURL          string // X-CallbackUrl - webhook for status updates
      CallbackToken        string // X-CallbackToken - token for SSE filtering
      FullStatusUpdates    bool   // X-FullStatusUpdates - send all status updates
      SkipFeeValidation    bool   // X-SkipFeeValidation
      SkipScriptValidation bool   // X-SkipScriptValidation
  }
  ```

- [x] `Policy` struct (move from routes.PolicyResponse)
  ```go
  type Policy struct {
      MaxScriptSizePolicy     uint64  `json:"maxscriptsizepolicy"`
      MaxTxSigOpsCountsPolicy uint64  `json:"maxtxsigopscountspolicy"`
      MaxTxSizePolicy         uint64  `json:"maxtxsizepolicy"`
      MiningFeeBytes          uint64  `json:"miningFeeBytes"`
      MiningFeeSatoshis       uint64  `json:"miningFeeSatoshis"`
  }
  ```

### 1.2 Define Service Interface
**File:** `service/interface.go`

- [x] `ArcadeService` interface
  ```go
  type ArcadeService interface {
      // SubmitTransaction submits a single transaction for broadcast.
      // rawTx can be raw bytes or BEEF format.
      SubmitTransaction(ctx context.Context, rawTx []byte, opts *models.SubmitOptions) (*models.TransactionStatus, error)

      // SubmitTransactions submits multiple transactions for broadcast.
      SubmitTransactions(ctx context.Context, rawTxs [][]byte, opts *models.SubmitOptions) ([]*models.TransactionStatus, error)

      // GetStatus retrieves the current status of a transaction.
      GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)

      // Subscribe returns a channel for transaction status updates.
      // If callbackToken is empty, all status updates are returned.
      // If callbackToken is provided, only updates for that token are returned.
      Subscribe(ctx context.Context, callbackToken string) (<-chan *models.TransactionStatus, error)

      // Unsubscribe removes a subscription channel.
      Unsubscribe(ch <-chan *models.TransactionStatus)

      // GetPolicy returns the transaction policy configuration.
      GetPolicy(ctx context.Context) (*models.Policy, error)
  }
  ```

### 1.3 Update Routes
**File:** `routes/fiber/routes.go`

- [x] Remove `PolicyResponse`, `FeeAmount`, `HealthResponse` structs
- [x] Import `models.Policy` instead
- [x] Simplify health endpoint to return 200 OK or 503 with plain text reason

---

## Phase 2: Embedded Implementation

### 2.1 Create Embedded Wrapper
**File:** `service/embedded/embedded.go`

- [x] `Embedded` struct wrapping existing `*arcade.Arcade` and dependencies
- [x] Constructor `New(cfg Config) (*Embedded, error)`
- [x] Implement `SubmitTransaction` - reuse logic from routes.handlePostTx
- [x] Implement `SubmitTransactions` - reuse logic from routes.handlePostTxs
- [x] Implement `GetStatus` - delegate to StatusStore
- [x] Implement `Subscribe` - delegate to Arcade.SubscribeStatus
- [x] Implement `Unsubscribe` - manage subscription lifecycle
- [x] Implement `GetPolicy` - return configured policy

### 2.2 Refactor Routes to Use Interface
**File:** `routes/fiber/routes.go`

- [x] Update Routes struct to accept `service.ArcadeService` instead of individual components
- [x] Refactor handlers to delegate to interface methods
- [x] Remove duplicated business logic (move to embedded implementation)
- [x] Keep health endpoint in routes only (not part of interface)

---

## Phase 3: REST Client Implementation

### 3.1 Add All-Events SSE Endpoint
**File:** `routes/fiber/routes.go`

- [x] Add `GET /events` route for unfiltered SSE stream
- [x] Refactor SSE handler to support optional token filtering
  ```go
  router.Get("/events", r.handleEventsSSE)           // all events
  router.Get("/events/:callbackToken", r.handleEventsSSE) // filtered
  ```

### 3.2 Create REST Client
**File:** `client/client.go`

- [x] `Client` struct with baseURL, httpClient, SSE state
- [x] Constructor `New(baseURL string, opts ...Option) *Client`
- [x] Implement `SubmitTransaction`
  - POST /tx with Content-Type: application/octet-stream
  - Map SubmitOptions to X-* headers
- [x] Implement `SubmitTransactions`
  - POST /txs with JSON body
  - Map SubmitOptions to X-* headers
- [x] Implement `GetStatus`
  - GET /tx/:txid
- [x] Implement `GetPolicy`
  - GET /policy

### 3.3 SSE Subscription
**File:** `client/sse.go`

- [x] SSE connection management (lazy connect on first subscriber)
- [x] `Subscribe(ctx context.Context, callbackToken string)` implementation
  - GET /events or GET /events/:callbackToken
  - Parse SSE events into TransactionStatus
- [x] `Unsubscribe` implementation
- [x] Fan-out to multiple subscribers
- [x] Reconnection logic with Last-Event-ID
- [x] Graceful shutdown on last unsubscribe

### 3.4 Client Options
**File:** `client/options.go`

- [x] `WithHTTPClient(client *http.Client)` option
- [x] `WithTimeout(timeout time.Duration)` option

---

## Phase 4: Factory/Config Pattern

### 4.1 Configuration Types
**File:** `factory/factory.go`

- [x] `Mode` type (embedded, remote)
- [x] `Config` struct
  ```go
  type Mode string
  const (
      ModeEmbedded Mode = "embedded"
      ModeRemote   Mode = "remote"
  )

  type Config struct {
      Mode Mode   `mapstructure:"mode"`
      URL  string `mapstructure:"url"` // Required for remote mode

      // Embedded mode config (existing arcade.Config fields)
      // ...
  }
  ```

- [x] `New(cfg Config) (ArcadeService, error)` factory function

---

## Phase 5: TypeScript Client

**Status:** Moved to external library. See separate TypeScript SDK repository.

---

## API Reference

### HTTP Endpoints

| Method | Path           | Description                    | In Interface    |
|--------|----------------|--------------------------------|-----------------|
| POST   | /tx            | Submit single transaction      | ✓               |
| POST   | /txs           | Submit multiple transactions   | ✓               |
| GET    | /tx/:txid      | Get transaction status         | ✓               |
| GET    | /events        | SSE stream (all events)        | ✓               |
| GET    | /events/:token | SSE stream (filtered by token) | ✓               |
| GET    | /policy        | Get policy configuration       | ✓               |
| GET    | /health        | Health check (200 OK / 503)    | ✗ (server only) |

### Request Headers (POST /tx, /txs)

| Header                 | Description                          |
|------------------------|--------------------------------------|
| X-CallbackUrl          | Webhook URL for status callbacks     |
| X-CallbackToken        | Token for SSE event filtering        |
| X-FullStatusUpdates    | Send all status updates (true/false) |
| X-SkipFeeValidation    | Skip fee validation (true/false)     |
| X-SkipScriptValidation | Skip script validation (true/false)  |

### SSE Event Format

```
id: {timestamp_nanoseconds}
event: status
data: {"txid":"...","txStatus":"...","timestamp":"..."}
```

### Transaction Status Values

| Status                 | Description                    |
|------------------------|--------------------------------|
| UNKNOWN                | Initial/unknown state          |
| RECEIVED               | Transaction received by Arcade |
| SENT_TO_NETWORK        | Submitted to Teranode          |
| ACCEPTED_BY_NETWORK    | Accepted by Teranode           |
| SEEN_ON_NETWORK        | Seen in P2P subtree message    |
| DOUBLE_SPEND_ATTEMPTED | Double spend detected          |
| REJECTED               | Transaction rejected           |
| MINED                  | Included in a block            |
| IMMUTABLE              | Deeply confirmed               |

---

## Testing Plan

- [ ] Unit tests for Go interface implementations
- [ ] Integration tests for REST client against running server
- [ ] E2E tests for SSE subscription lifecycle

---

## Notes

- Pattern based on go-chaintracks implementation
- Both implementations must behave identically for consumers
- SSE reconnection should use Last-Event-ID for catchup

## Implementation Summary

### Files Created/Modified

**Go:**
- `models/transaction.go` - Added `SubmitOptions` and `Policy` types
- `service/interface.go` - Created `ArcadeService` interface
- `service/embedded/embedded.go` - Embedded implementation
- `client/client.go` - REST client implementation
- `client/sse.go` - SSE subscription handling
- `client/options.go` - Client options
- `routes/fiber/routes.go` - Refactored to use service interface

**TypeScript:** Moved to external library.
