# Arcade Architecture

## Overview

Arcade is a P2P-first Bitcoin transaction broadcast client for Teranode that provides an Arc-compatible HTTP API. It implements a pluggable architecture with support for multiple storage backends (SQLite, PostgreSQL) and event systems (in-memory, Redis).

## Transaction State Model

### Status States

Transactions progress through a defined set of states, managed as an append-only log.

### State Transitions and Triggers

```mermaid
stateDiagram-v2
    [*] --> RECEIVED
    RECEIVED --> SENT_TO_NETWORK
    SENT_TO_NETWORK --> ACCEPTED_BY_NETWORK
    SENT_TO_NETWORK --> REJECTED
    ACCEPTED_BY_NETWORK --> MINED
    ACCEPTED_BY_NETWORK --> SEEN_ON_NETWORK
    ACCEPTED_BY_NETWORK --> REJECTED
    ACCEPTED_BY_NETWORK --> DOUBLE_SPEND_ATTEMPTED
    SEEN_ON_NETWORK --> MINED
    SEEN_ON_NETWORK --> REJECTED
    SEEN_ON_NETWORK --> DOUBLE_SPEND_ATTEMPTED
    MINED --> [*]
    REJECTED --> [*]
    DOUBLE_SPEND_ATTEMPTED --> [*]
```

**Components and State Triggers:**

```mermaid
flowchart TD
    Client[HTTP Client]
    API[API Server]
    Teranode[Teranode]
    SubtreeGossip[P2P: subtree Topic]
    BlockGossip[P2P: block Topic]
    RejectedGossip[P2P: rejected-tx Topic]

    Client -->|POST /v1/tx| API
    API -->|Set Status| RECEIVED[RECEIVED]
    API -->|Submit TX| Teranode
    Teranode -->|Submitted| SENT[SENT_TO_NETWORK]
    Teranode -->|200/202 Response| ACCEPTED[ACCEPTED_BY_NETWORK]
    Teranode -->|Error Response| REJ1[REJECTED]

    SubtreeGossip -->|Gossip Message| SEEN[SEEN_ON_NETWORK]
    BlockGossip -->|Gossip Message| MINED[MINED]
    RejectedGossip -->|Rejection Reason| REJ2[REJECTED]
    RejectedGossip -->|Double Spend Reason| DS[DOUBLE_SPEND_ATTEMPTED]

    style RECEIVED stroke:#2196F3,stroke-width:3px
    style SENT stroke:#2196F3,stroke-width:3px
    style ACCEPTED stroke:#2196F3,stroke-width:3px
    style SEEN stroke:#2196F3,stroke-width:3px
    style MINED stroke:#4CAF50,stroke-width:3px
    style REJ1 stroke:#F44336,stroke-width:3px
    style REJ2 stroke:#F44336,stroke-width:3px
    style DS stroke:#FF9800,stroke-width:3px
```

### Status Definitions

| Status | Set By | Trigger |
|--------|--------|---------|
| `RECEIVED` | API Server | Transaction accepted via POST /v1/tx |
| `SENT_TO_NETWORK` | Teranode Client | Teranode returns 202 (queued) |
| `ACCEPTED_BY_NETWORK` | Teranode Client | Teranode returns 200 (accepted) |
| `SEEN_ON_NETWORK` | P2P Subscriber | Subtree gossip message received |
| `MINED` | P2P Subscriber | Block gossip message contains transaction |
| `MINED_IN_STALE_BLOCK` | P2P Subscriber | ChainTracks detects block reorganization |
| `REJECTED` | Teranode Client / P2P Subscriber | Teranode error or P2P rejected-tx message |
| `DOUBLE_SPEND_ATTEMPTED` | P2P Subscriber | P2P rejected-tx with double spend reason |

**Arc Statuses Not Implemented in Arcade:**

| Arc Status | Reason Not Implemented |
|------------|------------------------|
| `UNKNOWN` | Arcade uses explicit initial state (`RECEIVED`) |
| `QUEUED` | Arcade has no queue timeout - returns immediately |
| `STORED` | Not needed - storage is implicit |
| `ANNOUNCED_TO_NETWORK` | P2P network details not tracked at this granularity |
| `REQUESTED_BY_NETWORK` | P2P network details not tracked at this granularity |
| `SEEN_IN_ORPHAN_MEMPOOL` | Does not map to Arcade's architecture |

## System Communication Overview

```mermaid
graph TB
    Client[HTTP Client]
    UserService[User Service<br/>Callback Endpoint]
    API[Arcade API Server]
    Store[(Status Store<br/>Submission Store<br/>Network Store)]
    Events[Event Publisher<br/>memory/Redis]
    Teranode[Teranode Endpoints]
    P2P[P2P LibP2P Network]
    Webhook[Webhook Handler]

    Client -->|POST /v1/tx| API
    Client -->|GET /v1/tx/:txid| API
    Client <-->|SSE /v1/events/:callbackToken| Events

    API -->|Insert Status| Store
    API -->|Submit Transaction| Teranode
    API -->|Query Status| Store

    Teranode -.->|Response| API
    API -->|Publish StatusUpdate| Events

    P2P -->|block, subtree, rejected-tx| Store
    Store -->|Publish StatusUpdate| Events

    Events -->|Subscribe| Webhook

    Webhook -->|HTTP POST| UserService
    Webhook -->|Query Submissions| Store
```

### Communication Interfaces

#### 1. HTTP API (Arc-Compatible)

**Transaction Submission:**
- `POST /v1/tx` - Submit single transaction (raw hex)
- `POST /v1/txs` - Submit batch transactions (array of hex)

**Request Headers:**
- `X-CallbackUrl` - Webhook endpoint for status notifications
- `X-CallbackToken` - Token for webhook authentication and SSE filtering
- `X-FullStatusUpdates` - Boolean to receive all intermediate statuses
- `X-WaitFor` - Target status before HTTP response (not fully implemented)
- `X-MaxTimeout` - Maximum wait time (5-30 seconds)
- `X-SkipFeeValidation` / `X-SkipScriptValidation` - Validation overrides

**Status Query:**
- `GET /v1/tx/:txid` - Returns current status and timestamp

**Response Format:**
```json
{
  "txid": "abc123...",
  "txStatus": "DOUBLE_SPEND_ATTEMPTED",
  "timestamp": "2024-01-15T10:30:00Z",
  "competingTxs": ["def456..."]
}
```

#### 2. Server-Sent Events (SSE)

**Endpoint:** `GET /v1/events/:callbackToken`

**Features:**
- Callback token-based filtering (only events matching the callback token)
- Automatic catchup using `Last-Event-ID` header
- Event IDs are nanosecond timestamps for ordering
- Supports browser auto-reconnection

**Event Format:**
```
id: 1699632123456789000
event: status
data: {"txid":"abc...","status":"SEEN_ON_NETWORK","timestamp":"2024-01-15T10:30:00Z"}
```

#### 3. Webhook Notifications

**Delivery:**
- Asynchronous HTTP POST to callback URL
- Bearer token authentication using callback token
- Exponential backoff retry on failure
- Configurable max retries and expiration

**Request Format:**
```json
POST {callbackUrl}
Authorization: Bearer {callbackToken}

{
  "txid": "abc123...",
  "txStatus": "MINED",
  "timestamp": "2024-01-15T10:35:00Z"
}
```

**Delivery Tracking:**
- Prevents duplicate notifications for same status
- Tracks retry count and next retry time
- Respects `X-FullStatusUpdates` preference

#### 4. P2P LibP2P Gossip

**Subscribed Topics:**
- `{prefix}-block` - Block announcements with height and hash
- `{prefix}-subtree` - Validated unmined transactions
- `{prefix}-rejected-tx` - Transaction rejection notifications

**Message Processing:**
- Block messages contain transaction IDs, mark transactions as `MINED`
- Subtree messages contain transaction IDs, mark transactions as `SEEN_ON_NETWORK`
- Rejected-tx messages parse rejection reason and mark as `REJECTED` or `DOUBLE_SPEND_ATTEMPTED` based on the rejection type

**ChainTracks Integration:**
- Maintains blockchain state (current height, block hashes)
- Used to detect chain reorganizations for `MINED_IN_STALE_BLOCK` status

#### 5. Teranode HTTP Client

**Submission:**
- `POST {endpoint}/tx` with raw transaction bytes
- Fan-out to multiple Teranode endpoints concurrently
- 30-second timeout per request

**Response Handling:**
- `200` - Transaction accepted → `ACCEPTED_BY_NETWORK`
- `202` - Transaction queued → `SENT_TO_NETWORK`
- `4xx/5xx` - Error → `REJECTED` with error details

## Event System

### Architecture

The event system uses a publisher/subscriber pattern with pluggable backends:

**Interface:**
```go
type Publisher interface {
    Publish(ctx context.Context, update StatusUpdate) error
    Subscribe(ctx context.Context) (<-chan StatusUpdate, error)
    Close() error
}
```

**Event Type:**
```go
type StatusUpdate struct {
    TxID      string
    Status    Status
    Timestamp time.Time
}
```

### Event Flow

1. **Status Change** → `StatusStore.UpdateStatus()` called
2. **Publish** → `Publisher.Publish()` broadcasts event
3. **Fan-Out** → Multiple subscribers receive event:
   - Webhook Handler queries submissions and delivers notifications
   - SSE Handler filters by token and streams to connected clients
   - Future: WaitFor handler blocks HTTP responses until target status
   
### Backends

**In-Memory Publisher:**
- Go channels with configurable buffer size
- Fan-out to multiple goroutine subscribers
- Non-blocking publish (drops slow consumers)
- Single-node deployments only

**Redis Publisher:**
- Redis Pub/Sub on channel `arcade:status_updates`
- JSON-serialized events
- Supports multi-node distributed deployments
- Events survive service restarts if subscribers reconnect

## Storage Layer

### Store Interfaces

**StatusStore:**
- `InsertStatus()` - Initial submission (status: RECEIVED)
- `UpdateStatus()` - P2P or Teranode updates
- `GetStatus()` - Current status for transaction
- `GetStatusHistory()` - All status changes over time
- `GetStatusesSince()` - Catchup query for SSE

**SubmissionStore:**
- `InsertSubmission()` - Register client subscription
- `GetSubmissionsByTxID()` - Query for webhook delivery
- `GetSubmissionsByToken()` - Query for SSE filtering
- `UpdateDeliveryStatus()` - Track webhook delivery and retries

**NetworkStateStore:**
- `UpdateNetworkState()` - Current block height/hash from P2P
- `GetNetworkState()` - Query current blockchain state

### Implementations

**SQLite:**
- Single database file
- Append-only transaction status log
- Indexes: `txid`, `timestamp`, `callback_token`
- JSON storage for competing transaction arrays

**PostgreSQL:**
- Same schema as SQLite
- Better for multi-node deployments with Redis events
- Connection pooling for concurrent access

## Component Initialization

Application startup sequence ([cmd/arcade/main.go](cmd/arcade/main.go)):

1. Load configuration (YAML + environment variables)
2. Run database migrations
3. Create store instances (status, submission, network)
4. Initialize event publisher (memory or Redis)
5. Create Teranode client with endpoint list
6. Initialize transaction validator
7. Start P2P subscriber (gossip listener)
8. Start webhook handler (event subscriber)
9. Start HTTP API server
10. Wait for shutdown signal (graceful cleanup)

## Data Flow Examples

### Transaction Submission

```mermaid
sequenceDiagram
    participant Client
    participant API as API Server
    participant Validator
    participant StatusStore
    participant SubmissionStore
    participant Teranode as Teranode Client
    participant Events as Event Publisher
    participant Webhook as Webhook Handler
    participant SSE as SSE Handler

    Client->>API: POST /v1/tx (X-CallbackUrl, X-CallbackToken)
    API->>Validator: ValidateTransaction()
    Validator-->>API: Valid
    API->>StatusStore: InsertStatus() [RECEIVED]
    API->>SubmissionStore: InsertSubmission()
    API->>Teranode: SubmitTransaction() (async)
    API-->>Client: HTTP 200 (txid)
    Teranode->>StatusStore: UpdateStatus() [SENT_TO_NETWORK]
    Teranode->>StatusStore: UpdateStatus() [ACCEPTED_BY_NETWORK]
    StatusStore->>Events: Publish(StatusUpdate)
    Events->>Webhook: StatusUpdate
    Webhook->>Client: HTTP POST (callback URL)
    Events->>SSE: StatusUpdate
    SSE->>Client: Stream event
```

### P2P Status Update

```mermaid
sequenceDiagram
    participant P2P as P2P Network
    participant Sub as P2P Subscriber
    participant StatusStore
    participant Events as Event Publisher
    participant Webhook as Webhook Handler
    participant SubmissionStore
    participant SSE as SSE Handler
    participant Client as User Service

    P2P->>Sub: Gossip message (subtree/block/rejected-tx)
    Sub->>Sub: Parse message
    Sub->>StatusStore: UpdateStatus() [SEEN_ON_NETWORK/MINED/REJECTED]
    StatusStore->>Events: Publish(StatusUpdate)
    Events->>Webhook: StatusUpdate
    Webhook->>SubmissionStore: Query submissions by txid
    SubmissionStore-->>Webhook: Submissions
    Webhook->>Client: HTTP POST (callback URLs)
    Events->>SSE: StatusUpdate
    SSE->>Client: Stream event (filtered by token)
```

### SSE Streaming with Catchup

```mermaid
sequenceDiagram
    participant Client
    participant SSE as SSE Handler
    participant Events as Event Publisher
    participant StatusStore

    Client->>SSE: GET /v1/events/:callbackToken (Last-Event-ID)
    SSE->>Events: Subscribe()
    SSE->>StatusStore: GetStatusesSince(lastEventID)
    StatusStore-->>SSE: Missed events
    SSE->>Client: Stream missed events
    Events->>SSE: Real-time StatusUpdate
    SSE->>Client: Stream real-time event
    Note over Client,SSE: On disconnect...
    Client->>SSE: Reconnect with updated Last-Event-ID
```

## Configuration

Configuration is managed via Viper with YAML files and environment variable overrides (`ARCADE_*`).

**Key Sections:**
- `server` - HTTP address, timeouts, CORS
- `database` - Type (sqlite/postgres), connection string, migrations
- `events` - Type (memory/redis), Redis connection
- `teranode` - Endpoint list, timeout
- `p2p` - Peer list, topic prefix
- `validator` - Size limits, fee validation, script validation
- `webhook` - Retry policy, max retries, expiration
- `auth` - Token-based authentication (future)

## Architecture Patterns

**Pluggable Backends:**
Storage and event systems use interfaces to allow runtime configuration of backends without code changes.

**Event-Driven:**
All status changes flow through a central event system, enabling multiple notification channels (webhooks, SSE, future: long-polling).

**Async Processing:**
Transaction submission returns immediately. Teranode submission and webhook delivery happen asynchronously.

**Append-Only State:**
Status history is immutable. Current status is derived by querying the most recent entry.

**Token-Based Routing:**
Callback tokens isolate event streams for multi-tenant use cases. SSE and webhooks filter by token.

**Arc Compatibility:**
API endpoints, headers, and response formats match Arc specification for drop-in replacement scenarios.
