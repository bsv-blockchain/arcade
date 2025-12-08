# Arcade

A P2P-first Bitcoin transaction broadcast client for Teranode with Arc-compatible API.

## Overview

Arcade is a lightweight transaction broadcast service that:
- Listens to Bitcoin network events via libp2p gossip
- Provides Arc-compatible HTTP API for easy migration
- Supports pluggable storage (SQLite) and event backends (in-memory)
- Tracks transaction status through the complete lifecycle
- Delivers notifications via webhooks and Server-Sent Events (SSE)

## Features

- **Arc-Compatible API** - Drop-in replacement for Arc clients
- **P2P Network Listening** - Direct gossip subscription for real-time updates
- **Flexible Storage** - SQLite for simple deployments, PostgreSQL for distributed setups
- **Event Streaming** - In-memory or Redis pub/sub for distributed architectures
- **Webhook Delivery** - Async notifications with retry logic
- **SSE Streaming** - Real-time status updates with automatic catchup on reconnect
- **Transaction Validation** - Local validation before network submission
- **Status Tracking** - Complete audit trail of transaction lifecycle
- **Extensible** - All packages public, easy to customize

## Quick Start

### Installation

```bash
go install github.com/bsv-blockchain/arcade/cmd/arcade@latest
```

### Configuration

Create `config.yaml`:

```yaml
server:
  address: ":8080"

database:
  type: "sqlite"
  sqlite_path: "./arcade.db"

events:
  type: "memory"
  buffer_size: 1000

teranode:
  base_urls:
    - "http://teranode1.example.com:8080"
    - "http://teranode2.example.com:8080"
  timeout: "30s"

p2p:
  process_name: "arcade-node-1"
  private_key: ""  # Generated on first run if empty
  bootstrap_peers:
    - "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
  topic_prefix: "mainnet"
```

### Run

```bash
arcade -config config.yaml
```

## API Usage

### Submit Transaction

```bash
curl -X POST http://localhost:8080/v1/tx \
  -H "Content-Type: text/plain" \
  -H "X-WaitFor: SEEN_ON_NETWORK" \
  -H "X-MaxTimeout: 10" \
  -H "X-CallbackUrl: https://myapp.com/webhook" \
  --data "<hex-encoded-transaction>"
```

### Get Transaction Status

```bash
curl http://localhost:8080/v1/tx/{txid}
```

### Get Policy

```bash
curl http://localhost:8080/v1/policy
```

## Transaction Status Flow

```
Client → Arcade
   ↓
QUEUED/STORED (validated locally)
   ↓
SENT_TO_NETWORK (submitted to Teranode via HTTP)
   ↓
SEEN_ON_NETWORK (heard in subtree gossip - validated, in mempool)
   ↓
MINED (heard in block gossip - included in block)
```

Or rejected:
```
REJECTED (from rejected-tx gossip)
DOUBLE_SPEND_ATTEMPTED (from rejected-tx gossip with specific reason)
```

## Architecture

See [DESIGN.md](DESIGN.md) for detailed architecture documentation.

### Key Components

- **HTTP API Server** - Arc-compatible REST endpoints
- **P2P Subscriber** - LibP2P gossip listener (block, subtree, rejected-tx topics)
- **Teranode Client** - HTTP client for transaction submission with fan-out
- **Status Store** - Append-only transaction status log
- **Submission Store** - Client subscription tracking
- **Event System** - Global status update channel with multiple handlers
- **Webhook Handler** - Async notification delivery with retry logic
- **SSE Handler** - Real-time status streaming with catchup support
- **WaitFor Handler** - Synchronous status waiting for HTTP requests

## Development

### Prerequisites

- Go 1.21+
- SQLite or PostgreSQL (optional)
- Redis (optional)

### Build

```bash
go build -o arcade ./cmd/arcade
```

### Run Tests

```bash
go test ./...
```

## Configuration Options

| Section | Field | Description | Default |
|---------|-------|-------------|---------|
| `server.address` | string | HTTP server listen address | `:8080` |
| `database.type` | string | Database type: `sqlite` or `postgres` | `sqlite` |
| `database.sqlite_path` | string | SQLite database file path | `./arcade.db` |
| `database.postgres_conn_str` | string | PostgreSQL connection string | - |
| `events.type` | string | Event backend: `memory` or `redis` | `memory` |
| `events.buffer_size` | int | Event channel buffer size | `1000` |
| `events.redis_url` | string | Redis connection URL | - |
| `teranode.base_urls` | []string | Teranode propagation service URLs (fan-out) | `[]` |
| `teranode.timeout` | duration | HTTP request timeout | `30s` |
| `p2p.process_name` | string | P2P node identifier | - |
| `p2p.private_key` | string | P2P private key (hex) for consistent peer ID | - |
| `p2p.bootstrap_peers` | []string | Initial P2P peers (multiaddrs) | `[]` |
| `p2p.topic_prefix` | string | Gossip topic prefix: `mainnet`, `testnet` | `mainnet` |
| `validator.max_tx_size` | uint64 | Maximum transaction size (bytes) | `10000000` |
| `validator.enable_fee_check` | bool | Enable fee validation | `true` |
| `validator.enable_script_exec` | bool | Enable script execution validation | `false` |
| `callbacks.max_retries` | int | Max webhook retry attempts | `10` |
| `callbacks.max_age_hours` | int | Hours to keep retrying webhooks | `168` |

## HTTP Headers

Arcade supports Arc-compatible headers:

- `X-CallbackUrl` - Webhook URL for async updates
- `X-CallbackToken` - Token for webhook auth and SSE stream filtering
- `X-FullStatusUpdates` - Include all intermediate statuses (default: final only)
- `X-WaitFor` - Status to wait for before returning
- `X-MaxTimeout` - Wait timeout in seconds (max 30)
- `X-SkipFeeValidation` - Skip fee validation
- `X-SkipScriptValidation` - Skip script execution

## Webhook Notifications

When you provide `X-CallbackUrl`, Arcade will POST status updates:

```json
{
  "timestamp": "2024-03-26T16:02:29.655390092Z",
  "txid": "...",
  "txStatus": "SEEN_ON_NETWORK",
  "blockHash": "...",
  "blockHeight": 123456,
  "merklePath": "..."
}
```

**Features:**
- Automatic retries with exponential backoff
- Configurable expiration and max retries
- Delivery tracking

## Server-Sent Events (SSE) Streaming

Arcade provides real-time transaction status updates via SSE, offering an alternative to webhook callbacks.

### How It Works

1. **Submit Transaction with Callback Token:**
   ```bash
   curl -X POST http://localhost:8080/v1/tx \
     -H "Content-Type: application/json" \
     -H "X-CallbackToken: my-token-123" \
     -d '{"rawTx": "01000000..."}'
   ```

2. **Connect SSE Client:**
   ```javascript
   const eventSource = new EventSource('http://localhost:8080/v1/events/my-token-123');

   eventSource.addEventListener('status', (e) => {
     const update = JSON.parse(e.data);
     console.log(`${update.txid}: ${update.status}`);
   });
   ```

3. **Receive Real-Time Updates:**
   ```
   id: 1699632123456789000
   event: status
   data: {"txid":"abc123...","status":"SENT_TO_NETWORK","timestamp":"2024-11-10T12:00:00Z"}
   ```

### Catchup on Reconnect

When the SSE connection drops, the browser's EventSource automatically reconnects and sends the `Last-Event-ID` header. Arcade replays all missed events since that timestamp.

**No client code needed** - this is handled automatically by the EventSource API.

### Event ID Format

Event IDs are nanosecond timestamps (`time.UnixNano()`), ensuring:
- Chronological ordering
- Virtually collision-free IDs
- Easy catchup queries by time

### Use Cases

- **Webhooks:** Server-to-server notifications with retry logic
- **SSE:** Real-time browser updates with automatic reconnection
- **Both:** Use the same `X-CallbackToken` for webhooks AND SSE streaming

### Filtering

Each SSE connection only receives events for transactions submitted with the matching callback token. This allows:
- Multiple users/sessions with isolated event streams
- Scoped access without complex authentication
- Simple token-based routing

See [examples/sse_client.html](examples/sse_client.html) and [examples/sse_client.go](examples/sse_client.go) for complete examples.

## Extending Arcade

All packages are public and designed for extension:

### Custom Storage Backend

```go
import "github.com/bsv-blockchain/arcade/store"

type MyStore struct {}

func (s *MyStore) InsertStatus(ctx context.Context, status *models.TransactionStatus) error {
    // Your implementation
}

// Implement other store.StatusStore methods...
```

### Custom Event Handler

```go
import "github.com/bsv-blockchain/arcade/events"

type MyHandler struct {
    publisher events.Publisher
}

func (h *MyHandler) Start(ctx context.Context) error {
    ch, _ := h.publisher.Subscribe(ctx)
    for update := range ch {
        // Handle status update
    }
}
```

## Differences from Arc

- **Simpler Deployment** - Single binary, no PostgreSQL/NATS required by default
- **P2P-First** - Direct gossip listening instead of node callbacks
- **Pluggable Backends** - Swap components without code changes
- **Teranode-Only** - No legacy node support
- **Open & Extensible** - All packages public

## License

Open BSV License

## Resources

- [Design Documentation](DESIGN.md)
- [Teranode Documentation](https://docs.bitcoinsv.io/)
- [Arc API Reference](https://github.com/bsv-blockchain/arc)
