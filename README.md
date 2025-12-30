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
- **Chain Tracking** - Blockchain header management with merkle proof validation
- **Flexible Storage** - SQLite for persistent storage
- **Event Streaming** - In-memory pub/sub for event distribution
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

Create `config.yaml` (see `config.example.yaml` for a complete example):

```yaml
network: main
storage_path: ~/.arcade

server:
  address: ":3011"

database:
  type: sqlite
  sqlite_path: ~/.arcade/arcade.db

events:
  type: memory
  buffer_size: 1000

teranode:
  # Set via ARCADE_TERANODE_BASE_URL environment variable
  timeout: 30s

validator:
  max_tx_size: 4294967296
  min_fee_per_kb: 100
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

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture documentation.

### Key Components

- **HTTP API Server** - Arc-compatible REST endpoints
- **P2P Subscriber** - LibP2P gossip listener (block, subtree, rejected-tx topics)
- **Teranode Client** - HTTP client for transaction submission with fan-out
- **Chain Tracker** - Blockchain header management with local persistence
- **Status Store** - Append-only transaction status log
- **Submission Store** - Client subscription tracking
- **Event System** - Global status update channel with multiple handlers
- **Webhook Handler** - Async notification delivery with retry logic
- **SSE Handler** - Real-time status streaming with catchup support
- **WaitFor Handler** - Synchronous status waiting for HTTP requests

## Chain Tracking

Arcade includes blockchain header tracking for merkle proof validation. Headers are loaded from embedded checkpoint files on startup and updated via P2P block announcements.

### Header API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /network` | Returns the network name (mainnet, testnet, etc.) |
| `GET /height` | Returns current blockchain height |
| `GET /tip` | Returns current chain tip header |
| `GET /tip/stream` | SSE stream of chain tip updates |
| `GET /header/height/:height` | Get header by block height |
| `GET /header/hash/:hash` | Get header by block hash |
| `GET /headers?height=N&count=M` | Get M headers starting at height N |

### Remote Client

For applications that need Arcade functionality without running a full server, use the REST client:

```go
import "github.com/bsv-blockchain/arcade/client"

c := client.New("http://arcade-server:8080")

// Submit a transaction
status, err := c.SubmitTransaction(ctx, rawTxBytes, nil)

// Get transaction status
status, err := c.GetStatus(ctx, "txid...")

// Subscribe to transaction status updates
statusChan, err := c.Subscribe(ctx, "my-callback-token")
for status := range statusChan {
    fmt.Printf("Status: %s %s\n", status.TxID, status.Status)
}
```

## Development

### Prerequisites

- Go 1.22+
- SQLite

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
| `network` | string | Bitcoin network: `main`, `test`, `stn` | `main` |
| `storage_path` | string | Data directory for persistent files | `~/.arcade` |
| `server.address` | string | HTTP server listen address | `:3011` |
| `server.read_timeout` | duration | HTTP read timeout | `30s` |
| `server.write_timeout` | duration | HTTP write timeout | `30s` |
| `database.type` | string | Database type: `sqlite` | `sqlite` |
| `database.sqlite_path` | string | SQLite database file path | `~/.arcade/arcade.db` |
| `events.type` | string | Event backend: `memory` | `memory` |
| `events.buffer_size` | int | Event channel buffer size | `1000` |
| `teranode.base_url` | string | Teranode propagation service URL | - |
| `teranode.timeout` | duration | HTTP request timeout | `30s` |
| `validator.max_tx_size` | int | Maximum transaction size (bytes) | `4294967296` |
| `validator.max_script_size` | int | Maximum script size (bytes) | `500000` |
| `validator.max_sig_ops` | int64 | Maximum signature operations | `4294967295` |
| `validator.min_fee_per_kb` | uint64 | Minimum fee per KB (satoshis) | `100` |
| `webhook.max_retries` | int | Max webhook retry attempts | `10` |
| `webhook.max_age` | duration | Max age to keep retrying webhooks | `24h` |
| `auth.enabled` | bool | Enable authentication | `false` |

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

- [Architecture Documentation](ARCHITECTURE.md)
- [Teranode Documentation](https://docs.bitcoinsv.io/)
- [Arc API Reference](https://github.com/bsv-blockchain/arc)
