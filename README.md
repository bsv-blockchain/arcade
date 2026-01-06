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

teranode:
  broadcast_urls:
    - "https://arc.taal.com"
  timeout: 30s
```

### Run

```bash
arcade -config config.yaml
```

## API Usage

### Submit Transaction

Arcade accepts transactions as hex-encoded strings, raw bytes, or BEEF format:

```bash
curl -X POST http://localhost:3011/tx \
  -H "Content-Type: text/plain" \
  -H "X-CallbackUrl: https://myapp.com/webhook" \
  --data "<hex-encoded-transaction>"
```

### Get Transaction Status

```bash
curl http://localhost:3011/tx/{txid}
```

### Get Policy

```bash
curl http://localhost:3011/policy
```

### Submit Multiple Transactions

```bash
curl -X POST http://localhost:3011/txs \
  -H "Content-Type: application/json" \
  --data '[{"rawTx": "0100000001..."}, {"rawTx": "0100000001..."}]'
```

### Health Check

```bash
curl http://localhost:3011/health
```

## Transaction Status Flow

```
Client → Arcade
   ↓
RECEIVED (validated locally)
   ↓
SENT_TO_NETWORK (submitted to Teranode via HTTP)
   ↓
ACCEPTED_BY_NETWORK (acknowledged by Teranode)
   ↓
SEEN_ON_NETWORK (heard in subtree gossip - in mempool)
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

- **Arcade** (`arcade.go`) - Core P2P listener that subscribes to block, subtree, and rejected-tx gossip topics, processes messages, and updates transaction statuses
- **Store** (`store/`) - SQLite storage for transaction statuses, webhook submissions, and delivery tracking
- **TxTracker** (`store/tracker.go`) - In-memory index of transactions being monitored, with persistence
- **Event Publisher** (`events/`) - In-memory pub/sub for distributing status updates to handlers
- **Webhook Handler** (`handlers/webhook.go`) - Delivers status updates to client callback URLs with retry logic
- **HTTP Routes** (`routes/fiber/`) - Arc-compatible REST API including SSE streaming

## Chain Tracking

Arcade uses [go-chaintracks](https://github.com/bsv-blockchain/go-chaintracks) for blockchain header tracking and merkle proof validation. Headers are loaded from embedded checkpoint files on startup and updated via P2P block announcements.

For applications that need both transaction broadcast and header endpoints, see the [combined server example](examples/combined_server/).

### Remote Client

For applications that need Arcade functionality without running a full server, use the REST client:

```go
import "github.com/bsv-blockchain/arcade/client"

c := client.New("http://arcade-server:3011")
defer c.Close()

// Submit a transaction
status, err := c.SubmitTransaction(ctx, rawTxBytes, nil)

// Get transaction status
status, err := c.GetStatus(ctx, "txid...")

// Subscribe to status updates for a callback token
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

| Field | Description | Default |
|-------|-------------|---------|
| `network` | Bitcoin network: `main`, `test`, `stn` | `main` |
| `storage_path` | Data directory for persistent files | `~/.arcade` |
| `log_level` | Log level: `debug`, `info`, `warn`, `error` | `info` |
| `server.address` | HTTP server listen address | `:3011` |
| `server.shutdown_timeout` | Graceful shutdown timeout | `30s` |
| `teranode.broadcast_urls` | Teranode broadcast service URLs (array) | - |
| `teranode.datahub_urls` | DataHub URLs for fetching block data (array) | - |
| `teranode.timeout` | HTTP request timeout | `30s` |
| `validator.max_tx_size` | Maximum transaction size (bytes) | `4294967296` |
| `validator.max_script_size` | Maximum script size (bytes) | `500000` |
| `validator.max_sig_ops` | Maximum signature operations | `4294967295` |
| `validator.min_fee_per_kb` | Minimum fee per KB (satoshis) | `100` |
| `webhook.max_retries` | Max webhook retry attempts | `10` |
| `webhook.max_age` | Max age to keep retrying webhooks | `24h` |
| `webhook.prune_interval` | How often to prune expired webhooks | `1h` |
| `auth.enabled` | Enable Bearer token authentication | `false` |
| `auth.token` | Bearer token (required if auth enabled) | - |

## HTTP Headers

Arcade supports Arc-compatible headers:

- `X-CallbackUrl` - Webhook URL for async status updates
- `X-CallbackToken` - Token for webhook auth and SSE stream filtering
- `X-FullStatusUpdates` - Include all intermediate statuses (default: final only)
- `X-SkipFeeValidation` - Skip fee validation
- `X-SkipScriptValidation` - Skip script validation

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
   curl -X POST http://localhost:3011/tx \
     -H "Content-Type: text/plain" \
     -H "X-CallbackToken: my-token-123" \
     --data "<hex-encoded-transaction>"
   ```

2. **Connect SSE Client:**
   ```javascript
   const eventSource = new EventSource('http://localhost:3011/events?callbackToken=my-token-123');

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

- **Simpler Deployment** - Single binary with SQLite, no PostgreSQL/NATS required
- **P2P-First** - Direct gossip listening instead of node callbacks
- **Teranode-Only** - Designed for Teranode, no legacy node support
- **Extensible** - All packages public for customization

## License

Open BSV License

## Resources

- [Architecture Documentation](ARCHITECTURE.md)
- [Teranode Documentation](https://docs.bitcoinsv.io/)
- [Arc API Reference](https://github.com/bsv-blockchain/arc)
