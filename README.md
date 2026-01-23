<div align="center">

# 🕹&nbsp;&nbsp;arcade

**A P2P-first Bitcoin transaction broadcast client for Teranode with Arc-compatible API**

<br/>

<a href="https://github.com/bsv-blockchain/arcade/releases"><img src="https://img.shields.io/github/release-pre/bsv-blockchain/arcade?include_prereleases&style=flat-square&logo=github&color=black" alt="Release"></a>
<a href="https://golang.org/"><img src="https://img.shields.io/github/go-mod/go-version/bsv-blockchain/arcade?style=flat-square&logo=go&color=00ADD8" alt="Go Version"></a>
<a href="LICENSE"><img src="https://img.shields.io/badge/license-OpenBSV-blue?style=flat-square&logo=springsecurity&logoColor=white" alt="License"></a>

<br/>

<table align="center" border="0">
  <tr>
    <td align="right">
       <code>CI / CD</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/bsv-blockchain/arcade/actions"><img src="https://img.shields.io/github/actions/workflow/status/bsv-blockchain/arcade/fortress.yml?branch=main&label=build&logo=github&style=flat-square" alt="Build"></a>
       <a href="https://github.com/bsv-blockchain/arcade/actions"><img src="https://img.shields.io/github/last-commit/bsv-blockchain/arcade?style=flat-square&logo=git&logoColor=white&label=last%20update" alt="Last Commit"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Quality</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://goreportcard.com/report/github.com/bsv-blockchain/arcade"><img src="https://goreportcard.com/badge/github.com/bsv-blockchain/arcade?style=flat-square&v=1" alt="Go Report"></a>
       <a href="https://codecov.io/gh/bsv-blockchain/arcade"><img src="https://codecov.io/gh/bsv-blockchain/arcade/branch/main/graph/badge.svg?style=flat-square" alt="Coverage"></a>
    </td>
  </tr>

  <tr>
    <td align="right">
       <code>Security</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://scorecard.dev/viewer/?uri=github.com/bsv-blockchain/arcade"><img src="https://api.scorecard.dev/projects/github.com/bsv-blockchain/arcade/badge?style=flat-square" alt="Scorecard"></a>
       <a href=".github/SECURITY.md"><img src="https://img.shields.io/badge/policy-active-success?style=flat-square&logo=security&logoColor=white" alt="Security"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Community</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/bsv-blockchain/arcade/graphs/contributors"><img src="https://img.shields.io/github/contributors/bsv-blockchain/arcade?style=flat-square&color=orange" alt="Contributors"></a>
       <a href="https://github.com/sponsors/bsv-blockchain"><img src="https://img.shields.io/badge/sponsor-BSV-181717.svg?logo=github&style=flat-square" alt="Sponsor"></a>
    </td>
  </tr>
</table>

</div>

<br/>
<br/>

<div align="center">

### <code>Project Navigation</code>

</div>

<table align="center">
  <tr>
    <td align="center" width="33%">
       📦&nbsp;<a href="#-installation"><code>Installation</code></a>
    </td>
    <td align="center" width="33%">
       🚀&nbsp;<a href="#-quick-start"><code>Quick&nbsp;Start</code></a>
    </td>
    <td align="center" width="33%">
       📚&nbsp;<a href="#-documentation"><code>Documentation</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🏗️&nbsp;<a href="#-architecture"><code>Architecture</code></a>
    </td>
    <td align="center">
       🧪&nbsp;<a href="#-examples--tests"><code>Examples&nbsp;&&nbsp;Tests</code></a>
    </td>
    <td align="center">
       ⚡&nbsp;<a href="#-benchmarks"><code>Benchmarks</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🛠️&nbsp;<a href="#-code-standards"><code>Code&nbsp;Standards</code></a>
    </td>
    <td align="center">
       🤖&nbsp;<a href="#-ai-usage--assistant-guidelines"><code>AI&nbsp;Usage</code></a>
    </td>
    <td align="center">
       📚&nbsp;<a href="#-resources"><code>Resources</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🤝&nbsp;<a href="#-contributing"><code>Contributing</code></a>
    </td>
    <td align="center">
       👥&nbsp;<a href="#-maintainers"><code>Maintainers</code></a>
    </td>
    <td align="center">
       📝&nbsp;<a href="#-license"><code>License</code></a>
    </td>
  </tr>
</table>
<br/>

## 📦 Installation

**arcade** requires a [supported release of Go](https://golang.org/doc/devel/release.html#policy).
```shell script
go get -u github.com/bsv-blockchain/arcade
```

<br/>

## 🚀 Quick Start

### Prerequisites

- **Go 1.25+** (see `go.mod` for exact version)
- **SQLite** (included with most systems)
- **Teranode broadcast URL** (e.g., `https://arc.taal.com`)

### Build from Source

```bash
git clone https://github.com/bsv-blockchain/arcade.git
cd arcade
go build -o arcade ./cmd/arcade
```

### Configuration

Create `config.yaml` with your Teranode broadcast URL:

```yaml
# Minimal working configuration
network: main
storage_path: ~/.arcade

server:
  address: ":3011"

teranode:
  broadcast_urls:           # REQUIRED - at least one URL needed
    - "https://arc.taal.com"
  timeout: 30s
```

See `config.example.yaml` for all available options.

### Run

```bash
arcade -config config.yaml
```

You should see output indicating the server is running on port 3011.

<br/>

## 📚 Documentation

### Overview

Arcade is a lightweight transaction broadcast service that:
- Listens to Bitcoin network events via libp2p gossip
- Provides Arc-compatible HTTP API for easy migration
- Supports pluggable storage (SQLite) and event backends (in-memory)
- Tracks transaction status through the complete lifecycle
- Delivers notifications via webhooks and Server-Sent Events (SSE)

**What is [Teranode](https://bsv-blockchain.github.io/teranode/topics/teranodeIntro/)?** Teranode is the next-generation BSV node implementation designed for enterprise-scale transaction processing. Arcade acts as a bridge between your application and the Teranode network, handling transaction submission and status tracking.

<br>

### Features

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
- **API Reference** – Dive into the godocs at [pkg.go.dev/github.com/bsv-blockchain/arcade](https://pkg.go.dev/github.com/bsv-blockchain/arcade)

<br>

### API Usage

#### Submit Transaction (`POST /tx`)

```bash
curl -X POST http://localhost:3011/tx \
  -H "Content-Type: text/plain" \
  -H "X-CallbackUrl: https://myapp.com/webhook" \
  --data "<hex-encoded-transaction>"
```

#### Get Transaction Status (`GET /tx/{txid}`)

```bash
curl http://localhost:3011/tx/{txid}
```

#### Health Check (`GET /health`)

```bash
curl http://localhost:3011/health
```

#### API Docs (`GET /docs`)

Interactive API documentation (Scalar UI) available at `http://localhost:3011/docs`

### Transaction Status Flow

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

### Differences from Arc

- **Simpler Deployment** - Single binary with SQLite, no PostgreSQL/NATS required
- **P2P-First** - Direct gossip listening instead of node callbacks
- **Teranode-Only** - Designed for Teranode, no legacy node support
- **Extensible** - All packages public for customization

<details>
<summary><strong>Configuration Reference</strong></summary>

#### Required Fields

| Field                     | Description                             | Default      |
|---------------------------|-----------------------------------------|--------------|
| `teranode.broadcast_urls` | Teranode broadcast service URLs (array) | **Required** |

#### Optional Fields

| Field                       | Description                                  | Default               |
|-----------------------------|----------------------------------------------|-----------------------|
| `mode`                      | Operating mode: `embedded`, `remote`         | `embedded`            |
| `url`                       | Arcade server URL (required for remote mode) | -                     |
| `network`                   | Bitcoin network: `main`, `test`, `stn`       | `main`                |
| `storage_path`              | Data directory for persistent files          | `~/.arcade`           |
| `log_level`                 | Log level: `debug`, `info`, `warn`, `error`  | `info`                |
| `server.address`            | HTTP server listen address                   | `:3011`               |
| `server.read_timeout`       | HTTP read timeout                            | `30s`                 |
| `server.write_timeout`      | HTTP write timeout                           | `30s`                 |
| `server.shutdown_timeout`   | Graceful shutdown timeout                    | `10s`                 |
| `database.type`             | Storage backend: `sqlite`                    | `sqlite`              |
| `database.sqlite_path`      | SQLite database file path                    | `~/.arcade/arcade.db` |
| `events.type`               | Event backend: `memory`                      | `memory`              |
| `events.buffer_size`        | Event channel buffer size                    | `1000`                |
| `teranode.datahub_urls`     | DataHub URLs for fetching block data (array) | -                     |
| `teranode.auth_token`       | Authentication token for Teranode API        | -                     |
| `teranode.timeout`          | HTTP request timeout                         | `30s`                 |
| `validator.max_tx_size`     | Maximum transaction size (bytes)             | `4294967296`          |
| `validator.max_script_size` | Maximum script size (bytes)                  | `500000`              |
| `validator.max_sig_ops`     | Maximum signature operations                 | `4294967295`          |
| `validator.min_fee_per_kb`  | Minimum fee per KB (satoshis)                | `100`                 |
| `webhook.max_retries`       | Max webhook retry attempts                   | `10`                  |
| `auth.enabled`              | Enable Bearer token authentication           | `false`               |
| `auth.token`                | Bearer token (required if auth enabled)      | -                     |

**Planned but not yet implemented:**
- `database.type: postgres` - PostgreSQL storage backend
- `events.type: redis` - Redis event backend for distributed deployments

</details>

<details>
<summary><strong>HTTP Headers</strong></summary>

Arcade supports Arc-compatible headers:

- `X-CallbackUrl` - Webhook URL for async status updates
- `X-CallbackToken` - Token for webhook auth and SSE stream filtering
- `X-FullStatusUpdates` - Include all intermediate statuses (default: final only)
- `X-SkipFeeValidation` - Skip fee validation
- `X-SkipScriptValidation` - Skip script validation

</details>

<details>
<summary><strong>Webhook Notifications</strong></summary>

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
- Automatic retries with linear backoff (1min, 2min, 3min, etc.)
- Configurable max retries via `webhook.max_retries`
- Delivery tracking

</details>

<details>
<summary><strong>SSE Streaming</strong></summary>

Arcade provides real-time transaction status updates via SSE, offering an alternative to webhook callbacks.

#### How It Works

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

#### Catchup on Reconnect

When the SSE connection drops, the browser's EventSource automatically reconnects and sends the `Last-Event-ID` header. Arcade replays all missed events since that timestamp.

**No client code needed** - this is handled automatically by the EventSource API.

#### Event ID Format

Event IDs are nanosecond timestamps (`time.UnixNano()`), ensuring:
- Chronological ordering
- Virtually collision-free IDs
- Easy catchup queries by time

#### Use Cases

- **Webhooks:** Server-to-server notifications with retry logic
- **SSE:** Real-time browser updates with automatic reconnection
- **Both:** Use the same `X-CallbackToken` for webhooks AND SSE streaming

#### Filtering

Each SSE connection only receives events for transactions submitted with the matching callback token. This allows:
- Multiple users/sessions with isolated event streams
- Scoped access without complex authentication
- Simple token-based routing

See [examples/sse_client.html](examples/sse_client.html) and [examples/sse_client.go](examples/sse_client.go) for complete examples.

</details>

<details>
<summary><strong>Remote Client</strong></summary>

For applications that need Arcade functionality without running a full server, use the REST client:

```go
import "github.com/bsv-blockchain/arcade/client"

c := client.New("http://arcade-server:3011")
defer c.Close() // Always close when done to clean up SSE connections

// Submit a single transaction
status, err := c.SubmitTransaction(ctx, rawTxBytes, nil)

// Submit multiple transactions
statuses, err := c.SubmitTransactions(ctx, [][]byte{rawTx1, rawTx2}, nil)

// Get transaction status
status, err := c.GetStatus(ctx, "txid...")

// Get policy configuration
policy, err := c.GetPolicy(ctx)

// Subscribe to status updates for a callback token
statusChan, err := c.Subscribe(ctx, "my-callback-token")
for status := range statusChan {
    fmt.Printf("Status: %s %s\n", status.TxID, status.Status)
}
```

</details>

<details>
<summary><strong>Extending Arcade</strong></summary>

All packages are public and designed for extension:

#### Custom Storage Backend

```go
import "github.com/bsv-blockchain/arcade/store"

type MyStore struct {}

func (s *MyStore) InsertStatus(ctx context.Context, status *models.TransactionStatus) error {
    // Your implementation
}

// Implement other store.StatusStore methods...
```

#### Custom Event Handler

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

</details>

<details>
<summary><strong>Troubleshooting</strong></summary>

**Server fails to start with "no teranodes configured"**
- Ensure `teranode.broadcast_urls` is set in your config file with at least one valid URL

**Transactions stuck in SENT_TO_NETWORK**
- Check your Teranode broadcast URL is reachable
- Verify network connectivity and firewall rules

**SQLite errors**
- Ensure `storage_path` directory exists and is writable
- Check disk space availability

</details>

<br/>

## 🏗️ Architecture

### Key Components

- **Arcade** (`arcade.go`) - Core P2P listener that subscribes to block, subtree, and rejected-tx gossip topics
- **Store** (`store/`) - SQLite storage for transaction statuses and webhook tracking
- **TxTracker** (`store/tracker.go`) - In-memory index of transactions being monitored
- **Event Publisher** (`events/`) - In-memory pub/sub for distributing status updates
- **Webhook Handler** (`handlers/webhook.go`) - Delivers status updates with retry logic
- **HTTP Routes** (`routes/fiber/`) - Arc-compatible REST API including SSE streaming

### Chain Tracking

Arcade uses [go-chaintracks](https://github.com/bsv-blockchain/go-chaintracks) for blockchain header tracking and merkle proof validation. Headers are loaded from embedded checkpoint files on startup and updated via P2P block announcements.

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

<br/>

## 🧪 Examples & Tests

All unit tests and examples run via [GitHub Actions](https://github.com/bsv-blockchain/arcade/actions) and use [Go version 1.25.x](https://go.dev/doc/go1.25). View the [configuration file](.github/workflows/fortress.yml).

Run all tests (fast):

```bash script
magex test
```

Run all tests with race detector (slower):
```bash script
magex test:race
```

See [examples/](examples) for SSE client examples and the [combined server example](examples/combined_server).

<br/>

## ⚡ Benchmarks

Run the Go benchmarks:

```bash script
magex bench
```

> **Note:** Comprehensive benchmarks for P2P operations (peer discovery, message throughput, connection establishment) are planned for future releases. The current focus is on correctness and stability of the networking implementation.

<br/>

## 🛠️ Code Standards
Read more about this Go project's [code standards](.github/CODE_STANDARDS.md).

<br/>

## 🤖 AI Usage & Assistant Guidelines
Read the [AI Usage & Assistant Guidelines](.github/tech-conventions/ai-compliance.md) for details on how AI is used in this project and how to interact with AI assistants.

<br/>

## 📚 Resources

- [Architecture Documentation](docs/ARCHITECTURE.md)
- [Teranode Documentation](https://docs.bsvblockchain.org/)
- [Arc API Reference](https://github.com/bitcoin-sv/arc)

<br/>

## 🤝 Contributing
View the [contributing guidelines](.github/CONTRIBUTING.md) and please follow the [code of conduct](.github/CODE_OF_CONDUCT.md).

### How can I help?
All kinds of contributions are welcome :raised_hands:!
The most basic way to show your support is to star :star2: the project, or to raise issues :speech_balloon:.

[![Stars](https://img.shields.io/github/stars/bsv-blockchain/arcade?label=Please%20like%20us&style=social&v=1)](https://github.com/bsv-blockchain/arcade/stargazers)

<br/>

## 👥 Maintainers
| [<img src="https://github.com/icellan.png" height="50" alt="Siggi" />](https://github.com/icellan) | [<img src="https://github.com/galt-tr.png" height="50" alt="Galt" />](https://github.com/galt-tr) | [<img src="https://github.com/mrz1836.png" height="50" alt="MrZ" />](https://github.com/mrz1836) |
|:--------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------:|
|                                [Siggi](https://github.com/icellan)                                 |                                [Dylan](https://github.com/galt-tr)                                |                                [MrZ](https://github.com/mrz1836)                                 |

<br/>

## 📝 License

[![License](https://img.shields.io/badge/license-OpenBSV-blue?style=flat&logo=springsecurity&logoColor=white)](LICENSE)
