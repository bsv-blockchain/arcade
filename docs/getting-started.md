# Getting Started — Standalone Server

A quick path from `git clone` to a running arcade process on the network of your
choice. "Standalone" means a single binary with no external infrastructure: an
in-memory Kafka replacement, an embedded Pebble key/value store, and optional
libp2p peer discovery using built-in bootstrap peers.

At the end you'll have:

- One `arcade` process serving the Arc-compatible HTTP API on `:8080`
- A `/health` endpoint on `:8081`
- All state under `~/.arcade/` (Pebble DB, chaintracks headers, libp2p key)
- No external services running

For full configuration, see [`config.example.standalone.yaml`](../config.example.standalone.yaml).
For production deployments (Aerospike + external Kafka), see [`config.example.yaml`](../config.example.yaml).

## Prerequisites

- Go 1.26 or newer (see [`go.mod`](../go.mod))
- Git
- ~1 GB free disk for Pebble data and chaintracks headers

No SQLite, PostgreSQL, Kafka, or Aerospike are needed for the standalone
profile.

## Build

```bash
git clone https://github.com/bsv-blockchain/arcade.git
cd arcade
go build -o arcade ./cmd/arcade
```

## Base config

Save the following as `config.yaml`. This is the lowest-friction starting
point — it targets `teratestnet`, which has built-in bootstrap peers and no
production-transaction risk.

```yaml
mode: all
log_level: info
network: teratestnet
storage_path: ~/.arcade

api:
  host: 0.0.0.0
  port: 8080

health:
  port: 8081

kafka:
  backend: memory
  consumer_group: arcade

store:
  backend: pebble
  pebble:
    path: ~/.arcade/pebble
    memtable_size_mb: 64
    l0_compaction_threshold: 4
    sync_writes: false

p2p:
  datahub_discovery: true

chaintracks_server:
  enabled: true
```

## Run

```bash
./arcade --config config.yaml
```

`mode: all` runs every service (api-server, bump-builder, propagation,
p2p-client, sse, webhook, chaintracks_server, watchdog) in one process.

## Per-network deltas

The base config above runs against `teratestnet`. To target another network,
change only the lines shown below.

### mainnet — production Bitcoin SV

```yaml
network: mainnet
```

Caution: this is the live network. Bind `api.host` to a private interface (or
front it with auth) if the host is reachable from the public internet. See the
[Merkle Service](#merkle-service) section below for the mainnet endpoint.

### testnet — public BSV testnet

```yaml
network: testnet
```

Built-in bootstrap peers; no other changes needed.

### teratestnet — Teranode scaling testnet

The base config already targets `teratestnet`. No changes needed.

### regtest — local / private network

```yaml
network: regtest

p2p:
  datahub_discovery: true
  bootstrap_peers:
    - <your regtest peer multiaddr, e.g. /ip4/127.0.0.1/tcp/9905/p2p/12D3KooW...>

# chaintracks_server is force-disabled for regtest (no genesis header)
```

Two regtest-specific constraints, enforced at config load:

- `p2p.bootstrap_peers` is required whenever `p2p.datahub_discovery: true` (or
  set `datahub_discovery: false` and use `datahub_urls` instead).
- `chaintracks_server` is auto-disabled regardless of what you set; the
  embedded chaintracks library has no regtest genesis header.

## Merkle Service

Arcade delegates merkle-proof construction to a separate **Merkle Service**.
The service watches the network for arcade's registered txids and drives the
transaction lifecycle forward via callbacks
(`SEEN_ON_NETWORK` → `MINED` → `IMMUTABLE`).

**If `merkle_service.url` is left empty, arcade still accepts and broadcasts
transactions, but every row stays at `RECEIVED` forever** — no callback source
means nothing advances the state machine. Configure Merkle Service whenever
you want real status progression.

### Public Merkle Service endpoints

| Network       | URL                                                 |
| ------------- | --------------------------------------------------- |
| `mainnet`     | `https://merkle-service-us-1.bsvb.tech`             |
| `testnet`     | `https://merkle-service-testnet-us-1.bsvb.tech`     |
| `teratestnet` | `https://merkle-service-ttn-us-1.bsvb.tech`         |
| `regtest`     | bring your own — no public instance                 |

### Generate a callback auth token

Merkle Service authenticates its inbound callbacks to arcade with a bearer
token. Generate a high-entropy value:

```bash
openssl rand -hex 32
```

The same value must also be configured on the Merkle Service side so it can
attach `Authorization: Bearer <token>` to its callback requests. Arcade
**refuses to start** when `merkle_service.url` is set without `callback_token`
— an unauthenticated callback receiver would accept forged status updates for
any txid.

### Set `callback_url`

`callback_url` is the **public URL at which Merkle Service can reach this
arcade instance**, with path `/api/v1/merkle-service/callback`. Merkle
Service POSTs status updates here, so it must be resolvable and routable
*from Merkle Service*, not just from your laptop.

Examples:

- Public deployment: `https://arcade.example.com/api/v1/merkle-service/callback`
- Local dev with Merkle Service running in Docker on the same host:
  `http://host.docker.internal:8080/api/v1/merkle-service/callback`

### Example config block

Append the following to your `config.yaml`. Note that `callback_url` and
`callback_token` are **top-level** keys; only `url` and `auth_token` live
under `merkle_service:`.

```yaml
callback_url: "https://arcade.example.com/api/v1/merkle-service/callback"
callback_token: "<output of openssl rand -hex 32>"

merkle_service:
  url: "https://merkle-service-ttn-us-1.bsvb.tech"  # match your network
  auth_token: ""  # only if your Merkle Service requires outbound auth from arcade
```

## Verify it's running

```bash
# liveness
curl http://localhost:8081/health

# api-server health (includes datahub URL inventory)
curl http://localhost:8080/health

# interactive API docs (Scalar UI)
open http://localhost:8080/
```

Submit a transaction:

```bash
curl -X POST http://localhost:8080/tx \
  -H "Content-Type: text/plain" \
  --data "<hex-encoded-transaction>"
```

Look up its status:

```bash
curl http://localhost:8080/tx/<txid>
```

## Common next steps

- **Enable Merkle Service callbacks** — without this, all transactions stay
  at `RECEIVED`. See the [Merkle Service](#merkle-service) section above.
- **Use embedded PostgreSQL instead of Pebble** — uncomment the alternative
  `store:` block in [`config.example.standalone.yaml`](../config.example.standalone.yaml).
  The first run extracts the postgres binary, which takes a few seconds.
- **Production deployment** — start from
  [`config.example.yaml`](../config.example.yaml), which uses external Kafka
  brokers and an Aerospike cluster. Pre-create Kafka topics per
  [`docs/production-kafka.md`](production-kafka.md) — arcade does not create
  them and `arcade.propagation` has a hard partition-count constraint.

Any value above can be overridden by an environment variable prefixed with
`ARCADE_`, e.g. `ARCADE_LOG_LEVEL=debug` or `ARCADE_NETWORK=mainnet`.
