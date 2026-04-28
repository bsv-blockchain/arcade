## Why

The original arcade (`/git/arcade`) embeds a [`go-chaintracks`](https://github.com/bsv-blockchain/go-chaintracks) block-header tracker and exposes its HTTP API by default under `/chaintracks/v1/*` and `/chaintracks/v2/*`. Clients (SPV validators, block explorers, merkle-path verifiers) rely on that surface for block-height lookups, tip subscriptions, and bulk header downloads. The refactored arcade does not yet run chaintracks at all — consumers that previously depended on a single arcade binary now have to stand up a separate chaintracks service. This change restores the default-on behaviour so the refactor is operationally a drop-in replacement.

## What Changes

- Add `github.com/bsv-blockchain/go-chaintracks` as a dependency.
- Initialise a `chaintracks.Chaintracks` instance on `api-server` startup when `mode=all` or `mode=api-server` and the feature is enabled (default enabled), backed by a local storage directory configured via `chaintracks.storage_path`.
- Expose chaintracks HTTP routes on the existing Gin router under `/chaintracks/v1/*` (legacy) and `/chaintracks/v2/*` (current), since the refactor uses Gin rather than the original's Fiber framework. Writing a thin Gin adapter over the `chaintracks.Chaintracks` interface avoids running two HTTP frameworks in the same process.
- Serve bulk header downloads (`mainNet_X.headers`, `mainNetBlockHeaders.json`) as static files from the configured storage path under `/chaintracks/`.
- Merge chaintracks tip updates into the existing API server's Server-Sent Events pipeline (`/chaintracks/v2/tip/stream`, `/chaintracks/v2/reorg/stream`).
- Add config section `chaintracks_server.enabled` (default `true`) + `chaintracks.{network, storage_path, ...}` (delegates to go-chaintracks's own defaults).
- Wire graceful shutdown: chaintracks subscriptions are driven by `context.Context`, so service cancellation unwinds them.

## Capabilities

### New Capabilities

- `chaintracks-server`: embedded block-header tracker running in-process inside arcade's api-server, serving HTTP routes for tip/height/header queries, SSE streams, and bulk header static files. Covers initialization, route registration, default-on behaviour, storage layout, and shutdown.

### Modified Capabilities

<!-- None — this adds a new capability. The existing api-server spec doesn't constrain what routes it exposes, so no delta is required. -->

## Impact

- **New dependency**: `github.com/bsv-blockchain/go-chaintracks v1.1.x` (pulls in `go-p2p-message-bus`, `valyala/fasthttp` transitively). `go mod tidy` will widen the graph; CI cache invalidation expected.
- **HTTP surface**: adds `/chaintracks/v1/*`, `/chaintracks/v2/*`, plus static file routes under `/chaintracks/`. Must not collide with existing arcade routes (none of our handlers use the `/chaintracks` prefix today).
- **Storage**: new on-disk footprint at `<storage_path>/chaintracks/` holding block headers (bounded growth — mainnet header set is ~80 bytes × ~900k blocks ≈ 70 MB).
- **Network**: chaintracks subscribes to P2P block announcements via `go-teranode-p2p-client`; needs outbound network to BSV peers. In environments without outbound peering, the feature should be disabled via `chaintracks_server.enabled=false` rather than left to fail silently.
- **Startup latency**: initial header download on first run can be multi-minute. Documented behaviour; not a regression vs. original arcade.
- **Mode-gated**: only runs in `mode=all` or `mode=api-server`. `tx-validator`, `propagation`, and `bump-builder` modes do not initialize chaintracks.
- **Config**: `config.Config` gains `ChaintracksServerConfig` and `chaintracksconfig.Config` fields; `config.example.yaml` and `setDefaults()` updated.
- **Tests**: api-server tests grow to cover route registration when enabled vs. disabled; no need to exercise chaintracks internals (library is independently tested).
