## Context

Arcade's propagation path broadcasts each accepted transaction to every URL returned by `teranode.Client.GetEndpoints()`. That list is frozen at `cmd/arcade/main.go` startup from `cfg.DatahubURLs`. When the teranode fleet rotates, operators manually edit config and restart.

Meanwhile, teranode nodes already publish a `NodeStatusMessage` JSON payload every ten seconds to the libp2p pubsub topic `teranode/bitcoin/1.0.0/{network}-node_status` (e.g., `teranode/bitcoin/1.0.0/mainnet-node_status`). That message carries `BaseURL` (the datahub HTTP endpoint) and an optional `PropagationURL` — exactly the URL arcade needs to broadcast to. Two existing references inform the implementation:

- `/git/merkle-service/internal/p2p/client.go` — idiomatic libp2p client integration for this codebase family: Start/Stop lifecycle, goroutine-per-subscription, context-driven shutdown. Uses `go-teranode-p2p-client`.
- Teranode's `services/p2p/message_types.go` — canonical `NodeStatusMessage` shape. JSON, no wrapper envelope, peer ID validated against libp2p transport-layer ID.

Arcade already has a stub at `services/p2p_client/client.go` wired through `main.go` but with a `TODO` for the actual libp2p integration. This change replaces the stub with a real implementation.

## Goals / Non-Goals

**Goals:**
- Subscribe to the `node_status` topic via `github.com/bsv-blockchain/go-teranode-p2p-client` and process typed `NodeStatusMessage` payloads.
- Inherit the library's embedded bootstrap peers for `main`/`test`/`stn` and its canonical topic-name builder so operators don't hand-configure either.
- When `p2p.datahub_discovery: true`, merge discovered `PropagationURL`s (falling back to `BaseURL` when empty) into the runtime endpoint list used by `teranode.Client`.
- Preserve statically configured `datahub_urls` — discovered URLs are additive. No overwrite, no reordering.
- Dedupe: never add a URL that's already present (either from config or a prior announcement).
- Survive peer churn: if the same peer re-announces, no duplicate additions, no log spam.
- Default behavior is unchanged (`datahub_discovery` defaults to `false`, libp2p never starts).
- Shutdown is clean: context cancel tears down the libp2p client and the goroutine waits drain inside `Stop()`.

**Non-Goals:**
- **No URL removal / eviction.** If a peer stops announcing, its URL stays in the list. Propagation already tolerates dead endpoints via per-endpoint timeout and failure accounting — eviction would be a separate follow-up.
- **No peer health scoring or weighting.** Every discovered URL is treated equal to a statically configured one.
- **No signature verification beyond what the transport gives us.** Teranode itself doesn't sign `NodeStatusMessage` — we rely on libp2p's transport-layer peer ID identity, same as teranode does. Note that the wrapper library fans out pre-decoded `NodeStatusMessage` values, which means the libp2p `FromID` is not visible at the handler layer. Spoofing defense is delegated to libp2p pubsub signing.
- **No direct use of `go-p2p-message-bus`.** The wrapper pulls it in transitively and exposes typed subscribers + sensible defaults, so we never touch the raw `Subscribe(topic) <-chan Message` surface.

## Decisions

### Decision 1: Library choice — `go-teranode-p2p-client` ≥ v0.2.1

`go-teranode-p2p-client` wraps `go-p2p-message-bus` and adds `SubscribeNodeStatus(ctx) <-chan NodeStatusMessage` (added in v0.2.x). It also ships:

- Embedded bootstrap-peer lists for the `main`, `test`, and `stn` networks so operators flipping `datahub_discovery: true` need no extra config for those networks.
- `TopicName(network, topic)` — the canonical `teranode/bitcoin/1.0.0/{network}-{topic}` builder, so we don't silently drift from teranode's naming.
- `Config.Initialize(ctx, name)` — loads or generates a persistent `p2p_key.hex` under `StoragePath` and wires the msgbus client, keeping the peer ID stable across restarts.

Usage:

```go
cfg := p2pclient.Config{
    Network:     "main",
    StoragePath: "<arcade-storage>/p2p",
    MsgBus:      msgbus.Config{Port, Logger, DHTMode, ...},
}
client, err := cfg.Initialize(ctx, "arcade")
msgs := client.SubscribeNodeStatus(ctx)            // pre-decoded NodeStatusMessage
for m := range msgs { handle(m) }
```

**Trade-off:** the wrapper fans out pre-decoded messages and drops the libp2p `FromID`. We lose the `PeerID == FromID` spoof check that direct msgbus consumption allowed. Accepted: libp2p pubsub signing already binds the sender to the message, and our URL validation is the primary trust boundary.

**Alternatives considered:** use `go-p2p-message-bus` directly — rejected; we'd reimplement the bootstrap-peer defaults, topic-name builder, and persistent-identity plumbing that the wrapper hands us for free.

### Decision 2: URL registry lives on `teranode.Client`, not on a new object

The broadcast loop already calls `teranodeClient.GetEndpoints()` on every send. Adding URLs to that same struct means zero changes to propagation — the next broadcast picks them up automatically.

Shape:

```go
type Client struct {
    mu        sync.RWMutex
    endpoints []string
    seen      map[string]struct{}   // dedup set
    authToken string
    httpClient *http.Client
}

func (c *Client) AddEndpoints(urls []string) (added int) {
    c.mu.Lock()
    defer c.mu.Unlock()
    for _, u := range urls {
        if _, ok := c.seen[u]; ok { continue }
        c.seen[u] = struct{}{}
        c.endpoints = append(c.endpoints, u)
        added++
    }
    return added
}

func (c *Client) GetEndpoints() []string {
    c.mu.RLock()
    defer c.mu.RUnlock()
    out := make([]string, len(c.endpoints))
    copy(out, c.endpoints)
    return out
}
```

`GetEndpoints` returns a copy so callers can iterate without holding the lock. Broadcast rate is O(1s), dedup work is O(1) per URL, the copy is a short slice — the lock window is negligible.

**Alternatives considered:**
- A separate `EndpointRegistry` type: rejected; another pointer to wire through every service constructor for no functional gain.
- `atomic.Value` holding the slice: rejected; writes are rare and we'd still need a lock to coordinate the dedup check with the append.

### Decision 3: Config shape

```yaml
p2p:
  datahub_discovery: false        # master switch — default off
  network: main                   # main | test | stn (embedded bootstraps); any other value requires bootstrap_peers
  listen_port: 9905               # libp2p port
  dht_mode: off                   # off | client | server (library default is off)
  storage_path: ""                # where go-teranode-p2p-client persists p2p_key.hex; defaults to <top-level storage_path>/p2p
  bootstrap_peers: []             # override or extend the embedded defaults
  enable_mdns: false
  allow_private_urls: false
```

`datahub_discovery` is the single switch operators flip. When `false`, the p2p_client service's `Start()` returns immediately — no libp2p connect, no goroutines, no surprises. When `true`, `network` gates bootstrap-peer defaults: `main`/`test`/`stn` inherit the library's embedded peers, so `bootstrap_peers` can stay empty. For any other network value, `config.Validate()` requires an explicit `bootstrap_peers` list.

The config-file `datahub_urls` retains its name and semantics. It's the "seed list": always present in the runtime slice, and the first thing `teranode.NewClient` registers (populating the dedup set so an announcement of a statically-configured URL is silently skipped).

### Decision 4: Topic naming

Topic names come from the library's `TopicName(network, TopicNodeStatus)` — `teranode/bitcoin/1.0.0/{network}-node_status` (e.g. `teranode/bitcoin/1.0.0/mainnet-node_status`). The library maps config network aliases (`main`→`mainnet`, `test`→`testnet`, etc.) internally, so operators don't hand-construct topic strings.

### Decision 5: Ignoring bogus or dangerous URLs

Each parsed `BaseURL`/`PropagationURL` is validated before registration:

- Must parse as `http://` or `https://` (no ftp, no file, no ws).
- Host must not be a loopback (`127.0.0.0/8`, `::1`), link-local (`169.254.0.0/16`), or RFC1918 private range — unless `p2p.allow_private_urls: true` is set (off by default), since a misconfigured peer gossiping `http://192.168.1.5` would otherwise silently redirect traffic. Loopback gets an explicit opt-in for single-host e2e test setups.
- Must pass `url.Parse` without host == "".

Rejected URLs are logged at `warn` level with the peer ID so operators can see misbehaving peers.

**Alternatives considered:** full SSRF reachability check (pre-connect) — rejected; propagation already treats unreachable endpoints as transient failures, and a reachability probe adds a second network dependency to service startup.

### Decision 6: Dedup is by exact URL string, trimmed of trailing slash

Two peers announcing `https://hub.example.com` and `https://hub.example.com/` should be treated as the same. We trim a single trailing `/` on entry. No canonicalization beyond that (no DNS resolution, no port normalization — `:443` and implicit HTTPS port count as distinct, which is acceptable).

### Decision 7: Peer-ID-to-URL binding is not retained

We do not maintain a map from peer ID to URL. The p2p_client service only cares about the URL. If the same peer gossips a different URL tomorrow, we add the new URL (dedup still works because it's by URL), and the old one stays in the list until something else removes it (nothing does — see non-goal on eviction).

This means a single compromised peer could inject many URLs over time. In practice the bootstrap set and libp2p's peer-scoring gate this. If it becomes a problem, evicting stale URLs by last-seen timestamp is an easy follow-up.

## Risks / Trade-offs

- **Stale URLs accumulate forever** → Mitigation: propagation already tolerates dead endpoints (per-send timeout, failure count). A follow-up change can add last-seen-based eviction if the list bloats in practice.
- **Peer gossiping a malicious URL** → Mitigation: URL validation rejects private/loopback addresses by default, and libp2p's peer scoring gates who can publish. Operators running sensitive deployments keep `datahub_discovery: false`.
- **libp2p adds a large transitive dependency tree** (~20 MB of Go modules) → Mitigation: the dependency is always linked, but at runtime nothing libp2p runs until `datahub_discovery` is set. Deployments that don't need discovery pay binary-size cost only.
- **Topic prefix mismatch silently yields no discovery** → Mitigation: log at startup `subscribed to topic {full topic name}` and a debug-level log on every received announcement, so operators can see messages flowing or spot the misconfig quickly.
- **Bootstrap peer outage means zero discovery** → Mitigation: same fail-open behavior as today (arcade runs fine without discovery). Operators see warnings in logs, static `datahub_urls` keep working.

## Migration Plan

1. Ship the code with `datahub_discovery: false` default — existing deployments are untouched.
2. Operators who want it set `p2p.datahub_discovery: true`, supply `bootstrap_peers`, and optionally a persistent `private_key_path` so the peer ID is stable across restarts.
3. No data migration required; no schema changes; no runtime config reload needed (the flag is read once at startup).
4. Rollback: revert the flag to `false` and restart. The static `datahub_urls` resume being the only propagation targets.

## Open Questions

1. **Persist discovered URLs across restart?** Today each restart starts from the static seed list and rebuilds. We could checkpoint discovered URLs to disk for faster warmup. Probably not worth it — a 10-second tick from any live peer rebuilds the list quickly.
2. **Should `PropagationURL` override `BaseURL` when both are present?** Teranode's convention is that `PropagationURL` is the tx-specific endpoint and `BaseURL` is the general datahub. Arcade uses the URL for tx propagation, so `PropagationURL` is the more correct choice when set. Current proposal: prefer `PropagationURL`, fall back to `BaseURL`. Confirm with a teranode operator before locking in.
