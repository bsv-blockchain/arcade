## Why

Arcade operators currently have to hand-maintain the list of datahub/teranode endpoints arcade broadcasts transactions to. That list goes stale the moment the teranode fleet changes — new peers come online, old ones rotate out, and arcade keeps hammering dead endpoints until someone edits config and restarts.

Teranode nodes already gossip their own `BaseURL` on the libp2p `node_status` topic every ten seconds. Arcade can subscribe, parse, and auto-populate its propagation target list from live peer announcements — zero manual config churn, and the list stays warm as the network rotates.

## What Changes

- Add a real implementation behind the existing `services/p2p_client` stub that connects to libp2p via `github.com/bsv-blockchain/go-p2p-message-bus` and subscribes to the `node_status` topic.
- Parse inbound `NodeStatusMessage` payloads (raw JSON, defined by teranode) and extract `BaseURL` / `PropagationURL` as datahub URLs.
- Add a `datahub_discovery` config flag (default `false`) that, when enabled, feeds discovered URLs into the runtime endpoint list used by the propagation service.
- Make `teranode.Client.endpoints` concurrency-safe and mutable at runtime (new `AddEndpoints(urls []string)` method), so discovery can merge peer URLs with statically configured ones without racing the broadcast path.
- Dedupe discovered URLs against statically configured `datahub_urls` and against each other so a noisy peer fleet can't bloat the endpoint list.
- Add config fields for the libp2p client itself (bootstrap peers, listen port, DHT mode, private-key path) mirroring merkle-service's P2P config block.

## Capabilities

### New Capabilities
- `p2p-datahub-discovery`: Subscribes to the libp2p `node_status` topic, parses teranode node-status announcements, and dynamically registers discovered datahub URLs with the propagation path.

### Modified Capabilities
<!-- None — no existing spec documents the propagation endpoint list, and the existing p2p_client package is a stub with no published behavior contract. -->

## Impact

- **New service**: `services/p2p_client` gains a real libp2p integration (the stub is replaced, not kept). It becomes a long-lived service with its own `Start`/`Stop` in `cmd/arcade/main.go`.
- **teranode client**: `teranode.Client` grows a `sync.RWMutex` and an `AddEndpoints` method. `GetEndpoints` becomes a snapshot under read-lock. Callers already invoke `GetEndpoints()` per-broadcast, so no caller changes.
- **Config**: `p2p.datahub_discovery`, `p2p.bootstrap_peers`, `p2p.listen_port`, `p2p.dht_mode`, `p2p.private_key_path`, `p2p.topic_prefix` added to `P2PConfig`. `datahub_urls` semantics change from "the full list" to "the statically configured seed list that gets merged with discovered URLs".
- **Dependencies**: Direct imports `github.com/bsv-blockchain/go-p2p-message-bus` (v0.1.16+) and its transitive libp2p stack (`go-libp2p`, `libp2p-pubsub`, `libp2p-kad-dht`). Adds ~20MB to the final binary.
- **Standalone mode**: `datahub_discovery: false` keeps arcade's behavior identical to today, so the new dependency can ship dormant and get enabled only when an operator opts in.
