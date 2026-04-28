## ADDED Requirements

### Requirement: Subscribe to node_status topic

The p2p_client service SHALL subscribe to the canonical teranode `node_status` topic via `go-teranode-p2p-client.Client.SubscribeNodeStatus(ctx)` when `p2p.datahub_discovery` is enabled, using `p2p.network` (default `main`) to pick bootstrap peers and topic network name.

#### Scenario: Discovery enabled on a known network

- **WHEN** arcade starts with `p2p.datahub_discovery: true` and `p2p.network: main` (or unset)
- **THEN** the p2p_client service creates a `go-teranode-p2p-client` client using the library's embedded mainnet bootstrap peers, subscribes to `teranode/bitcoin/1.0.0/mainnet-node_status`, and begins processing `NodeStatusMessage` payloads.

#### Scenario: Discovery disabled

- **WHEN** arcade starts with `p2p.datahub_discovery: false` (the default)
- **THEN** the p2p_client service's `Start()` returns successfully without creating a libp2p client, opening a port, or subscribing to any topic.

#### Scenario: Alternate network

- **WHEN** `p2p.network` is set to `test`
- **THEN** the service subscribes to `teranode/bitcoin/1.0.0/testnet-node_status` and uses the library's embedded testnet bootstrap peers.

### Requirement: Parse NodeStatusMessage payloads

The service SHALL decode each received message on the node_status topic as JSON matching the teranode `NodeStatusMessage` shape, and SHALL extract a datahub URL by preferring `PropagationURL` when non-empty, falling back to `BaseURL` otherwise.

#### Scenario: PropagationURL present

- **WHEN** a `NodeStatusMessage` arrives with `PropagationURL: "https://prop.peer.example"` and `BaseURL: "https://hub.peer.example"`
- **THEN** the service considers the URL-for-discovery to be `https://prop.peer.example`.

#### Scenario: PropagationURL empty, BaseURL present

- **WHEN** a `NodeStatusMessage` arrives with `PropagationURL: ""` and `BaseURL: "https://hub.peer.example"`
- **THEN** the service considers the URL-for-discovery to be `https://hub.peer.example`.

#### Scenario: Both URLs empty

- **WHEN** a `NodeStatusMessage` arrives with both `PropagationURL` and `BaseURL` empty
- **THEN** the service logs a debug message identifying the peer and skips the announcement without adding any URL.

#### Scenario: Malformed JSON

- **WHEN** a message's payload fails to decode as JSON into `NodeStatusMessage`
- **THEN** the service logs a warning including the peer ID, does not add any URL, and continues processing subsequent messages.

### Requirement: Reject unsafe URLs

The service SHALL reject any discovered URL whose scheme is not `http` or `https`, or whose host resolves to a loopback, link-local, or RFC1918 private range, unless the operator explicitly opts in via `p2p.allow_private_urls: true`.

#### Scenario: Public HTTPS URL

- **WHEN** a peer announces `https://public.example.com`
- **THEN** the URL passes validation and is submitted for registration.

#### Scenario: Private RFC1918 URL with default config

- **WHEN** a peer announces `http://192.168.5.10:8080` and `p2p.allow_private_urls` is false
- **THEN** the URL is rejected, a warning including the peer ID and the rejected URL is logged, and the URL is not registered.

#### Scenario: Loopback URL with opt-in

- **WHEN** a peer announces `http://127.0.0.1:8080` and `p2p.allow_private_urls` is true
- **THEN** the URL passes validation and is submitted for registration.

#### Scenario: Non-HTTP scheme

- **WHEN** a peer announces `ftp://peer.example/` or `file:///etc/passwd`
- **THEN** the URL is rejected and logged, regardless of `allow_private_urls`.

#### Scenario: Unparseable URL

- **WHEN** a peer announces a string that fails `url.Parse` or has an empty host
- **THEN** the URL is rejected and logged.

### Requirement: Register discovered URLs with the propagation endpoint list

When a valid URL passes validation, the service SHALL register it with the shared `teranode.Client` via `AddEndpoints`, which deduplicates against both the statically configured `datahub_urls` and any previously registered URL.

#### Scenario: First announcement of a novel URL

- **WHEN** a peer announces `https://new.peer.example` and no such URL is currently in the endpoint list
- **THEN** the URL is appended to the endpoint list, and the next call to `teranode.Client.GetEndpoints()` returns it.

#### Scenario: Duplicate announcement

- **WHEN** a peer re-announces a URL already present in the endpoint list (either statically configured or previously discovered)
- **THEN** the endpoint list length is unchanged and no duplicate log is emitted at info level.

#### Scenario: URL differing only by trailing slash

- **WHEN** the statically configured `datahub_urls` contains `https://hub.example.com` and a peer announces `https://hub.example.com/`
- **THEN** both are treated as the same URL and the announcement is deduplicated.

#### Scenario: Two peers announcing the same URL

- **WHEN** peer A and peer B both announce `https://shared.example`
- **THEN** the URL is added exactly once on the first announcement; the second is silently deduplicated.

### Requirement: Preserve statically configured URLs

The `datahub_urls` configured in the config file SHALL always be present in the runtime endpoint list regardless of discovery activity. Discovered URLs SHALL be additive and SHALL NOT reorder, overwrite, or evict statically configured URLs.

#### Scenario: Static URLs seeded at startup

- **WHEN** arcade starts with `datahub_urls: ["https://static-1.example", "https://static-2.example"]` and `p2p.datahub_discovery` either true or false
- **THEN** `teranode.Client.GetEndpoints()` returns both static URLs immediately after `NewClient`, in the configured order.

#### Scenario: Discovery does not remove static URLs

- **WHEN** discovery is enabled and a peer announces a new URL
- **THEN** the statically configured URLs remain in the endpoint list in their original order, with the discovered URL appended after them.

### Requirement: Concurrency-safe endpoint mutation

`teranode.Client.AddEndpoints` and `GetEndpoints` SHALL be safe to call concurrently from any goroutine. `GetEndpoints` SHALL return a snapshot copy so callers may iterate without external locking.

#### Scenario: Concurrent add and read

- **WHEN** one goroutine calls `AddEndpoints` while another iterates the slice returned by `GetEndpoints`
- **THEN** neither the iteration nor the append races, and `-race`-tagged tests pass.

#### Scenario: Snapshot independence

- **WHEN** a caller holds a slice returned by `GetEndpoints` and `AddEndpoints` is subsequently called
- **THEN** the previously-returned slice is unchanged (subsequent writes do not mutate it).

### Requirement: Graceful lifecycle

The p2p_client service SHALL implement the `services.Service` interface. `Start` SHALL return only after the libp2p client has connected to at least one bootstrap peer or the attempt has failed; `Stop` SHALL cancel the subscription loop, close the libp2p client, and return only after all spawned goroutines have exited.

#### Scenario: Clean shutdown

- **WHEN** the process context is cancelled
- **THEN** the service's `Stop` returns within 10 seconds, releases the libp2p listen port, and no goroutines from the service remain.

#### Scenario: Bootstrap unreachable

- **WHEN** every bootstrap peer is unreachable at startup
- **THEN** `Start` logs an error but does not block shutdown; the rest of arcade continues running with the static `datahub_urls` only.

### Requirement: Validate config when discovery is enabled on an unknown network

`config.Validate` SHALL fail fast at startup if `p2p.datahub_discovery` is true, `p2p.network` is not one of the networks with embedded bootstrap peers (`main`/`test`/`stn`, or their aliases), and `p2p.bootstrap_peers` is empty.

#### Scenario: Discovery enabled on unknown network without bootstrap peers

- **WHEN** `p2p.datahub_discovery: true`, `p2p.network: teratestnet`, and `p2p.bootstrap_peers: []`
- **THEN** arcade fails to start with an error message identifying the missing `p2p.bootstrap_peers` field.

#### Scenario: Discovery enabled on known network without bootstrap peers

- **WHEN** `p2p.datahub_discovery: true`, `p2p.network: main`, and `p2p.bootstrap_peers: []`
- **THEN** arcade starts normally and uses the library's embedded mainnet bootstrap peers.

#### Scenario: Discovery disabled without bootstrap peers

- **WHEN** `p2p.datahub_discovery: false` and `p2p.bootstrap_peers: []`
- **THEN** arcade starts normally.
