## 1. Dependencies & config

- [x] 1.1 Add `github.com/bsv-blockchain/go-p2p-message-bus` (v0.1.16 or later) to `go.mod` via `go get`.
- [x] 1.2 Run `go mod tidy` and confirm only the message-bus plus its transitive libp2p stack was added.
- [x] 1.3 Extend `config.P2PConfig` with `DatahubDiscovery bool`, `ListenPort int`, `BootstrapPeers []string`, `DHTMode string`, `PrivateKeyPath string`, `TopicPrefix string`, `EnableMDNS bool`, `AllowPrivateURLs bool` (mapstructure tags matching the YAML keys in design.md Decision 3).
- [x] 1.4 Set `viper.SetDefault` values: `p2p.datahub_discovery=false`, `p2p.listen_port=9905`, `p2p.dht_mode="client"`, `p2p.topic_prefix="mainnet"`, `p2p.enable_mdns=false`, `p2p.allow_private_urls=false`.
- [x] 1.5 In `config.validate`, return an error when `cfg.P2P.DatahubDiscovery && len(cfg.P2P.BootstrapPeers) == 0`.

## 2. Teranode client endpoint registry

- [x] 2.1 In `teranode/client.go`, add `sync.RWMutex` and `seen map[string]struct{}` fields to `Client`.
- [x] 2.2 Rewrite `NewClient` to seed the `seen` map from the initial endpoints (after trailing-slash trim) so statically configured URLs can't be re-registered as duplicates.
- [x] 2.3 Implement `AddEndpoints(urls []string) int` — trims a single trailing slash, dedupes via `seen`, appends survivors to `endpoints`, returns count added. Hold the write lock for the entire mutation.
- [x] 2.4 Update `GetEndpoints()` to hold the read lock and return a defensive copy of the slice.
- [x] 2.5 Write a table-driven test `TestClient_AddEndpoints_Dedup` covering: novel URL added, exact duplicate ignored, trailing-slash variant deduplicated, statically-configured URL announced by peer is deduplicated.
- [x] 2.6 Write `TestClient_EndpointsConcurrency` spawning N writers and M readers under `-race` to verify the mutex pattern.

## 3. p2p_client service real implementation

- [x] 3.1 Replace the stub in `services/p2p_client/client.go` — keep the existing `BlockNotification` type and the dedup/cleanup helpers (still useful if we later subscribe to blocks), but wire the lifecycle to a real `go-p2p-message-bus` client.
- [x] 3.2 Expand the `Client` struct to hold `bus p2p.Client`, `teranode *teranode.Client`, `done chan struct{}` and a `sync.WaitGroup` for subscription goroutines.
- [x] 3.3 Change `New(...)` signature to also take `*teranode.Client` so the service can call `AddEndpoints`. Update `cmd/arcade/main.go:buildServices` to pass it.
- [x] 3.4 In `Start(ctx)`, short-circuit to `return nil` if `cfg.P2P.DatahubDiscovery == false` — no libp2p connect, no goroutines.
- [x] 3.5 When enabled, load or generate the libp2p private key: read `cfg.P2P.PrivateKeyPath` if set, else generate a fresh key via `p2p.GeneratePrivateKey()` and log the peer ID it corresponds to.
- [x] 3.6 Construct the `p2p.Client` via `p2p.NewClient(p2p.Config{Name: "arcade", Logger, PrivateKey, Port: cfg.P2P.ListenPort, BootstrapPeers: cfg.P2P.BootstrapPeers, DHTMode: cfg.P2P.DHTMode, ...})`. Propagate errors up.
- [x] 3.7 Subscribe to the topic `fmt.Sprintf("%s-node_status", cfg.P2P.TopicPrefix)` and log the full topic string at info level.
- [x] 3.8 Launch a goroutine reading from the subscription channel; on each message call a new `handleNodeStatus(msg p2p.Message)` method. Guard the loop with `select { case <-ctx.Done(): return }` so shutdown is prompt.

## 4. NodeStatusMessage parsing & URL validation

- [x] 4.1 Add `services/p2p_client/node_status.go` defining `type nodeStatusMessage struct { PeerID, BaseURL, PropagationURL string ... }` mirroring the teranode struct (only unmarshal the fields we need; unknown fields are ignored by encoding/json).
- [x] 4.2 Implement `pickDatahubURL(m nodeStatusMessage) string` returning `PropagationURL` when non-empty, else `BaseURL`, else `""`.
- [x] 4.3 Implement `validateURL(raw string, allowPrivate bool) (string, error)` — `url.Parse`, enforce `http`/`https`, require non-empty host, reject loopback/link-local/RFC1918 unless `allowPrivate`, trim a single trailing `/` on the returned normalized form.
- [x] 4.4 In `handleNodeStatus`, unmarshal the payload; if the payload's `PeerID` doesn't match `msg.FromID`, log at `warn` and drop (mirrors teranode's spoofing check).
- [x] 4.5 Pick the URL, validate it; on reject log at `warn` with the peer ID and the offending URL. On accept call `c.teranode.AddEndpoints([]string{normalized})`; when the returned count is 1, log at `info` "registered peer datahub URL".

## 5. Wiring & main.go

- [x] 5.1 In `cmd/arcade/main.go` where `teranodeClient` is constructed, pass the same pointer into the new `p2p_client.New`.
- [x] 5.2 Register the p2p_client service in `buildServices` for the `all` and `propagation` modes. Omit for modes that don't broadcast (e.g. `tx-validator`).
- [x] 5.3 Confirm the service's `Stop` is invoked during the existing sequential-shutdown loop. Add a 10-second bound on the `p2p.Client.Close()` call to avoid hanging shutdown.

## 6. Tests

- [x] 6.1 `services/p2p_client/node_status_test.go`: cover `pickDatahubURL` (both set, only BaseURL, both empty) and `validateURL` (public HTTPS, RFC1918 rejected, loopback rejected, loopback allowed with opt-in, ftp rejected, empty-host rejected, trailing-slash trim).
- [x] 6.2 `services/p2p_client/client_test.go`: inject a fake `p2p.Client` (interface extraction around `Subscribe`) and feed hand-crafted `p2p.Message` values into its channel; assert calls to a fake `teranode.Client.AddEndpoints`.
  - [x] Novel URL → registered once.
  - [x] Repeated announcement → registered once.
  - [x] Malformed JSON → no registration, warn logged.
  - [x] Spoofed `PeerID` (differs from `msg.FromID`) → no registration, warn logged.
- [x] 6.3 Startup integration test: `cfg.P2P.DatahubDiscovery = false` — `Start` returns immediately and no libp2p port is opened (verify by attempting to listen on the configured port after Start).
- [x] 6.4 Config validation test: discovery enabled + empty bootstrap peers → `config.Load` or `Validate` returns a non-nil error whose message mentions `bootstrap_peers`.

## 7. Observability & docs

- [x] 7.1 Log at service start: "p2p discovery enabled: topic=%s peer_id=%s bootstrap=%d", with a matching "p2p discovery disabled" line when the flag is false so operators can confirm intent from logs alone.
- [x] 7.2 Log the running endpoint count at `debug` level each time `AddEndpoints` returns a non-zero count.
- [x] 7.3 Update `config.yaml` example (if one exists in the repo — check `openspec/specs/` and top-level sample files) with the new `p2p.*` fields commented out by default.
- [x] 7.4 Add a short README section or inline package doc at the top of `services/p2p_client/client.go` explaining: what the service does when enabled, what it does when disabled, and how discovered URLs interact with statically configured ones.

## 8. Verification

- [x] 8.1 `go build ./...` and `go vet ./...` both clean.
- [x] 8.2 `go test -race ./...` green, including the new concurrency test on `teranode.Client`. (All packages pass; a pre-existing `services/api_server/chaintracks_routes.go` SSE-writer race fails at HEAD and is unrelated to this change.)
- [ ] 8.3 End-to-end smoke: run arcade against a local teranode (or a hand-rolled publisher that sends a `NodeStatusMessage` to the topic) and verify the URL shows up in arcade's endpoint list within one tick of the first announcement.
- [ ] 8.4 Default-config smoke: run arcade with no `p2p` config section — behavior is identical to today (static URLs only, no libp2p listen port opened).
