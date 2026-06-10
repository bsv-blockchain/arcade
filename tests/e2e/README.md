# End-to-end smoke tests

This directory holds the arcade ↔ merkle-service integration tests. The
suite boots a real, digest-pinned `ghcr.io/bsv-blockchain/merkle-service`
container alongside Postgres + Redpanda (via testcontainers-go), wires
arcade in-process against them, and drives representative scenarios.

> **Note:** the merkle-service image is pinned to an immutable digest
> (`defaultMerkleImage` in `harness/containers.go`) rather than `:latest`.
> The floating `:latest` tag is republished out-of-band and has regressed
> the block round-trip before; bump the pinned digest deliberately once a
> newer build is confirmed compatible.

## Layout

```
tests/e2e/
├── doc.go                       // package marker (e2e build tag)
├── smoke_test.go                // first scenario: tx → arcade → merkle-service
└── harness/
    ├── containers.go            // testcontainers wiring (Postgres, Redpanda, merkle-service)
    ├── libp2p_host.go           // in-process libp2p peer + Subtree/Block publishers
    ├── datahub.go               // in-process /block/<hash> + /subtree/<hash> server
    ├── txbuilder.go             // synthetic-tx + synthetic-block builders
    ├── arcade.go                // in-process arcade boot (via app.Bootstrap)
    ├── poll.go                  // BroadcastTx, WaitForMined, GetTxStatus
    └── harness.go               // top-level Harness type, Option functions
```

All harness files (and tests under this tree) carry the `e2e` build tag,
so the default `go test ./...` skips them. Run explicitly with:

```sh
go test -tags=e2e -timeout=15m ./tests/e2e/...
```

## Container runtime

Tests use [testcontainers-go](https://golang.testcontainers.org/), which
talks to whatever Docker-compatible daemon the `DOCKER_HOST` env var
points at.

### Docker (default)

Nothing extra: testcontainers-go finds `/var/run/docker.sock` (or the
Docker Desktop socket) automatically.

### Podman (rootless on Linux)

```sh
systemctl --user start podman.socket   # one-time per session
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
go test -tags=e2e -timeout=15m ./tests/e2e/...
```

The harness reaches the host from inside containers two ways:

1. **Docker bridge gateway IP** — auto-discovered by `harness.New()`
   via `network.NetworkInspect`. Used as the announce address for the
   in-process libp2p host so merkle-service can dial back. Works on
   Docker engines (the gateway IP routes to the host).
2. **`host.docker.internal`** — fallback when no gateway is available.
   Honored by Docker via `--add-host=host.docker.internal:host-gateway`.

**Known limitation on rootless podman + pasta networking:** if pasta
is started with `--no-map-gw` (the default for security), the host is
not reachable from inside containers via either the gateway IP or
`host.docker.internal`. Tests that require merkle-service to dial
back into the harness — `TestLibP2PHost_MerkleServicePeersWithHost`,
`TestSmoke_TxRegistersWithMerkleService`,
`TestSmoke_RealBlockMined_SingleSubtree`, and
`TestSmoke_RealBlockMined_ViaReprocess` (the last one fails its
`/reprocess` call with `no DataHub could serve the requested block`
because merkle-service can't reach the harness datahub at the
gateway IP) — will time out under this configuration. CI runners
use Docker so this doesn't affect the required PR gate.

To verify your setup: `podman run --rm alpine sh -c 'nc -zv -w2
host.docker.internal 22 || echo NOT REACHABLE'`. If the host is
reachable, the full e2e suite works locally.

## Disabling Ryuk (the testcontainers reaper)

Testcontainers ships a "Ryuk" sidecar to clean up containers when the
test process dies unexpectedly. On rootless podman it's flaky; the
harness sets `TESTCONTAINERS_RYUK_DISABLED=true` in CI and we
recommend the same locally:

```sh
export TESTCONTAINERS_RYUK_DISABLED=true
```

`t.Cleanup` already tears every container down on a normal exit, so
disabling Ryuk only matters if a test process is `kill -9`'d.

## Adding a new scenario

1. Write a `_test.go` under `tests/e2e/` with the `e2e` build tag.
2. Build a harness — `harness.New(t)` wires the bridge network,
   discovers the gateway IP, and auto-builds the libp2p host so
   merkle-service can dial back regardless of runtime:
   ```go
   h := harness.New(t)              // containers + libp2p host
   datahub := h.NewDatahub(t)       // gateway-IP-aware datahub
   rt := harness.StartArcade(t, harness.ArcadeOptions{
       MerkleServiceURL: h.Containers.MerkleHostURL,
       DatahubURL:       datahub.LocalURL(),  // arcade reaches via loopback
       LibP2PBootstrap:  h.LibP2P.BootstrapMultiaddr(),
       MerkleAuthToken:  "...",
       CallbackToken:    "...",
   })
   ```
3. Drive the scenario via the helpers in `harness/poll.go`
   (`BroadcastTx`, `BroadcastRawTxs`, `GetTxStatus`, `WaitForMined`,
   `WaitForMerkleRegistration`, `AssertMerklePathsMatchHeaderRoot`)
   and the libp2p publish methods (`PublishBlock`,
   `PublishSubtree`).
4. Use `harness.Containers.WaitForMerkleLogLine` if you need a
   merkle-service-side signal that isn't surfaced via HTTP.

## Real-block fixtures

Some scenarios use real BSV mainnet block data to dodge the
synthetic-merkle-tree-consistency problem (issue #135). Fixtures live
under `tests/e2e/fixtures/blocks/<block-hash>/`:

```
block.bin                  # bytes the harness datahub serves at /block/<hash>
subtrees/<hash>.bin        # concat 32-byte tx hashes per subtree
txs/<txid>.bin             # raw tx body for each picked watch target
meta.json                  # height, merkleRoot, picked txids, provenance
```

Generate or refresh a fixture with the bundled tool:

```sh
go run ./tools/fetch-block-fixture \
    --block <block-hash> \
    --out tests/e2e/fixtures/blocks/<block-hash>
```

The tool fetches the block binary from a teranode datahub
(default `bsva-ovh-teranode-eu-1.bsvb.tech:8000/api/v1`), reconstructs
the subtree binaries from WhatsOnChain's txid list, picks 10 random
non-coinbase txids, and downloads their raw bytes. The merkle math
self-check fails loudly if the txid list and the block header
disagree.

The shipped fixture for
`000000000000000001bc8a601dd5f0659d36a9b077808850375dfa2d9f009396`
(height 948351, 1 subtree, 1911 txs) drives
`TestSmoke_RealBlockMined_SingleSubtree`. **Multi-subtree blocks
are deferred** — current mainnet load fits in one subtree per block,
so a multi-subtree scenario needs teratestnet or scaling-net fixtures
(follow-up; would also need teratestnet datahub access). When that
lands, drop a fixture under
`tests/e2e/fixtures/blocks/<teratestnet-hash>` and add a
`TestSmoke_RealBlockMined_MultiSubtree` variant.

## Block-processing watchdog

`services/watchdog/watchdog.go` is a standalone arcade service
(`mode=watchdog`) that periodically queries `block_processing` for
rows whose chaintracks header arrived but whose BLOCK_PROCESSED
callback never landed, then calls merkle-service `POST /reprocess` to
recover. The smoke tests don't have their own scenario for this —
`TestSmoke_RealBlockMined_ViaReprocess` exercises the underlying
`/reprocess` machinery, and the watchdog's per-tick behavior is
covered by `services/watchdog/watchdog_test.go`.

## Microservice topology vs. e2e harness

In production, arcade runs each mode (api-server, sse, chaintracks,
watchdog, bump-builder, propagation, tx-validator, p2p-client) as a
separate pod — see `deploy/*.yaml`. The e2e harness here boots
`mode=all`, which launches every service as an in-process goroutine
sharing a single Kafka broker and store. The harness exercises the
same code paths the per-pod deployments do; only the supervisor
boundary differs.

## CI

`.github/workflows/e2e-smoke.yml` runs the full suite on every PR and
push to main. The workflow pre-pulls the merkle-service image so cold-
start time stays out of the test's own wait budgets. The job is the
required gate listed in repo settings.

## Known gaps

See `MERKLE_SERVICE_GAPS.md` in this directory for the friction points
the harness work surfaced — file these as issues against
`/git/merkle-service` to make future smoke tests easier.
