# End-to-end smoke tests

This directory holds the arcade ↔ merkle-service integration tests. The
suite boots a real `ghcr.io/bsv-blockchain/merkle-service:latest`
container alongside Postgres + Redpanda (via testcontainers-go), wires
arcade in-process against them, and drives representative scenarios.

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

The harness uses `host.docker.internal` to let containers reach the in-
process arcade callback URL and in-process datahub. Recent Docker
engines and Docker Desktop honor `--add-host=host.docker.internal:host-
gateway`. Rootless podman exposes the host on the same DNS name when
the container has been allow-listed for slirp4netns host loopback —
testcontainers-go's `WithExtraHosts` handles the wiring.

If you see `connection refused` from the merkle-service container when
it tries to call back to arcade, check:

1. Container has `host.docker.internal` resolved (`docker exec <id>
   getent hosts host.docker.internal`).
2. `arcade.cfg.CallbackURL` uses `host.docker.internal:<port>` (the
   harness sets this automatically — only relevant if you've forked
   the boot path).

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
2. Build a harness:
   ```go
   libp2pHost, _ := harness.NewLibP2PHost(t, "regtest", 0)
   datahub, _   := harness.NewDatahub(t)
   h := harness.New(t, harness.WithBootstrapPeers(libp2pHost.BootstrapMultiaddr()))
   rt := harness.StartArcade(t, harness.ArcadeOptions{...})
   ```
3. Drive the scenario via the helpers in `harness/poll.go`
   (`BroadcastTx`, `GetTxStatus`, `WaitForMined`) and the libp2p
   publish methods (`PublishBlock`, `PublishSubtree`).
4. Use `harness.Containers.WaitForMerkleLogLine` if you need a
   merkle-service-side signal that isn't surfaced via HTTP.

## CI

`.github/workflows/e2e-smoke.yml` runs the full suite on every PR and
push to main. The workflow pre-pulls the merkle-service image so cold-
start time stays out of the test's own wait budgets. The job is the
required gate listed in repo settings.

## Known gaps

See `MERKLE_SERVICE_GAPS.md` in this directory for the friction points
the harness work surfaced — file these as issues against
`/git/merkle-service` to make future smoke tests easier.
