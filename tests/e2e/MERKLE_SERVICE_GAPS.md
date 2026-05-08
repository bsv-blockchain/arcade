# Gaps in merkle-service that complicated the e2e harness

These are friction points the harness work surfaced. They don't block the
current smoke test — workarounds are documented in
`tests/e2e/harness/` — but each is worth filing as an issue against
[`/git/merkle-service`](https://github.com/bsv-blockchain/merkle-service)
so the next person writing an integration test has an easier time.

## 1. Backend-import drift risk

The SQL backend lives behind a blank import in
`cmd/merkle-service/main.go:16` and `cmd/api-server/main.go:13`. A
well-meaning cleanup that deletes the import would silently drop
Postgres + SQLite support from the published image; our harness
configures `STORE_BACKEND=sql` so it'd fail at container startup with
a confusing "unknown backend" error.

**Suggested fix:** add a `TestImportedBackends` regression test that
runs `go list -deps ./cmd/...` and asserts both `aerospike` and `sql`
backend packages are linked into every entry binary.

## 2. Mandatory external Kafka in all modes

Even the all-in-one binary requires a real broker. Our harness pays
the ~10s startup cost of a Redpanda container per test run; an
"embedded mode" — in-process Kafka shim or a sarama-mock equivalent
gated by `MODE=embedded` — would let smoke tests drop Redpanda
entirely.

**Suggested fix:** add an in-process Kafka backend selectable via
`KAFKA_BACKEND=memory` (mirroring arcade's pattern). Worth tracking
as a feature request.

## 3. `callback.allowPrivateIPs` defaults to false

Every test or local-dev integration must remember to flip this. Same
goes for `datahub.allowPrivateIPs`. We work around it with explicit
env vars in `containers.go`, but it's a checklist of opt-in flags
that's easy to miss.

**Suggested fix:** add a `MERKLE_DEV_MODE=true` env var that loosens
private-IP, NAT, and SSRF gates together so dev/test deployments
don't have to set them piecewise.

## 4. Health endpoint reports `degraded` when peer count is zero

merkle-service's `GET /health` returns 503 (degraded) until at least
one libp2p peer is connected. The harness's wait strategy has to
accept both 200 and 503 because peering only happens after the
container is up — there's no "I'm ready to accept peers" signal
distinct from "I have peers".

**Suggested fix:** split health into two flags — `ready` (HTTP
listener + backend reachable) and `peers_healthy` (libp2p peer count
≥ minimum). Tests can wait on `ready` while operators alert on
`peers_healthy` going false.

## 5. No deterministic block-replay endpoint

If our test publishes a `BlockMessage` via libp2p before merkle-service
finishes peering, the announcement is lost — gossipsub doesn't buffer
for late joiners. We work around it with the
`[CONNECTED] Topic peer <our-id>` log-line check before publishing,
but a deterministic "re-drive this block from your local DataHub
cache" endpoint would be more robust.

**Suggested fix:** extend `POST /reprocess` (already present) so test
harnesses can re-drive a block by hash without re-announcing on
libp2p. Or add a dev-mode `POST /testing/replay-block` with looser
auth.

## 6. No documented "private network" recipe

`p2p.network: regtest` works in our harness because
`go-p2p-message-bus` is network-name-agnostic — it just namespaces
pubsub topics with the network string. But this is implicit; nothing
in merkle-service's docs says regtest is supported, and the only way
to find out is to read upstream library code.

**Suggested fix:** a short `docs/private-network.md` in merkle-service
explaining the `bootstrapPeers` + `dhtMode: off` + arbitrary
network-name combo. Would save the next person an exploration loop.

## What's been worked around in this PR

- merkle-service container env vars set `STORE_BACKEND=sql`,
  `STORE_SQL_DRIVER=postgres`, `CALLBACK_ALLOW_PRIVATE_IPS=true`,
  `DATAHUB_ALLOW_PRIVATE_IPS=true`, `BLOB_STORE_URL=memory:`,
  `P2P_NETWORK=regtest`, `P2P_DHT_MODE=off`, `P2P_BOOTSTRAP_PEERS=<our
  multiaddr>` — see `tests/e2e/harness/containers.go`.
- Wait strategy on `/health` accepts 200 and 503.
- Peering signal uses `[CONNECTED] Topic peer <peer-id>` from the
  container logs (deterministic; visible regardless of msgbus's
  message-based peer tracker).

These keep the harness robust today; closing the gaps above would
make future tests simpler.
