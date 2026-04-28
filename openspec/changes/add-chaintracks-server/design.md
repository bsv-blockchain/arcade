## Context

The original arcade (`/git/arcade`) embeds `github.com/bsv-blockchain/go-chaintracks` v1.1.5 and mounts its Fiber-framework routes under `/chaintracks/v1/*` and `/chaintracks/v2/*`. It runs chaintracks by default unless `chaintracks_server.enabled=false`. Chaintracks itself tracks BSV block headers by subscribing to peers via `go-teranode-p2p-client`, persists headers under a storage directory, and offers both JSON and binary endpoints plus SSE streams for tip/reorg events.

The refactor is service-split and uses the Gin HTTP router, not Fiber. It has no chaintracks integration today. Consumers that previously relied on a single arcade binary for both tx broadcasting and header lookups cannot point at the refactor until this feature is restored.

Stakeholders: SPV validators that hit `/chaintracks/*` for block metadata; merkle-path verifiers that need the BUMP builder's `blockHeight`; ops who configure the service via `config.yaml`.

## Goals / Non-Goals

**Goals:**
- Restore default-on chaintracks serving so the refactor's `api-server` is a drop-in replacement for the original's single-binary surface.
- Keep the existing Gin router — don't introduce a second HTTP framework in the process.
- Mode-gated: only run chaintracks when the `api-server` service is active (`mode=all` or `mode=api-server`).
- Graceful: context-driven start/stop; no goroutine leaks on shutdown.
- Wire tip and reorg Server-Sent Events so clients can subscribe at `/chaintracks/v2/tip/stream` and `/chaintracks/v2/reorg/stream`.

**Non-Goals:**
- OpenAPI spec merging (original arcade's Scalar docs UI is not present in the refactor).
- Running chaintracks in the `tx-validator`, `propagation`, or `bump-builder` modes — those don't need the header tracker.
- Exposing chaintracks to callers **not** going through the api-server (e.g. internal services in the same process). They can call the `chaintracks.Chaintracks` Go interface directly if needed.
- Authenticating chaintracks endpoints separately. They inherit whatever auth the api-server uses (none today).

## Decisions

### Decision 1 — Gin adapter, not Fiber subapp

**Choice:** Write a thin Gin-based handler set in `services/api_server/chaintracks_routes.go` that implements the same URL surface as `go-chaintracks/routes/fiber/routes.go` by calling the `chaintracks.Chaintracks` interface. Mount it on the existing Gin router under `/chaintracks/`.

**Alternatives considered:**
- **Run Fiber as a sub-app on a separate port.** Clean dependency (zero translation) but operationally awkward: two ports to expose, two TLS configs, docs split. Rejected.
- **Mount Fiber as a Gin handler via `fiber.Adaptor`.** Requires pulling in Fiber + fasthttp transitively and serving Gin requests through a net/http→fasthttp→Fiber bridge. Performance-fragile and adds ~30 MB to the binary. Rejected.

**Rationale:** The chaintracks interface is small (`GetNetwork`, `GetHeight`, `GetTip`, `GetHeaderByHeight`, `GetHeaderByHash`, `GetHeaders`, `Subscribe`, `SubscribeReorg`). Each Fiber handler is mechanical serialization. Porting is ~300 LOC of straightforward code, keeps the dependency graph narrow, and lets us reuse the api-server's existing middleware (request logger, recovery).

### Decision 2 — Initialize in api-server.Start, tie lifecycle to that service's ctx

**Choice:** Add a `chaintracks.Chaintracks` field to `api_server.Server`. Construct it in `Server.Start(ctx)` before `router.ListenAndServe`, passing `ctx` so subscriptions and the P2P client shut down when the service does. Skip initialization if `cfg.ChaintracksServer.Enabled == false`.

**Alternatives considered:**
- **Initialize in `cmd/arcade/main.go` and inject.** Would share the instance with other services if they ever need it. Rejected because no other service does; premature coupling.
- **Separate `chaintracks` service (Service interface).** Clean separation, but the chaintracks routes need to live on the same router as the api-server, which means either a shared router (weird service boundary) or the duplicated-port problem from Decision 1's alternatives.

**Rationale:** Chaintracks is consumed only through HTTP here. Co-locating it with the only consumer keeps wiring simple. The `Service` interface already supports this — `Start(ctx)` is the natural integration point, and `Stop` just lets ctx cancel.

### Decision 3 — Chaintracks creates its own P2P client

**Choice:** Pass `nil` for `p2pClient` to `chaintracksconfig.Initialize`. The library will create its own `go-teranode-p2p-client` using the chaintracks config section.

**Rationale:** The refactor has no central P2P service to share. The alternative — creating a P2P client in `main.go` and threading it to api-server — adds another shared field without benefit. If we ever add a P2P consumer beyond chaintracks, we can refactor then. Matches original arcade's pattern when P2P is not pre-created.

### Decision 4 — Config shape mirrors original

**Choice:** Add two config sections:
- `chaintracks_server.enabled bool` (default `true`) — local to the refactor, gates whether routes are mounted.
- `chaintracks.*` — delegate entirely to `chaintracksconfig.Config` via its own `SetDefaults`, rooted at our top-level storage path (`<storage_path>/chaintracks/`).

**Rationale:** Operators moving from old arcade to refactor should have zero config migration. Using the upstream `chaintracksconfig.Config` means any new fields the library adds flow through without us rewriting defaults.

### Decision 5 — Static-file serving via Gin's built-in

**Choice:** Use `router.StaticFS("/chaintracks", gin.Dir(cfg.Chaintracks.StoragePath, false))` for bulk header downloads. Gin's static handler supports byte-range (via `http.ServeFile`) so partial downloads keep working.

**Rationale:** Original arcade uses Fiber's `Static` with `ByteRange: true, Compress: true, CacheDuration: 1h`. Gin's equivalent omits built-in compression and TTL caching, but those can be added by middleware if needed later — out of scope for parity. Range support is the hard requirement (bulk header files are tens of MB); Gin's `http.ServeFile` provides it.

### Decision 6 — SSE implementation reuses the fiber-routes pattern

**Choice:** For `/chaintracks/v2/tip/stream` and `/reorg/stream`, subscribe via `cm.Subscribe(ctx)` / `cm.SubscribeReorg(ctx)` and fan each update out to all connected Gin clients by writing to `c.Writer` with `text/event-stream` headers and calling `c.Writer.Flush()` after each event. Cleanup on client disconnect via `c.Request.Context().Done()`.

**Rationale:** Direct translation of the Fiber broadcaster goroutine pattern. Gin exposes the raw `http.ResponseWriter` plus `Flusher`, which is all we need for SSE.

## Risks / Trade-offs

- **New external dependency (go-chaintracks, go-teranode-p2p-client)** → bigger go.sum, longer CI builds. Mitigation: pin to the same version the original arcade uses (v1.1.5) to share cache keys.
- **Outbound P2P required at runtime** → environments without peer network will log errors continuously. Mitigation: `chaintracks_server.enabled=false` cleanly disables the whole feature, including the P2P client creation.
- **Initial header sync is slow on first boot** (minutes at mainnet). Mitigation: documented; operators can pre-seed `<storage_path>/chaintracks/` from a running instance or set `chaintracks.bootstrap_url` to an HTTP snapshot. Not a regression vs. original.
- **Port collisions with existing routes** → none today (`/chaintracks` is unused). Mitigation: api-server's route registration happens at startup; collisions would fail at mount time.
- **Gin adapter drift from Fiber adapter** → if go-chaintracks adds new routes, our adapter won't pick them up automatically. Mitigation: pin version in go.mod and treat `go-chaintracks` upgrades as an explicit, reviewed change. Add a comment in the adapter listing the upstream commit it targets.
- **Static-file path traversal** — Gin's `StaticFS` with `gin.Dir(root, false)` disables directory listing but still serves any file under `root`. Mitigation: the chaintracks storage directory only ever contains header files, never secrets; acceptable. We'll leave `listing=false` so clients can't enumerate.
- **SSE goroutine lifetime** — broadcaster goroutine per route must exit on ctx cancel. Mitigation: both goroutines are started from `Server.Start(ctx)` with that ctx; client-side goroutines use `c.Request.Context()`.

## Migration Plan

Greenfield — no existing chaintracks integration in the refactor. Steps:

1. Add the dependency + config additions.
2. Implement the Gin adapter and wire it into `Server.Start`.
3. Default-on deploy: operators who don't want it disable explicitly via config.
4. Document the new routes in `config.example.yaml` with a commented-out `chaintracks_server.enabled: false` line.

**Rollback**: set `chaintracks_server.enabled=false` and redeploy. No schema or data migrations to undo; chaintracks storage directory can be deleted at leisure.

## Open Questions

- **Which network defaults should we use?** Original arcade sets `network=main`. The refactor currently has no `network` config. Should we add a top-level `network` field here or scope it under `chaintracks.network`? **Proposal**: keep it under `chaintracks.network` for now (delegates to upstream defaults). Revisit if another service in the refactor needs network awareness.
- **Bootstrap URL default?** Original leaves it blank; we'll do the same and let ops opt in.
