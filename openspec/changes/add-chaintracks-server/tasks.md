## 1. Dependency and Config

- [x] 1.1 Add `github.com/bsv-blockchain/go-chaintracks` (pin the same minor version the original arcade uses) to `go.mod`; run `go mod tidy` and verify the transitive graph includes `go-teranode-p2p-client` and `valyala/fasthttp`.
- [x] 1.2 Add `ChaintracksServerConfig{ Enabled bool }` to `config/config.go`, plus a `Chaintracks chaintracksconfig.Config` field; add `chaintracks_server.enabled=true` viper default; delegate chaintracks defaults to the library's `SetDefaults("chaintracks")`.
- [x] 1.3 Update `config.example.yaml` with a commented `chaintracks_server: { enabled: true }` section and a pointer to the library's config fields.

## 2. Chaintracks Instance Wiring (api-server only)

- [x] 2.1 Add `chaintracks chaintracks.Chaintracks` field to `api_server.Server`; add nil-checks in code paths that touch it (so `Enabled=false` is safe).
- [x] 2.2 In `api_server.Server.Start(ctx)`, before `router.ListenAndServe`, construct the chaintracks instance via `cfg.Chaintracks.Initialize(ctx, "arcade", nil)` when `cfg.ChaintracksServer.Enabled` is true. Default `cfg.Chaintracks.StoragePath` to `<arcade storage>/chaintracks` if empty.
- [x] 2.3 Ensure initialization failures propagate from `Start` so `main.go` surfaces them as a fatal startup error (do not silently disable).
- [x] 2.4 Log `Chaintracks HTTP API enabled` with the storage path + network at info level on success; log `chaintracks disabled` at debug level when `Enabled=false`.

## 3. Gin Route Adapter

- [x] 3.1 Create `services/api_server/chaintracks_routes.go`. Add a `chaintracksRoutes` struct that wraps a `chaintracks.Chaintracks` plus SSE broadcaster state (tip/reorg channels, per-client writers with sync.RWMutex), mirroring `go-chaintracks/routes/fiber/routes.go`.
- [x] 3.2 Implement JSON handlers: `GET /network`, `/height`, `/tip`, `/header/height/:height`, `/header/hash/:hash`, `/headers`.
- [x] 3.3 Implement binary handlers: `GET /tip.bin`, `/header/height/:height.bin`, `/header/hash/:hash.bin`. Each returns the raw 80-byte block header (Content-Type `application/octet-stream`) with the matching height in the `X-Block-Height` response header, or 404.
- [x] 3.4 Implement SSE handlers: `/tip/stream`, `/reorg/stream`. Use `http.Flusher` + `c.Request.Context()` for disconnect detection. Run one broadcaster goroutine per stream that subscribes via `cm.Subscribe(ctx)` / `cm.SubscribeReorg(ctx)` and fans out to connected clients.
- [x] 3.5 Add a `Register(router *gin.RouterGroup)` helper and a `RegisterLegacy(router *gin.RouterGroup)` helper so v1 and v2 can mount the same handlers where paths are identical.
- [x] 3.6 Pin the upstream commit/tag targeted by the adapter in a header comment so future upgrades have a reference point.

## 4. Static File Serving

- [x] 4.1 In `registerRoutes`, when chaintracks is enabled, call `router.StaticFS("/chaintracks", gin.Dir(storagePath, false))` to serve bulk header files. Confirm `http.ServeFile` provides Range support.
- [x] 4.2 Verify the static handler does NOT shadow the chaintracks API routes (`/chaintracks/v1/*`, `/chaintracks/v2/*`). Gin route precedence handles longer-path matches first — if this is flaky, switch to explicit path-prefix splitting and add a test.

## 5. Route Mounting

- [x] 5.1 In `Server.registerRoutes`, when chaintracks is enabled, mount the adapter:
  - `router.Group("/chaintracks/v2")` → `chaintracksRoutes.Register`
  - `router.Group("/chaintracks/v1")` → `chaintracksRoutes.RegisterLegacy`
  - Plus the static handler from 4.1.
- [x] 5.2 Keep chaintracks routes out of the auth middleware if the api-server later adds auth; public-by-default matches original arcade behavior.

## 6. Tests

- [x] 6.1 Unit test: `TestServer_ChaintracksDisabled_Returns404ForChaintracksRoutes`. Set `ChaintracksServer.Enabled=false`, start, assert GET `/chaintracks/v1/height` is 404.
- [x] 6.2 Unit test: `TestServer_ChaintracksEnabled_MountsRoutes`. Use a fake implementing `chaintracks.Chaintracks` that returns canned values. Assert `/network`, `/height`, `/header/height/:h` return expected JSON.
- [x] 6.3 Unit test: `TestChaintracksRoutes_TipStream_ForwardsUpdates`. Wire a fake whose `Subscribe` returns a controlled channel; push one tip; assert the SSE client receives it; close client; assert goroutine exits.
- [x] 6.4 Unit test: `TestChaintracksRoutes_BinaryHeader_Returns80Bytes` — asserts body is exactly 80 bytes and `X-Block-Height` header is set.
- [x] 6.5 Integration: `go test ./services/api_server/...` passes.

## 7. Verification

- [x] 7.1 `go build ./...` clean.
- [x] 7.2 `go vet ./...` clean.
- [x] 7.3 `go test ./...` clean.
- [ ] 7.4 Smoke test: run arcade with `mode=all` against a real or minimal P2P network; `curl http://localhost:<port>/chaintracks/v1/network` returns the configured network; `curl http://localhost:<port>/chaintracks/v1/height` returns the current height (≥ 0, increases over time).
- [ ] 7.5 Smoke test: `curl http://localhost:<port>/chaintracks/v2/tip/stream` stays connected and emits at least one event when a new block arrives.
- [ ] 7.6 Smoke test: start with `chaintracks_server.enabled=false` and confirm no P2P connections are opened (no outbound traffic from the chaintracks P2P port).
