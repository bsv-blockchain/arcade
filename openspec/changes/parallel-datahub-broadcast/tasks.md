## 1. Configuration

- [x] 1.1 Add `EndpointHealthConfig` struct to `config/config.go` with `FailureThreshold`, `ProbeIntervalMs`, `ProbeTimeoutMs`, and `MinHealthyEndpoints` fields (mapstructure tags matching `propagation.endpoint_health.*`).
- [x] 1.2 Embed `EndpointHealth EndpointHealthConfig` inside the existing `PropagationConfig`.
- [x] 1.3 Register defaults in `setDefaults`: `failure_threshold = 3`, `probe_interval_ms = 30000`, `probe_timeout_ms = 2000`, `min_healthy_endpoints = 0`.
- [x] 1.4 Document the block in `config.example.yaml` and `config.example.standalone.yaml` with short inline comments describing each knob.

## 2. Endpoint health tracker in `teranode.Client`

- [x] 2.1 Add an `endpointHealth` struct (fields: `consecutiveFailures int`, `lastFailure time.Time`, `state` enum `healthy | unhealthy`) and a `health map[string]*endpointHealth` field on `Client`, guarded by the existing `sync.RWMutex`.
- [x] 2.2 Seed a health entry in the `healthy` state inside `NewClient` for every static endpoint and inside `AddEndpoints` for every newly registered URL (use the same normalized key as `seen`).
- [x] 2.3 Implement `RecordSuccess(url string)`: resets `consecutiveFailures` to zero, transitions state to `healthy` if previously `unhealthy`, emits a single structured log line on state transition; no-op for unknown URLs.
- [x] 2.4 Implement `RecordFailure(url string)`: increments `consecutiveFailures`, updates `lastFailure`, transitions state to `unhealthy` once `consecutiveFailures >= failure_threshold`, emits a single structured log line on state transition; no-op for unknown URLs.
- [x] 2.5 Implement `GetHealthyEndpoints() []string`: returns a snapshot copy containing only entries whose state is `healthy`, in the same registration order as `GetEndpoints()`.
- [x] 2.6 Thread the three new config values into `NewClient` via its constructor signature; keep existing callers compiling with zero-value defaults that fall back to the documented defaults inside the constructor.

## 3. Background recovery probe

- [x] 3.1 Add a `probe` goroutine launched by `NewClient`, receiving a context supplied by a new `Client.Start(ctx context.Context)` method (call from the composition root in `cmd/arcade/main.go` so the goroutine is tied to the service lifecycle).
- [x] 3.2 On each tick, build the list of `unhealthy` endpoints under an `RLock`, then issue `GET <url>/health` concurrently with a per-request timeout equal to `probe_timeout_ms`.
- [x] 3.3 Treat any HTTP response received (including 4xx / 5xx) as `RecordSuccess` — we only care whether the peer is reachable. Treat transport errors and context timeouts as `RecordFailure`.
- [x] 3.4 Add `Client.Close()` that cancels the probe goroutine's context and waits for it to exit; wire it into service shutdown in `cmd/arcade/main.go`.

## 4. Min-healthy warning

- [x] 4.1 Track a `belowThreshold bool` flag inside `Client` that remembers whether we are currently under `min_healthy_endpoints`.
- [x] 4.2 On any state transition that changes the healthy count, recompute the flag and emit a single `WARN` log line with the healthy count and threshold only on the false→true crossing; clear the flag on the true→false crossing.
- [x] 4.3 No-op the warning path when `min_healthy_endpoints == 0`.

## 5. Propagator broadcast refactor

- [x] 5.1 Replace `p.teranodeClient.GetEndpoints()` with `GetHealthyEndpoints()` inside `broadcastSingleToEndpoints` and `broadcastBatchToEndpoints` (keep `GetEndpoints` available for diagnostic log lines where appropriate).
- [x] 5.2 In both broadcast functions, derive `broadcastCtx, cancel := context.WithCancel(submitCtx)` and use `broadcastCtx` for each endpoint goroutine's HTTP request.
- [x] 5.3 On the first successful result (200 for single-tx / non-error for batch), call `cancel()` and break out of the aggregation loop; drain the channel with a short non-blocking select to collect any already-completed sibling results for health recording.
- [x] 5.4 Skip `RecordFailure` for siblings whose returned error is a direct consequence of `broadcastCtx` cancellation (check `errors.Is(err, context.Canceled)` combined with `broadcastCtx.Err() != nil`).
- [x] 5.5 For every endpoint result that is *not* a cancellation — successes, transport errors, non-success HTTP codes — call `RecordSuccess` or `RecordFailure` against `p.teranodeClient`.
- [x] 5.6 Handle the "no healthy endpoints" case: log an error identifying the condition and return a `broadcastResult{}` (single-tx) / per-tx nil statuses (batch) so the existing "no verdict" branch applies without introducing new rejection paths.

## 6. Reaper

- [x] 6.1 Verify that `services/propagation/reaper.go` delegates its broadcasts through the same `broadcast*ToEndpoints` helpers so it inherits healthy-endpoint filtering and health recording without additional changes; if not, refactor to go through the helpers.

## 7. Tests

- [x] 7.1 Unit tests for `endpointHealth` transitions: trip after N failures, reset on success, sub-threshold behaviour, unknown URL no-op, `-race`-tagged concurrent writer test.
- [x] 7.2 Unit test for `GetHealthyEndpoints` snapshot independence and registration-order preservation.
- [x] 7.3 Unit test for the probe goroutine: stubbed HTTP server that becomes reachable after N ticks; assert endpoint transitions back to healthy.
- [x] 7.4 Unit test for probe handling of 4xx/5xx — endpoint is treated as reachable and transitions to healthy.
- [x] 7.5 Propagator test with three endpoints where one hangs past `submitCtx`: assert the broadcast returns in ~first-success time, not in the hung endpoint's timeout, using a deterministic fake HTTP server.
- [x] 7.6 Propagator test asserting `RecordFailure` is called for transport errors and non-2xx responses but not for context-canceled siblings of a winning race.
- [x] 7.7 Integration-style test covering the end-to-end: three fake endpoints, one dead from the start, assert that after three broadcasts the dead endpoint no longer receives traffic (tripped to `unhealthy`).
- [x] 7.8 Min-healthy-warning test: assert exactly one WARN log line is produced on the threshold crossing and none on subsequent further-below transitions.

## 8. Wiring and docs

- [x] 8.1 Update `cmd/arcade/main.go` to call `teranode.Client.Start(ctx)` during service startup and `Close()` during shutdown.
- [x] 8.2 Confirm the `p2p_client` service continues to call `AddEndpoints` unchanged — no changes to that service beyond verifying the new health-seed path is exercised.
- [x] 8.3 Update `requirements.md` (if present and relevant) and `CLAUDE.md` to mention the new endpoint-health subsystem and its defaults.
- [x] 8.4 Add a short section to `README.md` (if operator-facing docs exist there) or inline config comments describing when an operator should tune `failure_threshold` or `probe_interval_ms`.

## 9. Validation

- [x] 9.1 Run `go test ./...` with the `-race` flag and confirm all new and existing tests pass.
- [x] 9.2 Run `openspec validate parallel-datahub-broadcast` and confirm no errors.
- [ ] 9.3 Manually exercise against a local teranode with one unreachable endpoint configured; confirm logs show the trip, the skipped broadcasts, and the recovery probe behaviour once the endpoint comes back.
