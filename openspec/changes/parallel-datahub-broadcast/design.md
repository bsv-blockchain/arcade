## Context

Every broadcast currently fans out to the full endpoint list returned by `teranode.Client.GetEndpoints()` via one goroutine per endpoint, with a shared 15 s `submitCtx`. The success criterion is "any endpoint returned 200" (single-tx) or "any endpoint did not return an error" (batch), but the aggregation loop in both `broadcastSingleToEndpoints` and `broadcastBatchToEndpoints` drains `resultCh` until it is closed — which happens only after `wg.Wait()` returns — so the effective wall-time per broadcast is `max(endpoint_latency)`, not `min(endpoint_latency)`.

With the recently-added p2p datahub discovery, the endpoint list can grow to tens of URLs drawn from the public `node_status` topic. A few of these will be unreachable, firewalled, or slow — and today every transaction pays the cost of dialing them on every broadcast, because `Client` has no memory of past failures. The request-path impact is twofold: (a) broadcasts wait on the slowest endpoint, and (b) sockets and goroutines are continually consumed trying to reach endpoints we already know are bad.

We need to (1) filter the endpoint list to known-healthy peers, (2) treat repeated failures as signal and trip a circuit, (3) recover from the unhealthy state automatically so a temporarily-flapping peer is not sidelined forever, and (4) change the fan-out pattern so a successful answer short-circuits the wait.

## Goals / Non-Goals

**Goals:**

- Reduce broadcast wall-time from `max(endpoint_latency)` to ~`min(healthy_endpoint_latency)` via first-success early-cancel.
- Never dial an endpoint that is currently in the `unhealthy` circuit state during normal broadcast paths.
- Automatically recover an `unhealthy` endpoint without operator intervention when it becomes reachable again.
- Keep the endpoint-health surface entirely inside `teranode.Client` so the propagator, reaper, and any future consumer (e.g. BUMP builder) inherit the behaviour by construction.
- Preserve the existing success semantics: any healthy endpoint accepting a tx is still a successful broadcast.
- Preserve ordering and dedup semantics of `AddEndpoints` / `GetEndpoints` — discovery behaviour is unchanged.

**Non-Goals:**

- Per-tx endpoint selection (e.g. consistent hashing, weighted shuffle). The same set of healthy endpoints is used for every broadcast in a tick.
- Endpoint-scoped retry budgets beyond the simple consecutive-failure counter — no sliding-window rate, no half-open probe success counts.
- Changing the retryable / permanent error classification (`missing inputs`, `txn-mempool-conflict`, `failed to validate`) — that logic stays in `retryable.go`.
- Adding a new Kafka topic, HTTP endpoint, or external dependency.
- Persisting endpoint-health state across restarts. On restart every endpoint starts healthy; this is a deliberate simplification — a truly dead peer will fail three times on first use and re-enter the unhealthy state within seconds.

## Decisions

### 1. Health tracking lives in `teranode.Client`

**Decision:** Extend the existing `Client` with an internal `health map[string]*endpointHealth` keyed on the normalized URL, guarded by the same `sync.RWMutex` already protecting the endpoint list. Expose three methods: `RecordSuccess(url)`, `RecordFailure(url)`, `GetHealthyEndpoints() []string`. The existing `GetEndpoints()` is preserved for callers that genuinely want the full list (e.g. diagnostic pages).

**Rationale:** `Client` already owns endpoint state and thread-safety; adding health to it keeps the invariant "the endpoint list and its health are always consistent" (e.g. a URL removed from `seen` is also removed from `health`). A separate `HealthTracker` struct would create a two-lock dance for no benefit — nothing else needs the tracker independently of the endpoint list.

**Alternative considered:** A dedicated middleware wrapping the HTTP client (e.g. `RoundTripper` that transparently annotates failures). Rejected because the unit of "failure" we care about is "this peer failed to accept a tx", which is a teranode-layer notion, not a transport one — a 404 on an unrelated path or a connection reset mid-body should both count, but only the caller knows whether the overall call succeeded.

### 2. Consecutive-failure threshold, not a sliding window

**Decision:** Each endpoint carries `consecutiveFailures int` and `lastFailure time.Time`. A single `RecordFailure` increments the counter; `RecordSuccess` resets it to zero. An endpoint trips to `unhealthy` when `consecutiveFailures >= failure_threshold` (default 3). Any success (including the probe) moves it straight back to `healthy`.

**Rationale:** We're trying to detect "this peer is broken right now", not do statistical reliability analysis. A consecutive-failure counter is O(1), trivially correct under concurrency (with a mutex), and matches the operator's mental model — "three strikes, you're out". Sliding windows are significantly more complex and add no precision that matters at the scale of "seconds to detect a dead peer".

**Alternative considered:** An EWMA of failure rate, or a token-bucket over `observed_failures / observed_attempts`. Rejected — neither adds anything a consecutive counter does not already capture for our workload, and both require deciding window length and weighting without data to anchor those choices.

### 3. Background probe loop with HTTP GET

**Decision:** `Client` starts a single probe goroutine on `NewClient` that ticks every `probe_interval_ms` (default 30 s). On each tick it iterates endpoints in `unhealthy`, issues `GET <url>/health` with `probe_timeout_ms` (default 2 s), and calls `RecordSuccess` on any 2xx / 3xx response (`RecordFailure` otherwise). A successful probe immediately restores the endpoint to the healthy set, where the next live broadcast will use it.

**Rationale:** A single ticker is cheap and cache-friendly. `/health` is the standard arcade health path (see `HealthConfig`) and teranode peers are expected to expose an equivalent; treating any non-error response as recovery avoids coupling to the exact status-code semantics of a peer we don't control. The probe is the only path that can *reduce* an endpoint's failure count — live traffic only ever resets it on success, so a peer that is merely slow (never succeeds on live traffic because other peers win first) isn't stuck unhealthy.

**Alternative considered:** In-band probes — on every broadcast, also hit one unhealthy endpoint to see if it has recovered. Rejected because it mixes recovery and live traffic, which complicates the early-cancel logic (a probe-only call must not count toward "any success"). A dedicated background goroutine keeps the concerns separate.

**Alternative considered:** Half-open state (one in-flight probe at a time, success promotes, failure demotes with longer backoff). Rejected as premature — we can add it if flapping becomes a real problem, but the simple version is sufficient for the stated pain point.

### 4. First-success early-cancel in broadcast fan-out

**Decision:** Replace the "wait for WaitGroup, then aggregate" pattern in `broadcastSingleToEndpoints` and `broadcastBatchToEndpoints` with:

1. Derive `broadcastCtx, cancel := context.WithCancel(submitCtx)`.
2. Launch one goroutine per healthy endpoint, using `broadcastCtx` for the HTTP request.
3. Goroutines write their result to a buffered channel sized to the endpoint count.
4. The aggregation loop reads from the channel; on the first success it calls `cancel()` and breaks. Remaining goroutines observe context cancellation and return quickly.
5. After the break, a `select` with a short drain timeout collects any in-flight results for health recording, but does not block the caller on them.

**Rationale:** The Go idiom for "race N things, take the first success, cancel the losers" is exactly this shape. The small drain window ensures most failures still reach the health tracker; any that arrive after are irrelevant — the circuit-breaker is eventually-consistent by design.

**Alternative considered:** Using `x/sync/errgroup` with `WithContext`. Rejected because `errgroup` returns the first *error*, not the first *success* — we'd still be hand-rolling the early-success logic, so the dependency buys nothing.

### 5. Batch broadcast binary semantics preserved

**Decision:** `broadcastBatchToEndpoints` keeps its "any endpoint success → all txs accepted" rule, but the success can now be declared as soon as the first endpoint returns 2xx. Other endpoints are cancelled.

**Rationale:** This matches the existing behaviour and the current test fixtures. The early-cancel is a strict improvement: same verdict, less wall-time. Per-endpoint success does not improve any individual tx's outcome because the batch is an all-or-nothing submission from the API's point of view — so waiting for more endpoints after the first success adds latency with no extra value.

### 6. Minimum-healthy-endpoints is soft

**Decision:** `min_healthy_endpoints` (default 0) is a warning threshold: when fewer than N endpoints are healthy, log a `WARN`-level line once per crossing and emit a gauge metric; broadcasts still proceed with whatever healthy set exists. A value of 0 disables the warning. `GetHealthyEndpoints` never returns the statically-configured endpoints as a fallback when the healthy set is empty — an empty set is an empty set, and the existing "no teranode endpoints configured" error path handles it.

**Rationale:** Operators configure this for visibility, not enforcement. If every endpoint is unhealthy the broadcast will fail regardless — there is no "fallback" better than the truth. Treating `min_healthy_endpoints` as an SLO input (warning) rather than a hard gate keeps the broadcast path's failure modes predictable.

**Alternative considered:** Hard-fail the broadcast when `healthy < min_healthy_endpoints` so Kafka retries the message later. Rejected because a transient network hiccup affecting every peer would produce a retry storm that cannot be resolved by retrying — the problem is at the arcade node itself, not a peer.

## Risks / Trade-offs

- **[Thundering probe on recovery]** If a whole peer cluster goes down and comes back, every arcade replica will probe them at roughly the same moment. → **Mitigation:** Probe interval is per-process and ticks from `NewClient` time, so replicas naturally phase-offset. The probe is a cheap `GET /health`, not a broadcast. If this becomes a concern, add ±10 % jitter to the probe tick — trivial to do but not done in v1.
- **[Cold start re-learning]** On restart we re-discover the bad-peer set, so the first few broadcasts after boot will eat the full slow-peer latency for a handful of transactions before peers trip. → **Mitigation:** The failure threshold is low (3) and the submission timeout is already capped at 15 s, so at most ~45 s of degraded broadcast latency occurs after boot. Persisting health state would add a durability concern disproportionate to the benefit.
- **[Probe path mismatch]** Some peers may not expose `/health`; a persistent 404 from the probe would keep an otherwise-working peer stuck unhealthy if live traffic also kept failing. → **Mitigation:** Treat any response received (including 4xx) as "peer is reachable" — `RecordSuccess` on probe if the HTTP request completed without a transport error. This matches what we actually want to know: can we talk to this peer at all? A peer that returns 404s but accepts transactions at `/tx` is still a usable peer.
- **[Early-cancel masking transient 2xx]** If endpoint A returns 200 in 50 ms and endpoint B would also have returned 200 in 60 ms, we record success for A and a cancellation error for B. The cancellation is not a real failure. → **Mitigation:** Goroutines that observe `ctx.Err() != nil` skip `RecordFailure`. Only real transport or HTTP-status failures increment the counter.
- **[Health state for unknown endpoints]** Callers could in theory pass a URL to `RecordSuccess`/`RecordFailure` that `AddEndpoints` never registered. → **Mitigation:** Both methods no-op on unknown URLs. The propagator only ever passes URLs it received from `GetHealthyEndpoints`, so the surface is not reachable by external callers.

## Migration Plan

No migration — the change is wire-compatible. Steps:

1. Ship `teranode.Client` changes behind zero-valued defaults (`failure_threshold = 3`, `probe_interval_ms = 30000`). Existing deployments see identical behaviour until three failures in a row on a given peer.
2. Merge propagator + reaper changes. Existing tests exercise the broadcast paths; new tests cover health recording, circuit trip, and probe recovery.
3. Roll out one service at a time. Validate via the new structured logs that endpoints trip when taken down and recover on the probe cadence.

Rollback is a simple revert — no stored state is written anywhere.

## Open Questions

- Should the probe use `HEAD /` instead of `GET /health` so we don't rely on a specific health-path contract across peers? Leaning toward `GET /health` with a `HEAD /` fallback, but this can be settled during implementation based on what the `p2p-datahub-discovery` peer sample actually exposes.
- Do we want Prometheus counters in v1, or only structured logs? The codebase does not yet have a consistent metrics story — defer unless the operator feedback explicitly asks for it.
