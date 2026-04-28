## Why

With P2P datahub discovery now able to add dozens of propagation URLs at runtime, a single unreachable or slow peer drags down every broadcast: the existing per-endpoint goroutine fan-out waits for **all** endpoints to return before the caller advances, so one endpoint that hangs for the full 15 s `submitCtx` timeout stalls every transaction in the batch. There is also no memory of which peers have been failing — every tx re-pays the cost of dialing the same dead URLs. We want broadcasts to complete in "fastest-healthy-peer" time and for repeatedly-failing URLs to be automatically sidelined so a sick peer does not degrade the whole service.

## What Changes

- **Introduce a per-endpoint health tracker** inside `teranode.Client` that records consecutive failures and the timestamp of the last failure for every registered URL, safe for concurrent readers and writers.
- **Add circuit-breaker state (`healthy` / `unhealthy`)** per endpoint. After `N` consecutive failures the endpoint enters `unhealthy`; a single success resets it. `GetEndpoints` continues to return the full list, but a new `GetHealthyEndpoints` returns only endpoints currently in the `healthy` state — broadcast paths switch to the healthy view.
- **Probe unhealthy endpoints on a background timer** so a peer that recovers is brought back without requiring a restart. The probe is an `HTTP GET /health` (or lightweight `HEAD /`) and is rate-limited per endpoint.
- **Refactor `broadcastSingleToEndpoints` and `broadcastBatchToEndpoints`** to operate on the healthy-endpoint view and to return as soon as the success criterion is met (any endpoint accepted), cancelling in-flight siblings instead of waiting for every endpoint to finish. Failure outcomes from individual endpoints continue to be fed into the health tracker so the circuit-breaker state stays current.
- **Record every endpoint outcome** (success or typed failure) into the health tracker from both the `/tx` and `/txs` submission paths, including batch calls from the reaper.
- **Expose observability**: structured logs and (optionally) Prometheus counters for `endpoint_success_total`, `endpoint_failure_total`, and `endpoint_state_transition_total{from,to}`, keyed by normalized URL.
- **Configuration**: new `propagation.endpoint_health` block with `failure_threshold` (default 3), `probe_interval_ms` (default 30000), `probe_timeout_ms` (default 2000), and `min_healthy_endpoints` (default 0 — soft warning only).

## Capabilities

### New Capabilities

- `datahub-endpoint-health`: Per-endpoint circuit-breaker state machine, background recovery probing, and health-filtered endpoint selection for broadcast. Defines the failure-to-unhealthy threshold, the recovery-probe cadence, and the success-clears-failure semantics.

### Modified Capabilities

- `broadcast-retry`: Broadcast fan-out is now filtered through the healthy-endpoint view and returns early on first success; unhealthy endpoints are excluded from broadcast selection until the probe loop restores them. No change to the existing retry-classification semantics (`missing inputs` / `txn-mempool-conflict` / `failed to validate` remain retryable, retry counts and backoff unchanged).
- `p2p-datahub-discovery`: Registration via `AddEndpoints` continues unchanged, but newly registered URLs are seeded into the health tracker in the `healthy` state so the circuit-breaker is immediately live for discovered peers.

## Impact

- **teranode/client.go**: `Client` gains an endpoint-health map and probe goroutine; new methods `GetHealthyEndpoints`, `RecordSuccess`, `RecordFailure`; `AddEndpoints` seeds health state; `NewClient` wires the probe loop; `Close` must now be called to stop it.
- **services/propagation/propagator.go**: `broadcastSingleToEndpoints` and `broadcastBatchToEndpoints` switch to `GetHealthyEndpoints`, use a "first success wins" early-cancel pattern, and call `RecordSuccess` / `RecordFailure` per endpoint outcome.
- **services/propagation/reaper.go**: Uses the same helpers — no special-case code for the retry path.
- **config/config.go**: New `PropagationConfig.EndpointHealth` block with four tunables and defaults documented.
- **config.example.yaml / config.example.standalone.yaml**: Document the new block.
- **cmd/arcade/main.go**: Ensure the teranode client's probe loop is started at service startup and stopped on shutdown.
- **No breaking API changes** — external HTTP contracts (`POST /tx`, `POST /txs`, callbacks) are unchanged.
- **No new external dependencies** — built on `sync`, `context`, `net/http`, and the existing `zap` logger.
