## ADDED Requirements

### Requirement: Per-endpoint health state

The teranode client SHALL maintain per-endpoint health state for every URL registered via `NewClient` seed endpoints or `AddEndpoints`. Each entry SHALL record at minimum a `consecutiveFailures` counter and a `lastFailure` timestamp, keyed by the normalized URL form already used for deduplication.

#### Scenario: Newly registered endpoint starts healthy

- **WHEN** an endpoint is added either via `NewClient` or `AddEndpoints`
- **THEN** the client initializes its health entry with `consecutiveFailures = 0` and no `lastFailure`, and the endpoint appears in `GetHealthyEndpoints()`.

#### Scenario: Concurrent health updates

- **WHEN** two goroutines call `RecordSuccess` and `RecordFailure` for the same endpoint simultaneously
- **THEN** the operations are serialized under the client's mutex and `-race`-tagged tests pass without data races.

#### Scenario: Unknown URL is ignored

- **WHEN** `RecordSuccess` or `RecordFailure` is called with a URL that was never registered
- **THEN** the call is a silent no-op and does not create a new health entry.

### Requirement: Consecutive-failure circuit-breaker

The client SHALL transition an endpoint to the `unhealthy` state when its `consecutiveFailures` counter reaches the configured `failure_threshold` (default 3), and SHALL transition it back to `healthy` on the next `RecordSuccess`. `GetHealthyEndpoints` SHALL return only endpoints currently in the `healthy` state.

#### Scenario: Trip after repeated failures

- **WHEN** `RecordFailure` is called three times in a row for the same endpoint and `failure_threshold = 3`
- **THEN** the endpoint's state becomes `unhealthy`, it is absent from `GetHealthyEndpoints()`, and a single structured log line is emitted recording the state transition from `healthy` to `unhealthy` with the endpoint URL.

#### Scenario: Reset on success

- **WHEN** an endpoint is `unhealthy` and `RecordSuccess` is called for it
- **THEN** `consecutiveFailures` is reset to zero, the state transitions to `healthy`, the endpoint reappears in `GetHealthyEndpoints()`, and a structured log line records the state transition.

#### Scenario: Sub-threshold failures do not trip

- **WHEN** `RecordFailure` is called twice and then `RecordSuccess` is called, with `failure_threshold = 3`
- **THEN** the endpoint never leaves `healthy` and `consecutiveFailures` is zero after the success.

#### Scenario: Static and discovered endpoints tracked identically

- **WHEN** an endpoint configured via `datahub_urls` and an endpoint registered via `AddEndpoints` both exceed the failure threshold
- **THEN** both transition to `unhealthy` using the same rules and both are excluded from `GetHealthyEndpoints()`.

### Requirement: Background recovery probe

The client SHALL run a single background probe goroutine that ticks at `probe_interval_ms` (default 30000). On each tick it SHALL issue `GET <endpoint>/health` with a timeout of `probe_timeout_ms` (default 2000) to every endpoint currently in `unhealthy`. Any HTTP response received without a transport-layer error SHALL be treated as success and SHALL call `RecordSuccess` for that endpoint. A transport error or context timeout SHALL call `RecordFailure`.

#### Scenario: Unhealthy endpoint recovers

- **WHEN** an endpoint is `unhealthy` and its next probe returns any HTTP status code
- **THEN** `RecordSuccess` is called, the endpoint transitions back to `healthy`, and its next appearance in `GetHealthyEndpoints()` is reflected in subsequent broadcasts.

#### Scenario: Probe times out

- **WHEN** a probe request for an `unhealthy` endpoint exceeds `probe_timeout_ms`
- **THEN** `RecordFailure` is called once and the endpoint remains `unhealthy`.

#### Scenario: Healthy endpoints are not probed

- **WHEN** the probe ticker fires and there are no `unhealthy` endpoints
- **THEN** no HTTP requests are issued and the tick completes without work.

#### Scenario: Probe stops on client close

- **WHEN** the client's `Close` method is called
- **THEN** the probe goroutine exits within one probe interval and no further probe requests are issued.

### Requirement: Healthy-endpoint view for broadcasts

`teranode.Client` SHALL expose a `GetHealthyEndpoints() []string` method that returns a snapshot copy of all endpoints currently in the `healthy` state, in the same registration order as `GetEndpoints()`. The existing `GetEndpoints()` method SHALL be preserved and SHALL continue to return the full list regardless of health state.

#### Scenario: Healthy subset returned

- **WHEN** three endpoints are registered, one of which is `unhealthy`
- **THEN** `GetHealthyEndpoints()` returns exactly the two healthy URLs, in registration order, and `GetEndpoints()` returns all three.

#### Scenario: Snapshot independence

- **WHEN** a caller holds a slice returned by `GetHealthyEndpoints()` and an endpoint subsequently trips to `unhealthy`
- **THEN** the previously returned slice is unchanged.

#### Scenario: All endpoints unhealthy

- **WHEN** every registered endpoint is `unhealthy`
- **THEN** `GetHealthyEndpoints()` returns an empty slice and the caller is responsible for handling the empty-endpoint case.

### Requirement: Configuration

Arcade SHALL accept a `propagation.endpoint_health` config block with the following fields and defaults: `failure_threshold: 3`, `probe_interval_ms: 30000`, `probe_timeout_ms: 2000`, `min_healthy_endpoints: 0`. Zero or negative values SHALL fall back to the defaults at client construction time.

#### Scenario: Defaults applied when block omitted

- **WHEN** arcade starts with no `propagation.endpoint_health` block in the config
- **THEN** the client uses `failure_threshold = 3`, `probe_interval_ms = 30000`, `probe_timeout_ms = 2000`, and `min_healthy_endpoints = 0`.

#### Scenario: Operator override

- **WHEN** `propagation.endpoint_health.failure_threshold: 5` is set
- **THEN** an endpoint must accumulate five consecutive failures before tripping to `unhealthy`.

### Requirement: Minimum-healthy warning

When `min_healthy_endpoints > 0` and the count of endpoints in `healthy` falls below that threshold, the client SHALL emit a single `WARN`-level log line per threshold crossing. Broadcasts SHALL still proceed with whatever healthy set remains; this threshold is advisory and does not block propagation.

#### Scenario: Warning on crossing below threshold

- **WHEN** `min_healthy_endpoints = 2`, three endpoints are registered, and a second endpoint trips to `unhealthy` (leaving one healthy)
- **THEN** a single `WARN`-level log line is emitted naming the current healthy count and threshold.

#### Scenario: No warning when threshold is zero

- **WHEN** `min_healthy_endpoints = 0` and every endpoint becomes `unhealthy`
- **THEN** no threshold-crossing warning is emitted.

#### Scenario: No repeat warning when already below

- **WHEN** the healthy count is already below the threshold and a further endpoint trips to `unhealthy`
- **THEN** no additional threshold-crossing warning is emitted — the warning fires only on the crossing, not on every subsequent trip.
