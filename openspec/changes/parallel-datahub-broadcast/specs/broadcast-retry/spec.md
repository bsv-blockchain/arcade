## ADDED Requirements

### Requirement: Broadcast selects only healthy endpoints

The propagation service SHALL broadcast transactions — both live and retried — only to endpoints currently reported as healthy by the teranode client. Both the single-tx path (`POST /tx`) and the batch path (`POST /txs`) SHALL obtain their endpoint list from `teranode.Client.GetHealthyEndpoints()` rather than `GetEndpoints()`.

#### Scenario: Unhealthy endpoint excluded from live broadcast

- **WHEN** a propagation batch is broadcast and one of the three configured endpoints is in the `unhealthy` state
- **THEN** the broadcast fans out to only the two healthy endpoints and no request is issued to the unhealthy URL.

#### Scenario: Unhealthy endpoint excluded from retry broadcast

- **WHEN** the reaper rebroadcasts a `PENDING_RETRY` transaction and one of the configured endpoints is `unhealthy`
- **THEN** the retry broadcast skips that endpoint and submits only to the healthy set.

#### Scenario: No healthy endpoints

- **WHEN** every configured endpoint is `unhealthy` at the moment of broadcast
- **THEN** the propagation service logs an error identifying the condition and the transaction remains in its prior status (no `REJECTED` transition from broadcast failure alone); the existing retryable-error and retry-buffer paths are unaffected.

### Requirement: First-success early-cancel

The single-tx and batch broadcast paths SHALL launch one goroutine per healthy endpoint and SHALL treat the broadcast as complete as soon as the success criterion is met by any endpoint: 200 OK for single-tx, non-error response for batch. The shared context SHALL be cancelled immediately on that first success so in-flight sibling requests to slower endpoints are terminated rather than awaited.

#### Scenario: Fastest healthy endpoint wins

- **WHEN** three endpoints are broadcast to in parallel and endpoint A returns 200 in 50 ms while B and C are still in flight at that moment
- **THEN** the propagator records `RecordSuccess` for A, cancels B and C, and returns without waiting for B or C to complete.

#### Scenario: Cancellation does not count as failure

- **WHEN** endpoint A wins the race and endpoints B and C observe `ctx.Err() != nil` on their in-flight request
- **THEN** no `RecordFailure` is called for B or C for that cancellation — their health state is unchanged by the winning race.

#### Scenario: All endpoints fail

- **WHEN** every healthy endpoint fails for a given broadcast — either with a transport error or a non-success HTTP status
- **THEN** the broadcast waits for all endpoints to report, the aggregated outcome follows the existing retryable/permanent tx-level classification rules, and reachability-based health recording is applied per the endpoint-health requirement below.

### Requirement: Reachability-based endpoint health recording

Every HTTP submission from the propagation service to a teranode endpoint — single-tx, batch, and reaper-driven retries — SHALL record a per-endpoint outcome into `teranode.Client` based on whether the peer responded, NOT on whether it accepted the particular payload. Specifically: a transport-level failure (no HTTP response received, context deadline exceeded other than early-cancel) SHALL call `RecordFailure`; any HTTP response — including 4xx and 5xx — SHALL call `RecordSuccess`, because the peer is alive and the non-2xx is a legitimate rejection of that payload, not a peer health signal. Tx-level status classification (accepted / rejected / retryable) is unchanged and continues to key off the HTTP status code and error body.

#### Scenario: 200 counts as success for health

- **WHEN** endpoint A returns 200 to a single-tx submission
- **THEN** `RecordSuccess` is called once for A.

#### Scenario: Transport error counts as failure

- **WHEN** endpoint B is unreachable and the HTTP client returns an error with no status code (connection refused, DNS failure, timeout before any bytes are received)
- **THEN** `RecordFailure` is called once for B.

#### Scenario: 500 response does not trip the circuit-breaker

- **WHEN** endpoint C returns HTTP 500 to a batch submission (e.g. Teranode's "missing inputs for tx" rejection)
- **THEN** `RecordSuccess` is called for C (peer is reachable) and the body is separately classified via the existing retryable-error logic for tx-level handling; repeated 500s from the same peer SHALL NOT accumulate toward the circuit-breaker's failure threshold.

#### Scenario: 4xx response does not trip the circuit-breaker

- **WHEN** endpoint D returns HTTP 400 (oversized batch, malformed body, etc.) to a submission
- **THEN** `RecordSuccess` is called for D; the 4xx is a peer-working signal at the health layer, even though the tx-level outcome is a failure.

#### Scenario: 202 counts as success for health

- **WHEN** endpoint E returns 202 Accepted for a single-tx submission
- **THEN** `RecordSuccess` is called for E even though the transaction status is not updated.
