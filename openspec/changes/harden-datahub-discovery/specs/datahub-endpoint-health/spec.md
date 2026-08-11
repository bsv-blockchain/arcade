## MODIFIED Requirements

### Requirement: Background probing covers every endpoint

The teranode client's background probe loop SHALL probe **every** registered endpoint on each probe interval — healthy endpoints included — so that a pod with no broadcast traffic (api-server, sse, watchdog) still demotes unreachable endpoints and its health surface reflects reality. Probe outcomes SHALL be recorded on a dedicated consecutive-probe-failure counter that trips the endpoint to `unhealthy` at the same threshold as the fast track (`failure_threshold`, default 3).

A probe success against a **healthy** endpoint SHALL reset only the probe-failure counter and SHALL NOT reset either broadcast-path counter (`consecutiveFailures`, `consecutiveBroadcastFailures`) — a reachable `/health` says nothing about whether broadcasts succeed, and resetting them would prevent the slow-track breaker from ever tripping while probes run. A probe success against an **unhealthy** endpoint SHALL perform full recovery exactly as before: all counters reset, state returns to healthy (any HTTP response, including 4xx/5xx, counts as reachable).

#### Scenario: Dead endpoint demoted without broadcast traffic

- **WHEN** a registered endpoint stops accepting connections and the pod issues no broadcasts
- **THEN** after `failure_threshold` consecutive failed probes the endpoint transitions to `unhealthy` (logged with reason `probe`), its health gauge drops to 0, and `GetEndpointStatuses` reports `healthy: false`.

#### Scenario: Probe successes do not mask broadcast failures

- **WHEN** an endpoint's `/health` responds 200 to every probe while broadcasts to it consistently return non-2xx
- **THEN** `consecutiveBroadcastFailures` accumulates across broadcasts uninterrupted by probe successes and the endpoint trips at `broadcast_failure_threshold` as if probing were disabled.

#### Scenario: Recovery contract unchanged

- **WHEN** an unhealthy endpoint answers a probe with any HTTP response (including 404)
- **THEN** the endpoint returns to `healthy` with all failure counters reset.
