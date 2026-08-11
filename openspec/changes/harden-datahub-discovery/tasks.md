# Tasks

## 1. ssrfguard package

- [x] 1.1 Port `ssrfguard` from merkle-service as a top-level arcade package: `ValidateURL`, `IsBlockedIP`, `IsBlockedHostname`, `CheckDialAddress`, `ErrInvalidURL`/`ErrBlockedAddress` sentinels.
- [x] 1.2 Fix the CGNAT (100.64.0.0/10) doc/code mismatch from the merkle copy: add the explicit range check.
- [x] 1.3 Port the test table; add CGNAT boundary cases.

## 2. Discovery-time validation (services/p2p_client)

- [x] 2.1 `validateURL` delegates to `ssrfguard.ValidateURL` with an injectable lookup; delete `isPrivateHost`.
- [x] 2.2 Add `lookupIP` seam (default `net.DefaultResolver.LookupIP`, 2s timeout) and a success-only 5-minute TTL cache on `Client`.
- [x] 2.3 Split the rejection metric outcome: `blocked` (SSRF policy) vs `invalid` (malformed/unresolvable) on `arcade_p2p_endpoint_discovery_total`.
- [x] 2.4 Tests: flip the "dns name resolving privately is treated as public" case; unresolvable-hostname (`asset:8090`) rejection; cache skips repeat lookups; upsert-per-announcement preserved.

## 3. Refresh-time filter (app.endpointSource)

- [x] 3.1 `newEndpointSource` with `datahubLister` seam; discovered rows validated behind the TTL cache, configured rows exempt.
- [x] 3.2 WARN-once/DEBUG-thereafter log dampening per URL; un-warn on recovery.
- [x] 3.3 New metric `arcade_teranode_endpoint_refresh_rejected_total{outcome}`.
- [x] 3.4 Tests: configured bypass, unresolvable filtered, cache, rejection-not-cached, allow-private opt-in, discovery-off exclusion.

## 4. Probe-all (teranode.Client)

- [x] 4.1 Add `consecutiveProbeFailures` to `endpointHealth`; document why it is separate from the broadcast counters.
- [x] 4.2 `recordProbeFailure` (trips at `failureThreshold`, reason="probe") and `recordProbeSuccess` (full recovery when unhealthy; probe-counter-only reset when healthy).
- [x] 4.3 `probeOnce` probes all endpoints; `probeEndpoint` routes outcomes to the probe counter; `RecordSuccess` also resets the probe counter.
- [x] 4.4 Tests: unreachable healthy endpoint demoted by probes alone; probe successes do not reset either broadcast counter; probe-counter consecutive semantics; existing recovery tests unchanged.

## 5. Surface + docs

- [x] 5.1 api-server test: `/health` flips a dead endpoint to `healthy:false` with zero broadcast traffic.
- [x] 5.2 Update `allow_private_urls` comments in both example configs.
- [x] 5.3 Update the p2p-datahub-discovery spec (Reject unsafe URLs requirement) and add the probe-all requirement to datahub-endpoint-health.
