# Harden DataHub discovery: DNS-validated URLs + truthful endpoint health

## Why

The public `/health` endpoint reported `http://asset:8090/api/v1` — a peer's cluster-internal service name, unreachable from arcade — as a healthy discovered DataHub URL. Two gaps compounded: (1) discovery validation was purely syntactic, so any non-IP-literal hostname was accepted without checking that it resolves or where it resolves to, and the URL then persisted forever in the shared registry (no eviction); (2) per-endpoint circuit-breaker state only ever changed in the propagation pod, because the recovery probe targeted already-unhealthy endpoints and only broadcast outcomes recorded failures — so in the api-server pod (which serves `/health`) every endpoint stayed `healthy: true` forever. merkle-service fixed the same announcement-validation bug class in its PR #181; this change mirrors that pattern.

## What Changes

- **New `ssrfguard` package** (ported from merkle-service): URL validation with scheme allowlist, userinfo rejection, metadata-hostname deny list, blocked-IP predicate (loopback/link-local/RFC1918/CGNAT/ULA/unspecified/multicast), and — the key addition over the old validator — DNS resolution of non-literal hostnames with every resolved IP checked. Unresolvable hostnames are rejected regardless of `p2p.allow_private_urls`.
- **Discovery-time validation** (`services/p2p_client`): announcements are validated with ssrfguard behind a success-only 5-minute TTL cache (injectable DNS lookup, 2s bound); rejections are metered (`invalid` vs `blocked`) and logged with peer ID.
- **Refresh-time filter** (`app.endpointSource`): `source=discovered` registry rows are re-validated on every listing (same cache design), neutralizing rows persisted before this guard existed. Operator-configured rows are exempt. New metric `arcade_teranode_endpoint_refresh_rejected_total{outcome}`.
- **Probe-all** (`teranode.Client`): the background probe loop now probes every endpoint, not just unhealthy ones, feeding a dedicated `consecutiveProbeFailures` counter (threshold shared with the fast track). Probe success on a healthy endpoint resets only the probe counter — never the broadcast-path counters — so the slow-track breaker still trips for reachable-but-useless endpoints. Unhealthy-endpoint recovery semantics are unchanged.

## Impact

- `/health` `datahub_urls[].healthy` now reflects reality in every pod (~90s to demote a dead endpoint); unresolvable URLs disappear from the list entirely.
- `arcade_teranode_endpoint_healthy` updates in non-broadcasting pods; `min_healthy_endpoints` warnings may fire more often (truthful signal).
- No new config keys; `p2p.allow_private_urls` now also gates DNS-resolved private addresses.
- Out of scope (follow-ups): registry eviction/LastSeen pruning; dial-time `CheckDialAddress` guard on the broadcast transport (DNS-rebinding window).
