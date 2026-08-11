## MODIFIED Requirements

### Requirement: Reject unsafe URLs

The service SHALL reject any discovered URL that is structurally invalid (unparseable, non-`http`/`https` scheme, empty host, or containing userinfo), whose hostname is on the cloud-metadata deny list, whose host **fails DNS resolution or resolves to no addresses**, or any of whose resolved addresses (or IP literal) falls in a blocked range — loopback, link-local, RFC1918 private, CGNAT (100.64.0.0/10), IPv6 unique-local, unspecified, or multicast — unless the operator explicitly opts in via `p2p.allow_private_urls: true`. The opt-in SHALL NOT admit unresolvable hostnames, metadata-deny-list hostnames, or unspecified/multicast destinations. Validation SHALL resolve hostnames at most once per URL per 5-minute window (success-only cache; rejections are always re-validated).

#### Scenario: Public HTTPS URL

- **WHEN** a peer announces `https://public.example.com` and it resolves to a public address
- **THEN** the URL passes validation and is submitted for registration.

#### Scenario: Unresolvable cluster-internal hostname

- **WHEN** a peer announces `http://asset:8090/api/v1` (its own cluster-internal service name) and the hostname does not resolve from this deployment
- **THEN** the URL is rejected regardless of `allow_private_urls`, a warning including the peer ID and rejected URL is logged, the rejection is counted with outcome `invalid`, and the URL is not registered.

#### Scenario: Hostname resolving to a private address

- **WHEN** a peer announces `http://internal.corp.local` which resolves to `10.0.0.5` and `p2p.allow_private_urls` is false
- **THEN** the URL is rejected with outcome `blocked` and not registered.

#### Scenario: Private RFC1918 URL with default config

- **WHEN** a peer announces `http://192.168.5.10:8080` and `p2p.allow_private_urls` is false
- **THEN** the URL is rejected, a warning including the peer ID and the rejected URL is logged, and the URL is not registered.

#### Scenario: Loopback URL with opt-in

- **WHEN** a peer announces `http://127.0.0.1:8080` and `p2p.allow_private_urls` is true
- **THEN** the URL passes validation and is submitted for registration.

#### Scenario: Non-HTTP scheme

- **WHEN** a peer announces `ftp://peer.example/` or `file:///etc/passwd`
- **THEN** the URL is rejected and logged, regardless of `allow_private_urls`.

#### Scenario: Repeat announcement uses the validation cache

- **WHEN** a peer re-announces a URL that passed validation within the last 5 minutes
- **THEN** the URL is accepted without a new DNS resolution and the registration upsert still occurs (LastSeen advances).

### Requirement: Registry rows are re-validated at refresh time

Every pod's endpoint refresh SHALL re-validate `source=discovered` registry rows with the same URL validation (behind the same success-only cache) before registering them into the in-memory endpoint set, so rows persisted before validation existed — or whose DNS has since broken — never enter any pod's broadcast or health surface. Rows with `source=configured` SHALL be exempt: statically configured `datahub_urls` are operator-controlled and may legitimately use cluster-internal names. Rejections SHALL be counted (`arcade_teranode_endpoint_refresh_rejected_total{outcome}`) and logged at WARN once per URL per process, DEBUG thereafter.

#### Scenario: Poisoned registry row is filtered

- **WHEN** the registry contains a discovered row `http://asset:8090/api/v1` persisted by an older build and the hostname does not resolve
- **THEN** every pod's refresh skips the row, it appears in no pod's `/health` output or broadcast set, and the rejection counter increments.

#### Scenario: Configured internal URL passes through

- **WHEN** the registry contains a configured row pointing at a cluster-internal name
- **THEN** the row is registered without validation or DNS traffic.
