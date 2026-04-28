## ADDED Requirements

### Requirement: Discovered URLs seeded into health tracker

When the p2p_client service registers a newly discovered URL via `teranode.Client.AddEndpoints`, the teranode client SHALL also seed that URL's entry in the endpoint-health tracker in the `healthy` state. A discovered URL SHALL be eligible for inclusion in `GetHealthyEndpoints()` immediately after registration, without waiting for a probe tick.

#### Scenario: Newly announced peer used on next broadcast

- **WHEN** a peer announces `https://new.peer.example`, the URL passes validation, and `AddEndpoints` registers it
- **THEN** the next broadcast that reads `GetHealthyEndpoints()` includes `https://new.peer.example`.

#### Scenario: Rediscovered previously-unhealthy URL

- **WHEN** a URL previously marked `unhealthy` (and still present in the endpoint list) is re-announced by a peer
- **THEN** the existing health entry is preserved — re-announcement does not reset the failure counter or state. The deduplication rule in `AddEndpoints` ensures no duplicate entry is created.

#### Scenario: Discovered URL respects failure threshold

- **WHEN** a discovered URL accumulates `failure_threshold` consecutive failures on subsequent broadcasts
- **THEN** it transitions to `unhealthy` and is excluded from `GetHealthyEndpoints()` using the same rules as statically configured URLs.

### Requirement: Normalize `ttn` network alias to `teratestnet`

The p2p_client service SHALL translate `p2p.network: "ttn"` into `"teratestnet"` before handing it to `go-teranode-p2p-client`. This compensates for the library's missing alias entry (as of v0.2.2, `getNetworkToTopic()` recognises `teratest` and `teratestnet` but not `ttn`) so operators can use the short form without silently subscribing to the wrong pubsub topic. All other network values — `main`, `mainnet`, `test`, `testnet`, `stn`, `teratestnet`, and unknown values — SHALL be passed through verbatim.

#### Scenario: ttn is translated

- **WHEN** arcade starts with `p2p.network: "ttn"` and `p2p.datahub_discovery: true` (with operator-supplied `bootstrap_peers`)
- **THEN** the `p2pclient.Config` handed to the library has `Network: "teratestnet"`, so the resulting subscription is `teranode/bitcoin/1.0.0/teratestnet-node_status` — the same topic teratestnet peers publish on.

#### Scenario: Known networks pass through unchanged

- **WHEN** arcade starts with `p2p.network: "stn"` (or `main`, `test`, `teratestnet`, empty)
- **THEN** the `p2pclient.Config.Network` equals the configured value verbatim; no translation is applied.

#### Scenario: Startup log records both configured and topic network

- **WHEN** p2p discovery starts
- **THEN** the `"p2p discovery enabled"` log line includes both the `network` (as configured) and the `topic_network` (post-normalization) fields so operators can see when translation occurred.
