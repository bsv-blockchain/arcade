## Context

Arcade broadcasts transactions to Teranode DataHub endpoints and fetches coinbase merkle proofs from them. The config supports multiple `datahub_urls`, but the current broadcast logic in `embedded.go` fans out to all endpoints and returns after the **first** successful response, cancelling the rest. This means some endpoints may never receive the transaction.

The coinbase/block-data fetch in `arcade.go:fetchBlockDataForBUMP` already iterates URLs sequentially and returns on first success — this is correct behavior for fetches.

## Goals / Non-Goals

**Goals:**
- Ensure every configured DataHub endpoint receives every broadcast transaction (single and batch)
- Return the best status to the caller without waiting for all endpoints to finish
- Keep coinbase merkle proof fetching as try-until-first-success (unchanged)

**Non-Goals:**
- Retry logic for failed broadcasts (out of scope)
- Per-endpoint health tracking or circuit breaking
- Changing the teranode client interface

## Decisions

### 1. Fan-out broadcast, wait for all endpoints

**Decision**: Submit to all endpoints concurrently. Collect results from all. Return the best status (prefer ACCEPTED > SENT > REJECTED). Do not cancel other goroutines when the first one succeeds.

**Rationale**: The purpose of multiple URLs is redundancy and propagation reach. Cancelling after first success defeats this. The 15-second timeout already bounds total wait time.

**Alternative considered**: Fire-and-forget for non-first endpoints. Rejected because we lose visibility into failures and can't aggregate the best status.

### 2. Status aggregation: best-status-wins

**Decision**: For single-tx submit, collect all endpoint responses and return the most favorable status. Priority: `ACCEPTED_BY_NETWORK` > `SENT_TO_NETWORK` > `REJECTED`. If any endpoint accepts, the tx is accepted.

**Rationale**: Different endpoints may respond differently (one rejects, another accepts). The most favorable status is the correct one to report since the tx did reach the network.

### 3. No changes to fetch behavior

**Decision**: `fetchBlockDataForBUMP` keeps its current sequential try-until-success pattern. This is already the requested behavior for merkle proof fetching.

## Risks / Trade-offs

- **[Slightly higher latency]** → Waiting for all endpoints instead of first-success adds latency. Mitigated by the existing 15-second timeout — we still return the best result seen so far when timeout fires.
- **[More network traffic]** → Every tx hits every endpoint. This is intentional — the whole point is maximum propagation.
- **[Mixed status responses]** → One endpoint accepts, another rejects. Mitigated by best-status-wins aggregation.
