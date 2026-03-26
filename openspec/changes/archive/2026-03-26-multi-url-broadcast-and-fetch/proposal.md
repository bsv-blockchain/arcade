## Why

When multiple `datahub_urls` are configured, Arcade currently races endpoints and uses only the first successful response. This means transactions may only reach one node, reducing propagation reliability. Similarly, coinbase merkle proof fetching stops at the first URL that responds, even if it fails — missing working alternatives. We need to broadcast to ALL URLs for maximum propagation, while fetching proofs can stop at the first success.

## What Changes

- **Transaction broadcast (single tx)**: Submit to ALL configured DataHub endpoints concurrently. Collect all results; return the best status (first acceptance). Do not short-circuit — ensure every endpoint receives the transaction.
- **Transaction broadcast (batch)**: Submit batch to ALL configured DataHub endpoints concurrently, same as single tx.
- **Coinbase merkle proof fetch**: Try each DataHub URL in sequence until one succeeds (current behavior is already correct — this change makes intent explicit and consistent).

## Capabilities

### New Capabilities
- `multi-url-propagation`: Broadcast transactions to all configured DataHub URLs concurrently, ensuring maximum network propagation.

### Modified Capabilities

## Impact

- `service/embedded/embedded.go`: `SubmitTransaction` and `SubmitTransactions` — change from "first success wins" to "fan-out to all, return best result"
- `teranode/client.go`: No changes needed — already accepts a single endpoint per call
- `arcade.go`: `fetchBlockDataForBUMP` — no behavioral change needed (already tries URLs in sequence until success)
- No config changes, no API changes, no breaking changes
