## Context

When Merkle Service is enabled, transaction mining proof follows a two-phase callback flow:

1. **STUMP** callback: Received per-transaction with the subtree merkle path data. Stored in the `stumps` table.
2. **BLOCK_PROCESSED** callback: Received once per block after all STUMPs are delivered. Triggers `ConstructBUMPsForBlock` which assembles full BUMPs and then calls `SetMinedByBlockHash` to mark transactions as MINED.

Currently, the `handleStump` handler only stores the STUMP data. The transaction status remains unchanged until the BLOCK_PROCESSED callback triggers BUMP construction. This creates a delay between Merkle Service confirming a transaction is mined and Arcade reflecting that status.

## Goals / Non-Goals

**Goals:**
- Update transaction status to MINED immediately when a STUMP is received
- Publish status events so downstream subscribers are notified promptly
- Maintain compatibility with the existing BUMP construction flow

**Non-Goals:**
- Changing the BUMP construction flow (still triggered by BLOCK_PROCESSED)
- Modifying the store layer or adding new store methods
- Changing status transition rules

## Decisions

**Update status in `handleStump`**: Add `UpdateStatus` and event publishing calls to the existing `handleStump` method in `routes/fiber/callback.go`. This mirrors the pattern already used by `handleSeenOnNetwork`.

*Alternative considered*: Updating status during BUMP construction only. Rejected because the STUMP already confirms the transaction is in a block, so waiting for BLOCK_PROCESSED adds unnecessary latency.

**Use existing `UpdateStatus` method**: The store's `UpdateStatus` already handles status transition guards via `DisallowedPreviousStatuses`. Since MINED has no disallowed previous statuses, this is safe to call at STUMP time. When `SetMinedByBlockHash` runs later during BUMP construction, transactions already marked MINED will be unaffected.

## Risks / Trade-offs

[STUMP arrives for unknown transaction] → `UpdateStatus` will fail gracefully (transaction not in DB). The existing warn logging in `handleStump` covers error cases.

[Duplicate MINED events] → `SetMinedByBlockHash` in `ConstructBUMPsForBlock` may produce a second MINED status event for transactions already marked MINED by the STUMP handler. This is acceptable as downstream consumers should be idempotent, but could be optimized later if needed.
