# Merkle lifecycle durability

Stage-2 must not enable Merkle callbacks until a store failure can be retried
and a failed mine cannot look finished. This is the contract on
`fix/merkle-lifecycle-durable-callbacks`.

## SEEN callback persistence

`SEEN_ON_NETWORK` and `SEEN_MULTIPLE_NODES` are applied in
`services/api_server/handlers.go` (`applySeenCallback`).

A failed `BatchUpdateStatusReturning` returns **HTTP 500** with
`failed to store seen status`. Merkle retries non-2xx. The failure is logged
as `batch update seen status failed` and counted on
`CallbackHandlerDuration` with `outcome="error"`.

A successful callback stays **HTTP 200**. Delivering the same SEEN callback
twice is idempotent: the status lattice does not regress the row, and the
transition is published once.

Unknown txids stay **HTTP 200** and do not create a row.

## Unknown callback types

An unrecognized `type` is acknowledged with **HTTP 200** and a warning.
Nothing is written. A 5xx would retry a message this build cannot apply. A
4xx can permanently reject the delivery.

## MINED persistence

`SetMinedByTxIDs` failure in `services/bump_builder/builder.go` is not a
finished block.

- `processed_at` stays unset.
- `ListStaleBlockProcessingStatus` selects `processed_at IS NULL`, so the
  block stays eligible for the watchdog.
- The compound BUMP is already stored. The watchdog re-drive takes
  `tryShortCircuit` and mines again. Rows already MINED are a no-op.
- `handleMessage` returns nil. The Kafka consumer commits the offset on a
  nil error. Kafka redelivery is not the recovery mechanism. Returning the
  store error would retry and then dead-letter a message that cannot finish
  until the watchdog runs.

The watchdog is the recovery mechanism. Do not remove it.

## Zero-STUMP finalization

`ZERO_STUMP_FINALIZATION: SAFE`

`BLOCK_PROCESSED` with zero stored STUMPs and no `expectedSubtreeIndices`
calls `finalizeEmptyBlock` and stamps `processed_at` (height 0, which does
not replace a chaintracks height). That block is not re-driven. Current
Merkle omits the expected set only when the block has no tracked
transactions, so finalizing it is the empty-block contract. Leaving
`processed_at` unset would make the watchdog re-drive every empty block.

When `expectedSubtreeIndices` is present and a STUMP is missing, the block
is not finalized. `processed_at` stays unset and the watchdog re-drives it
via `/reprocess`.

## Follow-up

Before enabling Merkle, confirm that deployment's Merkle sends
`expectedSubtreeIndices` for every block that contains a tracked subtree.
If that field is omitted, a dropped STUMP set is indistinguishable from an
empty block and `finalizeEmptyBlock` will stamp `processed_at`. This sprint
does not change that contract.

No production config was changed. Merkle stays disabled until that check
and the rest of Stage-2 are done outside this branch.
