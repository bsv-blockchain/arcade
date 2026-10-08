# Merkle lifecycle durability

This is the Stage-2 lifecycle durability contract. Stage-2 must not enable
Merkle callbacks until a store failure can be retried and a failed mine
cannot look finished.

## SEEN callback persistence

`SEEN_ON_NETWORK` and `SEEN_MULTIPLE_NODES` are applied in
`services/api_server/handlers.go` (`applySeenCallback`).

A failed `BatchUpdateStatusReturning` returns **HTTP 500** with
`failed to store seen status`. Merkle retries non-2xx. The failure is logged
as `batch update seen status failed` and counted on
`CallbackHandlerDuration` with `outcome="error"`.

Postgres, Aerospike, Pebble, and MongoDB report `Prev` only when that call
durably applied the requested transition. A known lattice skip, including
the race where a callback read `ACCEPTED_BY_NETWORK` and a concurrent writer
had already moved the tx to `MINED`, is `Current`: the row is known, SEEN is
not published, `txTracker` is not moved backwards, and
`CallbackUnknownTxIDTotal` does not increment. That counter is only for a
txid with no row. A failed write still does not advance `txTracker`, so the
HTTP 500 retry is not filtered out and reaches the store again. Rows in the
same batch that did persist may still be published.

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
- The build-duration outcome is `store_failed` on both the fresh build and
  the short-circuit redelivery. It is not `finalized_complete_no_grace`,
  `grace_waited`, or `short_circuited`.

The watchdog is the recovery mechanism. Do not remove it.

## Zero-STUMP finalization

`BLOCK_PROCESSED` with zero stored STUMPs and no `expectedSubtreeIndices`
calls `finalizeEmptyBlock` and stamps `processed_at` (height 0, which does
not replace a chaintracks height). That block is not re-driven. Current
Merkle omits the expected set only when the block has no tracked
transactions, so finalizing it is the empty-block contract. Leaving
`processed_at` unset would make the watchdog re-drive every empty block.

When `expectedSubtreeIndices` is present and a STUMP is missing, the block
is not finalized. `processed_at` stays unset and the watchdog re-drives it
via `/reprocess`.

## Enablement precondition

Merkle must send `expectedSubtreeIndices` for every block that contains a
tracked subtree. If that field is omitted, a dropped STUMP set is
indistinguishable from an empty block and `finalizeEmptyBlock` stamps
`processed_at`. Confirm this for a deployment's Merkle before enabling
callbacks.

## Multiple merkle-services

`merkle_service.urls` lists additional endpoints. The `merkleservice.Pool`
fans every `/watch` and `/reprocess` out to all of them, so each service
watches the same txids and each delivers its own callbacks.

Registration succeeds when **any** endpoint accepts; only a tx refused or
unreachable on every endpoint is requeued (F-024 still gates broadcast). When
every endpoint failed, the surfaced error is the most retryable one (context
error, then network/5xx, then 401/403, then other 4xx), so `auth_error` on the
registration metrics now means every endpoint rejected the token.

An endpoint that fails three consecutive transport/5xx requests has its
breaker opened and is skipped; 4xx answers never trip it. The pool probes
`GET /health` on open endpoints every 5s and closes the breaker on any non-5xx
answer. Closing fires the propagation recovery replay: every non-terminal tx
whose row moved since the breaker opened (minus 5 minutes of slack) is
re-registered with that endpoint only, ignoring `merkle_registered_at`
(the stamp is pool-wide). `/watch` is idempotent on merkle-service.

Duplicate callbacks:

- `SEEN_ON_NETWORK` / `SEEN_MULTIPLE_NODES` from a second service hit the
  tracker prefilter or the lattice re-assert path and are not published. A
  forward move (`SEEN_ON_NETWORK` → `SEEN_MULTIPLE_NODES`) from whichever
  service reports it first is applied and published once; a late
  `SEEN_ON_NETWORK` after that is a lattice skip.
- `STUMP` for the same `(block, subtree)` is an upsert of identical bytes.
- `BLOCK_PROCESSED` from a second service takes `tryShortCircuit`, which
  re-mines the stored BUMP's level-0 set with `onlyChanged=true`: rows already
  `MINED` against this block are not re-published, a tx registered after the
  first build still publishes. `processed_at` is re-stamped. The build
  duration histogram records `short_circuited` once per extra service.

Known limitation in this stage: a service that was down while txs were
registered (and therefore only knows a subset) emits STUMPs that lack those
txs' paths. Because STUMPs are keyed by `(block, subtree)` and overwritten on
insert, which variant the first build sees depends on arrival order; the
content-addressed STUMP handling that closes this lands in the follow-up
change.
