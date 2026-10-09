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

A tx the pool reported registered must still reach every endpoint, or the
endpoints' watch sets diverge and their STUMPs disagree. Every endpoint-
specific failure behind a pool-level success — a refused or timed-out
`/watch`, or an endpoint skipped because its breaker was open — is kept in
that endpoint's in-memory catch-up queue (`arcade_merkle_endpoint_catchup_pending`)
and re-sent from the pool's background loop once the endpoint answers again,
500 per 5s tick with backoff; what fails again goes back to the queue. The
queue is bounded (200k entries per endpoint, about half an hour at 100 TPS);
past that the oldest entries are dropped and, once the queue drains, the
pool asks propagation to resync that endpoint with a full lookback replay
(`register_replay_lookback_hours`, ignoring `merkle_registered_at`, since
the stamp is pool-wide). A `/watch` that fails during the resync goes back
to the queue. `/watch` is idempotent on merkle-service. The startup replay
re-registers every in-flight tx with the whole pool, so a restart loses
nothing durable.

An endpoint that fails three consecutive transport/5xx requests has its
breaker opened and is skipped; 4xx answers never trip it. The pool probes
`GET /health` on open endpoints every 5s and closes the breaker on any non-5xx
answer; the catch-up queue then drains.

Duplicate callbacks:

- `SEEN_ON_NETWORK` / `SEEN_MULTIPLE_NODES` from a second service hit the
  tracker prefilter or the lattice re-assert path and are not published. A
  forward move (`SEEN_ON_NETWORK` → `SEEN_MULTIPLE_NODES`) from whichever
  service reports it first is applied and published once; a late
  `SEEN_ON_NETWORK` after that is a lattice skip.
- `STUMP` rows are content-addressed: the key is
  `(block_hash, subtree_index, sha256(stump_data))`. Identical bytes from a
  second service are a no-op; a different STUMP for the same subtree is a
  second row. The difference is real: each merkle-service builds its STUMP
  from the txids *it* has registrations for, so a service that was down while
  txs were registered delivers a STUMP that lacks those txs' paths.
  `bump.BuildCompoundBUMP` merges same-subtree variants by `(level, offset)`
  union (hashes must agree; the tracked marker is OR-ed).
- `BLOCK_PROCESSED` is processed every time. When a compound BUMP already
  exists, the builder reads the retained STUMP set and diffs its level-0
  hashes against the stored BUMP:
  - nothing new ⇒ short-circuit: re-mine the stored level-0 set with
    `onlyChanged=true` (rows already `MINED` against this block are not
    re-published; a tx registered after the first build still publishes),
    re-stamp `processed_at`. Outcome `short_circuited`, once per extra
    service.
  - new leaves ⇒ rebuild from the full STUMP set, overwrite the BUMP, mine the
    union with `onlyChanged=true` so only the newly covered txs are
    published. Outcome `rebuilt`. A rebuild always waits the grace window
    first: the expected set names subtree indices, and an index covered by
    the earlier service's variant says nothing about whether the later
    service's own STUMP for it has landed. The rebuilt compound must cover
    every leaf of the stored one; if it would not (only possible if the
    janitor pruned the earlier STUMPs), the stored BUMP is kept and the block
    is deferred.

  Residual window: a later service's STUMP that straggles past the grace
  window for an index the earlier service already covered is not detected
  (the index looks satisfied and no further `BLOCK_PROCESSED` follows). It
  needs both a partial registration and a STUMP retry slower than
  `grace_window_ms`; `/reprocess` on the block recovers it.

## STUMP retention and deferral with several sources

STUMPs are no longer pruned right after a build. The janitor
(`pruneOrphanStumps`, run at startup and every quarter of the window) deletes
a block's STUMP rows once its `bump_built_at` is older than
`bump_builder.stump_retention_minutes` (default 60). Anchor-denied and
reconciler cleanups still delete immediately.

`processed_at` is still the watchdog's signal. The first service's build
stamps it. If a later service's `BLOCK_PROCESSED` then defers the block (its
expected STUMP set is still missing after the grace window, or a rebuild would
drop leaves), `deferStampedBlock` clears the stamp with the new store method
`ClearBlockProcessed`, so `ListStaleBlockProcessingStatus` surfaces the block
and the watchdog's `/reprocess` (fanned out to every merkle-service) re-drives
it. `ClearBlockProcessed` only unsets `processed_at` and never creates a row.
