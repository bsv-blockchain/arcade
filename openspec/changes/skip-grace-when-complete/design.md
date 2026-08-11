# Design — completeness-first grace-window handling

## Context

`services/bump_builder/builder.go:handleMessage` used to run: short-circuit → **wait grace window** → read STUMPs → completeness gate → build/finalize. The grace window (`bump_builder.grace_window_ms`, default 30s) defends against STUMP callbacks whose first HTTP attempt 5xx'd and are retrying asynchronously — merkle's stumpGate only waits for the first attempt of each STUMP before releasing BLOCK_PROCESSED.

Since merkle PR #162, the callback carries `expectedSubtreeIndices` (`models.CallbackMessage.ExpectedSubtreeIndices`), giving arcade a proof of completeness that makes the unconditional wait unnecessary in the common case.

## Decision 1: read-before-grace, three-way dispatch

`handleMessage` now resolves its STUMP set via `resolveStumps` (extracted, like `tryShortCircuit`, to keep nesting under the lint bar):

1. **Complete** (`len(expected) > 0 && missing == ∅` on the first read): skip the window, build from the first read. Disposition `finalized_complete_no_grace`. INFO log `expected STUMP set complete — skipping grace window` with `{expected_stumps, received_stumps}`.
2. **Absent + zero** (`len(expected) == 0 && len(stumps) == 0`): finalize immediately as an empty block (WARN unchanged, `EmptyStumpBlocksTotal`, `markBlockProcessed(hash, 0)`), outcome `no_stumps` — no grace wait first. Absent means "expect zero": merkle ≥ v0.4.5 always emits the field when the block has tracked txs (compat floor documented in proposal.md).
3. **Everything else** (expected set unsatisfied, or absent with STUMPs present — pre-#162 merkle, unverifiable): WARN when the set is absent, then wait the window (ctx-cancellable, `GraceWaitTotal.Inc()`), re-read, and re-run the completeness gate. Missing after the wait → defer (outcome `deferred_incomplete`, `IncompleteStumpsTotal`); zero after the wait → empty-block finalize; else build with disposition `grace_waited`. With `grace_window_ms: 0` there is no second store read — the first read's set is reused.

`BumpBuilderStumpCount` is observed exactly once per message, on the STUMP set that reaches the decision (first read when the wait is skipped, re-read otherwise).

## Decision 2: watchdog interplay — no store change

Both stale predicates key on `processed_at` being unset (`store/postgres/postgres.go` `ListStaleBlockProcessingStatus`: `processed_at IS NULL AND status='active' AND header_seen_at < $1 AND block_height >= $2`; aerospike mirrors the semantics with an in-memory filter). The empty-block finalize stamps `processed_at` via `MarkBlockProcessed(hash, 0, now)`, whose upsert only writes `processed_at` on conflict — the chaintracks-supplied height survives. So finalized empty blocks already leave the stale scan in both stores; calling `MarkBlockBUMPBuilt` for a block with no BUMP row would lie about BUMP existence for zero benefit, and a SQL carve-out is unnecessary. The smallest correct change is **zero lines of store code plus one pinning test** (`TestBlockProcessing_ListStale_ExcludesFinalizedEmptyBlock`) so a future predicate change (e.g. adding `bump_built_at IS NULL`) can't silently resurrect the empty-block re-drive storm.

## Decision 3: outcome label migration

`arcade_bump_builder_build_duration_seconds{outcome}` has a closed, pre-registered label set (PR #244: `metrics/preregister.go` `bumpBuildOutcomes`, doc-comment contract on the metric, positively-enumerated failure regex in `metrics/README.md`). A flat `success` can no longer say whether the window was paid, and keeping it while adding a sibling would make `outcome="success"` silently undercount. So:

| Old | New | Class |
|---|---|---|
| `success` | `finalized_complete_no_grace` \| `grace_waited` | benign |
| `incomplete_stumps` | `deferred_incomplete` | failure |
| all others | unchanged | as today |

All four touch-points move in one commit: the closed set, the metric doc comment, the README recipe, and the pre-registration tests. Dashboards filtering `outcome="success"` migrate to `outcome=~"finalized_complete_no_grace|grace_waited"`.

## Risks

- **Pre-#162 merkle**: absent+zero now finalizes without the wait; a genuinely-late STUMP from an old merkle would be orphaned (block already stamped). Accepted: compat floor is merkle ≥ v0.4.5, and the previous behavior only narrowed the race rather than closing it.
- **Second store read per grace-waited message**: one extra `GetStumpsByBlockHash` for blocks that actually wait; complete blocks now do exactly one read (same count as before, minus the 30s).
