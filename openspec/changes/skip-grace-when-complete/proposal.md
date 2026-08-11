# Skip the bump-builder grace window when the expected-STUMP set is complete

## Why

Every BLOCK_PROCESSED pays the full `bump_builder.grace_window_ms` (default 30s, 15s in compose) before bump-builder even reads its STUMPs — a fixed MINED-latency floor on every block. The window exists solely to ride out STUMP callbacks whose first HTTP attempt got a 5xx and are retrying asynchronously (merkle's stumpGate releases BLOCK_PROCESSED after the first attempt). But since merkle PR #162, BLOCK_PROCESSED carries `expectedSubtreeIndices` — the exact set of subtree indices that produced a STUMP. When every expected index is already stored on arrival, nothing can still be in flight that we need: the wait is pure latency with zero correctness benefit. The same window also serializes recovery drains: a watchdog /reprocess storm re-delivers many blocks whose STUMPs are all present, and each one still burned the full window.

## What Changes

- **Completeness-first ordering in `handleMessage`**: STUMPs are read BEFORE the grace window. If the expected set is present and fully satisfied, build immediately (grace skipped). If the expected set is absent AND zero STUMPs are stored, finalize immediately as an empty block (absent means "expect zero"). Only an unverifiable shape — expected set unsatisfied, or absent with STUMPs present — still waits the window, re-reads, and re-runs the completeness gate.
- **Outcome label migration** on `arcade_bump_builder_build_duration_seconds{outcome}`: `success` splits into `finalized_complete_no_grace` (completeness verified pre-grace) and `grace_waited` (every other successful build); `incomplete_stumps` renames to `deferred_incomplete`. Closed set, pre-registration, doc comment, and the README alert recipe updated together.
- **No store change**: both stores' stale predicates key on `processed_at`, which the empty-block finalize already stamps; a pinning regression test guards the predicate.

## Capabilities

### Modified Capabilities

- `bump-builder`: grace-window handling becomes conditional on expected-STUMP-set completeness; empty-block finalization no longer waits the window.

## Impact

- **Latency**: blocks whose STUMPs all landed before BLOCK_PROCESSED (the healthy steady state) go MINED up to `grace_window_ms` sooner. Watchdog re-drives of complete blocks drain without the per-block wait.
- **Metric/dashboard migration (breaking for queries)**: dashboards or alerts filtering `outcome="success"` must move to `outcome=~"finalized_complete_no_grace|grace_waited"`; `incomplete_stumps` → `deferred_incomplete`. The README failure-regex recipe is updated in the same commit.
- **Compatibility floor**: the absent-expected-set + zero-STUMPs case now finalizes immediately instead of waiting the window first. This is safe against merkle-service ≥ v0.4.5 (which always emits `expectedSubtreeIndices` when the block has tracked txs, so "absent" provably means "expect zero"). Against an older merkle, a block whose only STUMPs were still retrying at BLOCK_PROCESSED time would finalize empty; pre-#162 merkle deployments should not run with this arcade version.
- **Config**: none. `bump_builder.grace_window_ms` keeps its meaning as the incomplete-set fallback window.
