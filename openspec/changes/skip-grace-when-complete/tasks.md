## 1. Tests first (RED against the wait-first ordering)

- [x] 1.1 `TestBuilder_HandleMessage_CompleteExpectedSet_SkipsGraceWindow` — grace=5s, expected `{0}` satisfied pre-arrival; asserts handleMessage returns in <2s, BUMP stored, `MarkBlockProcessed` called. RED by timing on the old ordering.
- [x] 1.2 `TestBuilder_HandleMessage_AbsentExpectedSet_ZeroStumps_SkipsGraceAndFinalizes` — grace=5s, field absent, no STUMPs; <2s, stamped, no BUMP. RED by timing.
- [x] 1.3 `TestBumpOutcomesIncludeGraceDispositions` (metrics) — closed set carries `finalized_complete_no_grace`/`grace_waited`/`deferred_incomplete`, not `success`/`incomplete_stumps`. RED.
- [x] 1.4 `TestBuilder_HandleMessage_DeferPath_StampsDeferredIncompleteOutcome` — defer path observed under `deferred_incomplete` on the duration histogram. RED.

## 2. Regression pins (green before and after — guard the reorder)

- [x] 2.1 `TestBuilder_HandleMessage_IncompleteExpectedSet_WaitsGraceThenDefers` — elapsed ≥ grace, no BUMP, no stamp.
- [x] 2.2 `TestBuilder_HandleMessage_AbsentExpectedSet_WithStumps_WaitsGrace` — ambiguous set keeps the window; BUMP builds after it.
- [x] 2.3 `TestBuilder_HandleMessage_IncompleteThenCompleteWithinGrace_Builds` — late STUMP lands mid-window; post-wait re-read builds.
- [x] 2.4 `TestBuilder_HandleMessage_OutOfOrderStump_RecoveredOnRedelivery` — delivery 1 defers; STUMP lands; delivery 2 builds + stamps.
- [x] 2.5 `TestBlockProcessing_ListStale_ExcludesFinalizedEmptyBlock` (store/postgres) — `MarkBlockProcessed(hash, 0, now)` removes the row from the stale scan and preserves the chaintracks height.
- [x] 2.6 Existing builder suite kept green; `TestBuilder_LateSTUMP_ArrivesDuringGraceWindow` rewritten as `TestBuilder_LateSTUMP_AbsentExpectedSet_FinalizesWithoutWaiting` (its old expectation — absent+zero waits for a late STUMP — is exactly the behavior this change retires; mid-grace-arrival coverage moved to 2.3).

## 3. Builder implementation

- [x] 3.1 Extract `resolveStumps` (read → three-way dispatch → optional wait + re-read → completeness gate) returning `(stumps, disposition, done, err)`; `handleMessage` stamps `outcome` from the disposition on every terminal path.
- [x] 3.2 Extract `finalizeEmptyBlock` (WARN text unchanged, `EmptyStumpBlocksTotal`, `markBlockProcessed(hash, 0)`) shared by the absent+zero and post-wait-zero paths.
- [x] 3.3 `BumpBuilderStumpCount` observed once per message on the decision set; `GraceWaitTotal` only increments when the wait actually happens.

## 4. Metric label migration (all four touch-points in sync)

- [x] 4.1 `metrics/preregister.go` — `bumpBuildOutcomes` swaps `success` → `finalized_complete_no_grace` + `grace_waited`, `incomplete_stumps` → `deferred_incomplete`.
- [x] 4.2 `metrics/metrics.go` — `BumpBuilderBuildDuration` doc contract, `BumpBuilderGraceWaitTotal` help/comment ("before re-reading STUMPs"), `BumpBuilderIncompleteStumpsTotal` rename mention.
- [x] 4.3 `metrics/README.md` — failure regex and benign list updated; empty-stump recipe reads against the two build labels.

## 5. Gates

- [x] 5.1 `go build ./...`
- [x] 5.2 `CGO_ENABLED=1 go test ./... -count=1`
- [x] 5.3 `go test -race -count=1 ./services/bump_builder ./metrics ./store/postgres ./services/watchdog` (postgres also run with `-tags=postgres`)
- [x] 5.4 `golangci-lint run`
