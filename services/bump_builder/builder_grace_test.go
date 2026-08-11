package bump_builder

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

// This file pins the completeness-first grace-window contract:
//
//   - When merkle's expected-STUMP set (CallbackMessage.ExpectedSubtreeIndices)
//     is already fully satisfied by the stored STUMPs on arrival, the grace
//     window is SKIPPED — it exists only to ride out late STUMP retries, so a
//     verified-complete set has nothing to wait for.
//   - An absent expected set with zero stored STUMPs finalizes immediately
//     (merkle ≥ v0.4.5 omits the field exactly when the block has no tracked
//     txs, so "expect zero, have zero" is complete).
//   - Every other shape (incomplete set, or absent set with STUMPs present)
//     keeps the grace window: wait, re-read, then decide.
//
// Wall-clock bounds use a 5s configured grace vs a <2s assertion so the tests
// stay unambiguous under the race detector's slowdown.

// bumpOutcomeSampleCount returns the cumulative sample count of the
// arcade_bump_builder_build_duration_seconds child carrying the given outcome
// label, or 0 when that series does not exist yet. The metric is global to
// the process, so tests must compare before/after deltas rather than absolute
// values.
func bumpOutcomeSampleCount(t *testing.T, outcome string) uint64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != "arcade_bump_builder_build_duration_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "outcome" && lp.GetValue() == outcome {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

// TestBuilder_HandleMessage_CompleteExpectedSet_SkipsGraceWindow: merkle says
// subtree {0} should have a STUMP and it is already stored when
// BLOCK_PROCESSED arrives, so handleMessage must build immediately instead of
// burning the configured 5s grace window first. The wall-clock bound is the
// assertion: on the old wait-first ordering this test times out past 2s.
func TestBuilder_HandleMessage_CompleteExpectedSet_SkipsGraceWindow(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)

	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)
	b.cfg.BumpBuilder.GraceWindowMs = 5000

	start := time.Now()
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0})); err != nil {
		t.Fatalf("complete-set handleMessage returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Errorf("complete expected set must skip the grace window; handleMessage took %v (grace=5s)", elapsed)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored for a complete block")
	}
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("expected MarkBlockProcessed(%s) on a complete block, got %+v", blockHash, ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_AbsentExpectedSet_ZeroStumps_SkipsGraceAndFinalizes:
// no expectedSubtreeIndices field and zero stored STUMPs means "expect zero,
// have zero" — the block has no tracked txs and is complete on arrival, so it
// must finalize (stamp processed_at) without waiting the 5s grace window.
func TestBuilder_HandleMessage_AbsentExpectedSet_ZeroStumps_SkipsGraceAndFinalizes(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash

	// No datahub, teranode nil: the empty-block path must return before any
	// fetch; reaching the build path would crash the test.
	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}
	b.cfg.BumpBuilder.GraceWindowMs = 5000

	start := time.Now()
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("absent-set zero-stump handleMessage returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Errorf("absent set + zero STUMPs must finalize without the grace window; took %v (grace=5s)", elapsed)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.bumps) != 0 {
		t.Errorf("no BUMP should be built with zero STUMPs, got %d", len(ms.bumps))
	}
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("a no-tracked-tx block IS finalized — expected MarkBlockProcessed(%s), got %+v", blockHash, ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_DeferPath_StampsDeferredIncompleteOutcome pins the
// outcome-label migration on the deferral path: a block missing expected
// STUMPs after the grace window must land in the duration histogram as
// outcome="deferred_incomplete" (renamed from "incomplete_stumps").
func TestBuilder_HandleMessage_DeferPath_StampsDeferredIncompleteOutcome(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	ms.addStump(blockHash, 0, makeMinimalSTUMP(testTxidHex))

	b := &Builder{
		cfg:    &config.Config{}, // grace 0: the defer decision is immediate
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}

	before := bumpOutcomeSampleCount(t, "deferred_incomplete")
	beforeOld := bumpOutcomeSampleCount(t, "incomplete_stumps")

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0, 1})); err != nil {
		t.Fatalf("incomplete-set handleMessage must return nil, got: %v", err)
	}

	if after := bumpOutcomeSampleCount(t, "deferred_incomplete"); after != before+1 {
		t.Errorf("outcome=deferred_incomplete sample count = %d, want %d", after, before+1)
	}
	if afterOld := bumpOutcomeSampleCount(t, "incomplete_stumps"); afterOld != beforeOld {
		t.Errorf("retired outcome=incomplete_stumps must not be stamped; count went %d → %d", beforeOld, afterOld)
	}
}

// TestBuilder_HandleMessage_IncompleteExpectedSet_WaitsGraceThenDefers pins
// the fallback ordering: an expected set that is NOT yet satisfied must still
// wait the full grace window (the late-STUMP-retry defense) before deferring.
// Regression guard for the completeness-first reorder — skipping the wait
// here would defer blocks whose STUMPs were still in flight.
func TestBuilder_HandleMessage_IncompleteExpectedSet_WaitsGraceThenDefers(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	ms.addStump(blockHash, 0, makeMinimalSTUMP(testTxidHex))

	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}
	const graceMs = 150
	b.cfg.BumpBuilder.GraceWindowMs = graceMs

	start := time.Now()
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0, 1})); err != nil {
		t.Fatalf("incomplete-set handleMessage must return nil (watchdog recovers), got: %v", err)
	}
	if elapsed := time.Since(start); elapsed < graceMs*time.Millisecond {
		t.Errorf("incomplete expected set must wait the grace window before deferring; took only %v", elapsed)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.bumps) != 0 {
		t.Errorf("no BUMP should be built for an incomplete block, got %d", len(ms.bumps))
	}
	if len(ms.processedCalls) != 0 {
		t.Errorf("processed_at must NOT be stamped for an incomplete block, got %+v", ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_AbsentExpectedSet_WithStumps_WaitsGrace pins the
// ambiguous case: no expected set (pre-#162 merkle) but STUMPs exist, so
// completeness cannot be verified up-front — the grace window must still be
// waited before building.
func TestBuilder_HandleMessage_AbsentExpectedSet_WithStumps_WaitsGrace(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)

	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)
	const graceMs = 150
	b.cfg.BumpBuilder.GraceWindowMs = graceMs

	start := time.Now()
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("absent-set-with-stumps handleMessage returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < graceMs*time.Millisecond {
		t.Errorf("an unverifiable (absent) expected set must keep the grace window; took only %v", elapsed)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored after the grace window")
	}
}

// TestBuilder_HandleMessage_IncompleteThenCompleteWithinGrace_Builds covers
// the late-STUMP retry the grace window exists for, under the modern (#162)
// wire format: merkle expected {0}, the STUMP is not there on arrival but
// lands mid-window, and the post-wait re-read must pick it up and build.
func TestBuilder_HandleMessage_IncompleteThenCompleteWithinGrace_Builds(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	lateStump := makeMinimalSTUMP(txidHex)
	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: lateStump}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)
	b.cfg.BumpBuilder.GraceWindowMs = 150

	// Insert the STUMP mid-grace-window from another goroutine.
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = ms.InsertStump(context.Background(), &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: 0,
			StumpData:    lateStump,
		})
	}()

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0})); err != nil {
		t.Fatalf("expected success after the late STUMP landed in the grace window, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored after late STUMP landed in grace window")
	}
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("expected MarkBlockProcessed(%s) after the late STUMP completed the set, got %+v", blockHash, ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_OutOfOrderStump_RecoveredOnRedelivery pins the
// watchdog-shaped recovery: delivery 1 arrives before its STUMP and defers
// (no BUMP, no stamp); the STUMP lands afterwards; delivery 2 of the SAME
// message finds the expected set complete and builds + finalizes via the
// no-grace completeness path.
func TestBuilder_HandleMessage_OutOfOrderStump_RecoveredOnRedelivery(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)
	b.cfg.BumpBuilder.GraceWindowMs = 50

	msg := makeBlockProcessedMsgWithExpected(blockHash, []int{0})

	// Delivery 1: STUMP not yet stored → defer, leaving the block recoverable.
	if err := b.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("delivery 1 must return nil on deferral, got: %v", err)
	}
	ms.mu.Lock()
	if len(ms.bumps) != 0 || len(ms.processedCalls) != 0 {
		ms.mu.Unlock()
		t.Fatalf("delivery 1 must defer: bumps=%d processed=%d", len(ms.bumps), len(ms.processedCalls))
	}
	ms.mu.Unlock()

	// The out-of-order STUMP lands after the first delivery gave up.
	ms.addStump(blockHash, 0, stumpData)

	// Delivery 2 (watchdog /reprocess re-emits BLOCK_PROCESSED): the set is
	// now complete, so the block builds and finalizes.
	if err := b.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("delivery 2 must build, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored on redelivery")
	}
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("expected MarkBlockProcessed(%s) on redelivery, got %+v", blockHash, ms.processedCalls)
	}
}
