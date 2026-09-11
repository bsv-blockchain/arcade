package chaintracks_server

import (
	"context"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// trackerStore is the minimal store.Store fake the tracker touches: a
// map-backed block_processing table with the real mutation semantics of
// UpsertBlockHeaderSeen / MarkBlocksOrphaned (see the store contract), plus
// call recording so tests can assert exactly what the tracker asked for.
// The embedded nil interface panics on anything else, pinning the tracker's
// store surface.
type trackerStore struct {
	store.Store

	mu       sync.Mutex
	rows     map[string]*models.BlockProcessingStatus
	orphaned [][]string
	upserts  []string
	lists    int // ListBlockProcessingStatus calls — "did a scan run" signal
}

func newTrackerStore(rows ...*models.BlockProcessingStatus) *trackerStore {
	s := &trackerStore{rows: make(map[string]*models.BlockProcessingStatus, len(rows))}
	for _, r := range rows {
		cp := *r
		s.rows[r.BlockHash] = &cp
	}
	return s
}

func (s *trackerStore) ListBlockProcessingStatus(_ context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	// Honor the real keyset contract (height DESC, block_height < before,
	// limit) — the tie-scan pages via store.ForEachBlockProcessing, which
	// depends on it for termination. The hash tiebreak keeps page contents
	// stable across the pager's re-fetches (map iteration is random).
	sorted := make([]*models.BlockProcessingStatus, 0, len(s.rows))
	for _, row := range s.rows {
		cp := *row
		sorted = append(sorted, &cp)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].BlockHeight != sorted[j].BlockHeight {
			return sorted[i].BlockHeight > sorted[j].BlockHeight
		}
		return sorted[i].BlockHash < sorted[j].BlockHash
	})
	var out []*models.BlockProcessingStatus
	for _, row := range sorted {
		if beforeHeight > 0 && row.BlockHeight >= beforeHeight {
			continue
		}
		out = append(out, row)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *trackerStore) GetBlockProcessingStatus(_ context.Context, hash string) (*models.BlockProcessingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[hash]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *row
	return &cp, nil
}

// UpsertBlockHeaderSeen mirrors the backends' conflict path: chaintracks
// owns block_height and status, the orphan/reconcile marks clear, and the
// milestone timestamps survive.
func (s *trackerStore) UpsertBlockHeaderSeen(_ context.Context, hash string, height uint64, seen time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, hash)
	row, ok := s.rows[hash]
	if !ok {
		s.rows[hash] = &models.BlockProcessingStatus{
			BlockHash: hash, BlockHeight: height, HeaderSeenAt: seen,
			Status: models.BlockStatusActive,
		}
		return nil
	}
	row.BlockHeight = height
	row.Status = models.BlockStatusActive
	row.OrphanedAt = nil
	row.ReconciledAt = nil
	return nil
}

// MarkBlocksOrphaned records the call and, like every backend, mutates
// only rows that exist (missing hashes are silently skipped).
func (s *trackerStore) MarkBlocksOrphaned(_ context.Context, hashes []string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orphaned = append(s.orphaned, append([]string(nil), hashes...))
	for _, h := range hashes {
		if row, ok := s.rows[h]; ok {
			ts := at
			row.Status = models.BlockStatusOrphaned
			row.OrphanedAt = &ts
		}
	}
	return nil
}

func (s *trackerStore) orphanCalls() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.orphaned...)
}

func (s *trackerStore) upsertCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.upserts...)
}

func (s *trackerStore) listCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists
}

func (s *trackerStore) has(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[hash]
	return ok
}

func (s *trackerStore) row(t *testing.T, hash string) *models.BlockProcessingStatus {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[hash]
	if !ok {
		t.Fatalf("no block_processing row for %s", hash)
	}
	cp := *row
	return &cp
}

func headerAt(height uint32, seed byte) *chaintracks.BlockHeader {
	var h chainhash.Hash
	h[0] = seed // seeds are unique per test row; height needn't feed the hash
	return &chaintracks.BlockHeader{Height: height, Hash: h}
}

func activeRow(hash string, height uint64) *models.BlockProcessingStatus {
	return &models.BlockProcessingStatus{
		BlockHash:   hash,
		BlockHeight: height,
		Status:      models.BlockStatusActive,
	}
}

// orphanedRow is a row a previous detection edge marked orphaned; with
// reconciled=true the anchor reconciler has also finished with it, so it
// has left the reconciler's queue — the state issue #339 was stuck in.
func orphanedRow(hash string, height uint64, reconciled bool) *models.BlockProcessingStatus {
	now := time.Now()
	row := activeRow(hash, height)
	row.Status = models.BlockStatusOrphaned
	row.OrphanedAt = &now
	if reconciled {
		row.ReconciledAt = &now
	}
	return row
}

func parkedRow(hash string, height uint64) *models.BlockProcessingStatus {
	row := activeRow(hash, height)
	row.Status = models.BlockStatusParked
	return row
}

func newTestTracker(ct chaintracks.Chaintracks, st store.Store, depth int, minInterval time.Duration) *blockStatusTracker {
	return &blockStatusTracker{
		store:           st,
		ct:              ct,
		logger:          zap.NewNop(),
		scanDepth:       depth,
		scanMinInterval: minInterval,
		branchWalkLimit: maxReorgBranchWalk,
	}
}

func assertActiveClean(t *testing.T, row *models.BlockProcessingStatus) {
	t.Helper()
	if row.Status != models.BlockStatusActive || row.OrphanedAt != nil || row.ReconciledAt != nil {
		t.Fatalf("row %s must be active with orphanedAt/reconciledAt cleared, got status=%s orphanedAt=%v reconciledAt=%v",
			row.BlockHash, row.Status, row.OrphanedAt, row.ReconciledAt)
	}
}

func sortedCopy(in []string) []string { return slices.Sorted(slices.Values(in)) }

func equalStrings(a, b []string) bool { return slices.Equal(a, b) }

// TestTieScan_MarksSameHeightLoserOrphaned is the no-ReorgEvent detection
// path of issue #279: an 'active' row whose height is held by a different
// block on the active chain must be marked orphaned by the tip-update scan.
func TestTieScan_MarksSameHeightLoserOrphaned(t *testing.T) {
	ct := newFakeChaintracks()
	winner := headerAt(10, 0xAA)
	ct.headers[10] = winner

	loserHash := headerAt(10, 0xBB).Hash.String()
	st := newTrackerStore(
		activeRow(winner.Hash.String(), 10), // matches the active chain — untouched
		activeRow(loserHash, 10),            // same height, different hash — orphan
	)

	tr := newTestTracker(ct, st, 20, 0)
	tr.tieScan(context.Background(), 12, false)

	calls := st.orphanCalls()
	if len(calls) != 1 || len(calls[0]) != 1 || calls[0][0] != loserHash {
		t.Fatalf("expected exactly the loser %s orphaned, got %v", loserHash, calls)
	}
	if ups := st.upsertCalls(); len(ups) != 0 {
		t.Fatalf("an already-active winner must not be re-upserted, got %v", ups)
	}
}

// TestTieScan_SkipsUnjudgeableRows: rows the scan cannot positively judge —
// height 0 placeholders, heights above the tip, heights chaintracks doesn't
// know, parked rows — must be left alone in BOTH directions. The orphaned
// row sits at a height chaintracks has no header for, so it stays orphaned.
func TestTieScan_SkipsUnjudgeableRows(t *testing.T) {
	ct := newFakeChaintracks()
	ct.headers[10] = headerAt(10, 0xAA)

	orphanHash := headerAt(9, 0xEE).Hash.String()
	parkedHash := headerAt(10, 0xAA).Hash.String()

	st := newTrackerStore(
		activeRow(headerAt(0, 0x01).Hash.String(), 0),   // placeholder: height filter
		activeRow(headerAt(50, 0x02).Hash.String(), 50), // above tip
		activeRow(headerAt(11, 0x03).Hash.String(), 11), // tip-window but chaintracks has no header at 11
		orphanedRow(orphanHash, 9, true),                // orphaned, chaintracks has no header at 9: unjudgeable
		parkedRow(parkedHash, 10),                       // parked IS the active block, but parked rows are the watchdog's
	)

	tr := newTestTracker(ct, st, 20, 0)
	tr.tieScan(context.Background(), 12, false)

	if calls := st.orphanCalls(); len(calls) != 0 {
		t.Fatalf("expected no orphan marks, got %v", calls)
	}
	if ups := st.upsertCalls(); len(ups) != 0 {
		t.Fatalf("expected no reactivations, got %v", ups)
	}
	if got := st.row(t, orphanHash).Status; got != models.BlockStatusOrphaned {
		t.Fatalf("unjudgeable orphaned row must stay orphaned, got %s", got)
	}
	if got := st.row(t, parkedHash).Status; got != models.BlockStatusParked {
		t.Fatalf("parked row must stay parked, got %s", got)
	}
}

// TestTieScan_Debounce: scans inside the min interval are dropped; the next
// one after it runs. A scan that runs always pages the table, so the
// fake's list-call count is the "did it run" signal.
func TestTieScan_Debounce(t *testing.T) {
	ct := newFakeChaintracks()
	ct.headers[10] = headerAt(10, 0xAA)
	loser := activeRow(headerAt(10, 0xBB).Hash.String(), 10)
	st := newTrackerStore(loser)

	tr := newTestTracker(ct, st, 20, time.Hour)
	tr.tieScan(context.Background(), 12, false)
	if calls := st.orphanCalls(); len(calls) != 1 {
		t.Fatalf("expected the first scan to orphan the loser, got %v", calls)
	}
	ran := st.listCalls()
	tr.tieScan(context.Background(), 12, false) // debounced
	if got := st.listCalls(); got != ran {
		t.Fatalf("expected the second scan to be debounced, table was paged again (%d → %d)", ran, got)
	}

	tr.lastScan = time.Now().Add(-2 * time.Hour)
	tr.tieScan(context.Background(), 12, false)
	if got := st.listCalls(); got == ran {
		t.Fatal("expected the post-interval scan to run")
	}
}

// TestTieScan_ForceBypassesDebounce: the reorg loop's scan runs on a strong
// signal (a ReorgEvent) and must never be the one the debounce drops.
func TestTieScan_ForceBypassesDebounce(t *testing.T) {
	ct := newFakeChaintracks()
	ct.headers[10] = headerAt(10, 0xAA)
	st := newTrackerStore(activeRow(headerAt(10, 0xBB).Hash.String(), 10))

	tr := newTestTracker(ct, st, 20, time.Hour)
	tr.tieScan(context.Background(), 12, false)
	ran := st.listCalls()
	tr.tieScan(context.Background(), 12, false) // debounced
	if got := st.listCalls(); got != ran {
		t.Fatalf("expected the unforced scan to be debounced (%d → %d)", ran, got)
	}
	tr.tieScan(context.Background(), 12, true) // forced: runs despite the interval
	if got := st.listCalls(); got == ran {
		t.Fatal("expected the forced scan to run despite the debounce interval")
	}
}

// TestTieScan_DisabledByZeroDepth: tie_scan_depth=0 turns the scan off.
func TestTieScan_DisabledByZeroDepth(t *testing.T) {
	ct := newFakeChaintracks()
	ct.headers[10] = headerAt(10, 0xAA)
	st := newTrackerStore(activeRow(headerAt(10, 0xBB).Hash.String(), 10))

	tr := newTestTracker(ct, st, 0, 0)
	tr.tieScan(context.Background(), 12, false)

	if calls := st.orphanCalls(); len(calls) != 0 {
		t.Fatalf("expected disabled scan to do nothing, got %v", calls)
	}
}

// TestTieScan_ReactivatesResurrectedRow is the inverse edge of issue #339:
// an 'orphaned' row (already reconciled, so the anchor reconciler will never
// visit it again) whose hash IS the active-chain block at its height must be
// reset to active in the same pass that orphans the competitor that lost the
// height. A genuine loser stays orphaned; parked rows are never touched.
func TestTieScan_ReactivatesResurrectedRow(t *testing.T) {
	ct := newFakeChaintracks()
	winner := headerAt(10, 0xAA) // the resurrected block: active chain holds it at 10
	ct.headers[10] = winner
	ct.headers[9] = headerAt(9, 0xDD)

	loserHash := headerAt(10, 0xBB).Hash.String() // was tip, just lost the height
	staleHash := headerAt(10, 0xCC).Hash.String() // lost the competition for real
	parkedHash := headerAt(9, 0xDD).Hash.String() // on the active chain but parked by the watchdog
	winnerHash := winner.Hash.String()

	st := newTrackerStore(
		activeRow(loserHash, 10),
		orphanedRow(winnerHash, 10, true),
		orphanedRow(staleHash, 10, true),
		parkedRow(parkedHash, 9),
	)

	tr := newTestTracker(ct, st, 20, 0)
	tr.tieScan(context.Background(), 12, false)

	// Orphan direction unchanged: exactly the loser.
	calls := st.orphanCalls()
	if len(calls) != 1 || len(calls[0]) != 1 || calls[0][0] != loserHash {
		t.Fatalf("expected exactly the loser %s orphaned, got %v", loserHash, calls)
	}
	// Reactivation: exactly the winner, via the resurrecting upsert.
	if ups := st.upsertCalls(); !equalStrings(ups, []string{winnerHash}) {
		t.Fatalf("expected exactly the winner %s reactivated, got %v", winnerHash, ups)
	}
	assertActiveClean(t, st.row(t, winnerHash))
	if got := st.row(t, staleHash).Status; got != models.BlockStatusOrphaned {
		t.Fatalf("genuine loser must stay orphaned, got %s", got)
	}
	if got := st.row(t, parkedHash).Status; got != models.BlockStatusParked {
		t.Fatalf("parked row must stay parked, got %s", got)
	}
}

// TestTieScan_ReactivationRespectsWindow: orphaned rows that DO match the
// active chain but sit outside the scan window (above the tip, below
// tip-depth, or the height-0 placeholders) are not touched — the same
// window the orphan direction honors.
func TestTieScan_ReactivationRespectsWindow(t *testing.T) {
	ct := newFakeChaintracks()
	above := headerAt(50, 0x01)
	below := headerAt(3, 0x02)
	zero := headerAt(0, 0x03)
	ct.headers[50], ct.headers[3], ct.headers[0] = above, below, zero

	st := newTrackerStore(
		orphanedRow(above.Hash.String(), 50, true), // above tip 12
		orphanedRow(below.Hash.String(), 3, true),  // below minHeight (12-5=7)
		orphanedRow(zero.Hash.String(), 0, true),   // placeholder height
	)

	tr := newTestTracker(ct, st, 5, 0)
	tr.tieScan(context.Background(), 12, false)

	if ups := st.upsertCalls(); len(ups) != 0 {
		t.Fatalf("out-of-window orphaned rows must not be reactivated, got %v", ups)
	}
}

// TestTieScan_DedupsAcrossPageBoundaries: with tie_scan_depth=1 the pager
// runs 4-row pages, and six rows at one height force the boundary-safe
// pager to re-fetch (and re-visit) rows. Each hash must still be marked or
// reactivated exactly once.
func TestTieScan_DedupsAcrossPageBoundaries(t *testing.T) {
	ct := newFakeChaintracks()
	winner := headerAt(10, 0xAA)
	ct.headers[10] = winner

	rows := []*models.BlockProcessingStatus{orphanedRow(winner.Hash.String(), 10, true)}
	wantOrphaned := make([]string, 0, 5)
	for seed := byte(0xB0); seed < 0xB5; seed++ {
		h := headerAt(10, seed).Hash.String()
		rows = append(rows, activeRow(h, 10))
		wantOrphaned = append(wantOrphaned, h)
	}
	st := newTrackerStore(rows...)

	tr := newTestTracker(ct, st, 1, 0)
	tr.tieScan(context.Background(), 10, false)

	calls := st.orphanCalls()
	if len(calls) != 1 || !equalStrings(sortedCopy(calls[0]), sortedCopy(wantOrphaned)) {
		t.Fatalf("expected one MarkBlocksOrphaned call with the 5 losers, got %v", calls)
	}
	if ups := st.upsertCalls(); !equalStrings(ups, []string{winner.Hash.String()}) {
		t.Fatalf("expected exactly one reactivation of %s, got %v", winner.Hash, ups)
	}
}

// TestRecordReorg_ReactivatesResurrectedBranch is issue #339 in miniature.
// A was tip at height 10; B lost the same-height tie, was marked orphaned
// and later reconciled (off the reconciler's queue). D arrives on B:
// chaintracks emits ReorgEvent{orphaned: [A], commonAncestor: 9, newTip: D}
// — B is never named. The handler must walk the new branch and reset B's
// row to active, in addition to orphaning A and recording D.
func TestRecordReorg_ReactivatesResurrectedBranch(t *testing.T) {
	ct := newFakeChaintracks()
	ancestor := headerAt(9, 0x01)
	a := headerAt(10, 0xAA)
	b := headerAt(10, 0xBB)
	d := headerAt(11, 0xDD)
	ct.headers[9], ct.headers[10], ct.headers[11] = ancestor, b, d

	st := newTrackerStore(
		activeRow(ancestor.Hash.String(), 9),
		activeRow(a.Hash.String(), 10),
		orphanedRow(b.Hash.String(), 10, true),
	)

	tr := newTestTracker(ct, st, 20, 0)
	tr.recordReorg(context.Background(), &chaintracks.ReorgEvent{
		OrphanedHashes: []chainhash.Hash{a.Hash},
		CommonAncestor: ancestor,
		NewTip:         d,
		Depth:          1,
	})

	calls := st.orphanCalls()
	if len(calls) != 1 || !equalStrings(calls[0], []string{a.Hash.String()}) {
		t.Fatalf("expected exactly A orphaned, got %v", calls)
	}
	if got := st.row(t, a.Hash.String()).Status; got != models.BlockStatusOrphaned {
		t.Fatalf("A must be orphaned, got %s", got)
	}
	assertActiveClean(t, st.row(t, b.Hash.String()))
	assertActiveClean(t, st.row(t, d.Hash.String()))
	// Exactly the new tip and the resurrected block are upserted; the
	// common ancestor was never off the chain and is left alone.
	want := sortedCopy([]string{b.Hash.String(), d.Hash.String()})
	if ups := sortedCopy(st.upsertCalls()); !equalStrings(ups, want) {
		t.Fatalf("expected upserts %v, got %v", want, ups)
	}
}

// TestRecordReorg_OrphanMetricCountsAppliedTransitions: the store silently
// skips hashes without a row, and re-marking an already-orphaned row is not
// a transition, so the reorg_event/orphaned series must count only rows
// that actually changed status — while the store call still names every
// hash the event carried.
func TestRecordReorg_OrphanMetricCountsAppliedTransitions(t *testing.T) {
	ct := newFakeChaintracks()
	tip := headerAt(11, 0xDD)
	ct.headers[11] = tip
	wasActive := headerAt(10, 0xAA).Hash
	wasOrphaned := headerAt(10, 0xBB).Hash
	neverSeen := headerAt(10, 0xCC).Hash
	st := newTrackerStore(
		activeRow(wasActive.String(), 10),
		orphanedRow(wasOrphaned.String(), 10, true),
	)

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceReorgEvent)
	before := testutil.ToFloat64(counter)

	tr := newTestTracker(ct, st, 20, 0)
	tr.recordReorg(context.Background(), &chaintracks.ReorgEvent{
		OrphanedHashes: []chainhash.Hash{wasActive, wasOrphaned, neverSeen},
		NewTip:         tip,
	})

	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("orphaned transitions = %v, want 1 (re-marks and missing rows are not transitions)", got)
	}
	calls := st.orphanCalls()
	if len(calls) != 1 || len(calls[0]) != 3 {
		t.Fatalf("every named hash must still be passed to the store, got %v", calls)
	}
}

// TestRecordReorg_DeepBranchWalksEveryHeight: a 3-deep reorg walks every
// intermediate height of the new branch. Only rows that exist AND are
// orphaned are reactivated — a missing row is not inserted (that is the tip
// channel's job) and a parked row stays parked.
func TestRecordReorg_DeepBranchWalksEveryHeight(t *testing.T) {
	ct := newFakeChaintracks()
	ancestor := headerAt(9, 0x01)
	h10, h11, h12, tip := headerAt(10, 0x10), headerAt(11, 0x11), headerAt(12, 0x12), headerAt(13, 0x13)
	ct.headers[9], ct.headers[10], ct.headers[11], ct.headers[12], ct.headers[13] = ancestor, h10, h11, h12, tip

	st := newTrackerStore(
		orphanedRow(h10.Hash.String(), 10, false), // orphaned, still queued: reactivate anyway
		// h11: no row at all
		parkedRow(h12.Hash.String(), 12),
	)

	tr := newTestTracker(ct, st, 20, 0)
	tr.recordReorg(context.Background(), &chaintracks.ReorgEvent{
		OrphanedHashes: []chainhash.Hash{headerAt(10, 0xAA).Hash, headerAt(11, 0xAB).Hash, headerAt(12, 0xAC).Hash},
		CommonAncestor: ancestor,
		NewTip:         tip,
		Depth:          3,
	})

	assertActiveClean(t, st.row(t, h10.Hash.String()))
	if st.has(h11.Hash.String()) {
		t.Fatal("the branch walk must not insert rows for blocks arcade never saw")
	}
	if got := st.row(t, h12.Hash.String()).Status; got != models.BlockStatusParked {
		t.Fatalf("parked row must stay parked, got %s", got)
	}
	want := sortedCopy([]string{h10.Hash.String(), tip.Hash.String()})
	if ups := sortedCopy(st.upsertCalls()); !equalStrings(ups, want) {
		t.Fatalf("expected upserts %v, got %v", want, ups)
	}
}

// TestRecordReorg_FailsOpenPerHeight: a height chaintracks cannot serve is
// skipped without aborting the walk — the remaining heights still heal.
func TestRecordReorg_FailsOpenPerHeight(t *testing.T) {
	ct := newFakeChaintracks()
	ancestor := headerAt(9, 0x01)
	h10, tip := headerAt(10, 0x10), headerAt(12, 0x12)
	ct.headers[9], ct.headers[10], ct.headers[12] = ancestor, h10, tip // no header at 11

	unknownAt11 := headerAt(11, 0x11).Hash.String()
	st := newTrackerStore(
		orphanedRow(h10.Hash.String(), 10, true),
		orphanedRow(unknownAt11, 11, true),
	)

	tr := newTestTracker(ct, st, 20, 0)
	tr.recordReorg(context.Background(), &chaintracks.ReorgEvent{
		CommonAncestor: ancestor,
		NewTip:         tip,
	})

	assertActiveClean(t, st.row(t, h10.Hash.String()))
	if got := st.row(t, unknownAt11).Status; got != models.BlockStatusOrphaned {
		t.Fatalf("row at an unjudgeable height must be left alone, got %s", got)
	}
}

// TestRecordReorg_SkipsWalkWithoutCommonAncestor: without a fork point the
// handler cannot bound the branch; it records the tip and leaves the
// reactivation to the (forced) tie-scan that follows in the reorg loop.
func TestRecordReorg_SkipsWalkWithoutCommonAncestor(t *testing.T) {
	ct := newFakeChaintracks()
	b, tip := headerAt(10, 0xBB), headerAt(11, 0xDD)
	ct.headers[10], ct.headers[11] = b, tip

	st := newTrackerStore(orphanedRow(b.Hash.String(), 10, true))
	tr := newTestTracker(ct, st, 20, 0)
	tr.recordReorg(context.Background(), &chaintracks.ReorgEvent{NewTip: tip})

	if ups := st.upsertCalls(); !equalStrings(ups, []string{tip.Hash.String()}) {
		t.Fatalf("expected only the tip upserted, got %v", ups)
	}
}

// TestRecordReorg_WalkIsBounded: the walk covers at most branchWalkLimit
// heights below the tip (nearest first); deeper rows are left for the
// reconciler's full-scan lever.
func TestRecordReorg_WalkIsBounded(t *testing.T) {
	ct := newFakeChaintracks()
	ancestor := headerAt(5, 0x01)
	h6, h7, h8, h9, tip := headerAt(6, 0x06), headerAt(7, 0x07), headerAt(8, 0x08), headerAt(9, 0x09), headerAt(10, 0x10)
	ct.headers[5], ct.headers[6], ct.headers[7], ct.headers[8], ct.headers[9], ct.headers[10] = ancestor, h6, h7, h8, h9, tip

	st := newTrackerStore(
		orphanedRow(h6.Hash.String(), 6, true),
		orphanedRow(h7.Hash.String(), 7, true),
		orphanedRow(h8.Hash.String(), 8, true),
		orphanedRow(h9.Hash.String(), 9, true),
	)

	tr := newTestTracker(ct, st, 20, 0)
	tr.branchWalkLimit = 2
	tr.recordReorg(context.Background(), &chaintracks.ReorgEvent{
		CommonAncestor: ancestor,
		NewTip:         tip,
	})

	assertActiveClean(t, st.row(t, h9.Hash.String()))
	assertActiveClean(t, st.row(t, h8.Hash.String()))
	for _, h := range []string{h7.Hash.String(), h6.Hash.String()} {
		if got := st.row(t, h).Status; got != models.BlockStatusOrphaned {
			t.Fatalf("row %s beyond the walk limit must be left for the full-scan, got %s", h, got)
		}
	}
}

// TestReorgLoop_ReactivatesViaForcedScan pins the loop wiring: even when the
// event carries no common ancestor (no branch walk) and a scan ran a moment
// ago (debounce armed), the reorg loop's tie-scan is forced and reactivates
// the resurrected row.
func TestReorgLoop_ReactivatesViaForcedScan(t *testing.T) {
	ct := newFakeChaintracks()
	b, tip := headerAt(10, 0xBB), headerAt(11, 0xDD)
	ct.headers[10], ct.headers[11] = b, tip

	st := newTrackerStore(orphanedRow(b.Hash.String(), 10, true))
	tr := newTestTracker(ct, st, 20, time.Hour)
	tr.lastScan = time.Now() // a header-loop scan just ran: an unforced scan would be dropped

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.runReorgLoop(ctx, ct.reorgCh)
	ct.reorgCh <- &chaintracks.ReorgEvent{NewTip: tip}

	deadline := time.Now().Add(2 * time.Second)
	for st.row(t, b.Hash.String()).Status != models.BlockStatusActive {
		if time.Now().After(deadline) {
			t.Fatalf("reorg loop must reactivate %s via the forced tie-scan, still %s", b.Hash, st.row(t, b.Hash.String()).Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertActiveClean(t, st.row(t, b.Hash.String()))
}
