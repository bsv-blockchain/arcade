package chaintracks_server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// trackerStore is the minimal store.Store fake the tie-scan touches:
// canned ListBlockProcessingStatus rows + a recording MarkBlocksOrphaned.
// The embedded nil interface panics on anything else, pinning the scan's
// store surface.
type trackerStore struct {
	store.Store

	mu       sync.Mutex
	rows     []*models.BlockProcessingStatus
	orphaned [][]string
}

func (s *trackerStore) ListBlockProcessingStatus(_ context.Context, _ uint64, _ int) ([]*models.BlockProcessingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*models.BlockProcessingStatus(nil), s.rows...), nil
}

func (s *trackerStore) MarkBlocksOrphaned(_ context.Context, hashes []string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orphaned = append(s.orphaned, append([]string(nil), hashes...))
	return nil
}

func (s *trackerStore) orphanCalls() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.orphaned...)
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

func newTestTracker(ct chaintracks.Chaintracks, st store.Store, depth int, minInterval time.Duration) *blockStatusTracker {
	return &blockStatusTracker{
		store:           st,
		ct:              ct,
		logger:          zap.NewNop(),
		scanDepth:       depth,
		scanMinInterval: minInterval,
	}
}

// TestTieScan_MarksSameHeightLoserOrphaned is the no-ReorgEvent detection
// path of issue #279: an 'active' row whose height is held by a different
// block on the active chain must be marked orphaned by the tip-update scan.
func TestTieScan_MarksSameHeightLoserOrphaned(t *testing.T) {
	ct := newFakeChaintracks()
	winner := headerAt(10, 0xAA)
	ct.headers[10] = winner

	loserHash := headerAt(10, 0xBB).Hash.String()
	st := &trackerStore{rows: []*models.BlockProcessingStatus{
		activeRow(winner.Hash.String(), 10), // matches the active chain — untouched
		activeRow(loserHash, 10),            // same height, different hash — orphan
	}}

	tr := newTestTracker(ct, st, 20, 0)
	tr.scanRecentActive(context.Background(), 12)

	calls := st.orphanCalls()
	if len(calls) != 1 || len(calls[0]) != 1 || calls[0][0] != loserHash {
		t.Fatalf("expected exactly the loser %s orphaned, got %v", loserHash, calls)
	}
}

// TestTieScan_SkipsUnjudgeableRows: rows the scan cannot positively judge —
// height 0 placeholders, heights above the tip, heights chaintracks doesn't
// know, non-active rows — must never be orphaned.
func TestTieScan_SkipsUnjudgeableRows(t *testing.T) {
	ct := newFakeChaintracks()
	ct.headers[10] = headerAt(10, 0xAA)

	orphanRow := activeRow(headerAt(9, 0xEE).Hash.String(), 9)
	orphanRow.Status = models.BlockStatusOrphaned

	st := &trackerStore{rows: []*models.BlockProcessingStatus{
		activeRow(headerAt(0, 0x01).Hash.String(), 0),   // placeholder: height filter
		activeRow(headerAt(50, 0x02).Hash.String(), 50), // above tip
		activeRow(headerAt(11, 0x03).Hash.String(), 11), // tip-window but chaintracks has no header at 11
		orphanRow, // already orphaned — only 'active' rows are scanned
	}}

	tr := newTestTracker(ct, st, 20, 0)
	tr.scanRecentActive(context.Background(), 12)

	if calls := st.orphanCalls(); len(calls) != 0 {
		t.Fatalf("expected no orphan marks, got %v", calls)
	}
}

// TestTieScan_Debounce: scans inside the min interval are dropped; the next
// one after it runs.
func TestTieScan_Debounce(t *testing.T) {
	ct := newFakeChaintracks()
	ct.headers[10] = headerAt(10, 0xAA)
	loser := activeRow(headerAt(10, 0xBB).Hash.String(), 10)
	st := &trackerStore{rows: []*models.BlockProcessingStatus{loser}}

	tr := newTestTracker(ct, st, 20, time.Hour)
	tr.scanRecentActive(context.Background(), 12)
	tr.scanRecentActive(context.Background(), 12) // debounced

	if calls := st.orphanCalls(); len(calls) != 1 {
		t.Fatalf("expected 1 scan to run (second debounced), got %d orphan calls", len(calls))
	}

	tr.lastScan = time.Now().Add(-2 * time.Hour)
	tr.scanRecentActive(context.Background(), 12)
	if calls := st.orphanCalls(); len(calls) != 2 {
		t.Fatalf("expected the post-interval scan to run, got %d orphan calls", len(calls))
	}
}

// TestTieScan_DisabledByZeroDepth: tie_scan_depth=0 turns the scan off.
func TestTieScan_DisabledByZeroDepth(t *testing.T) {
	ct := newFakeChaintracks()
	ct.headers[10] = headerAt(10, 0xAA)
	st := &trackerStore{rows: []*models.BlockProcessingStatus{
		activeRow(headerAt(10, 0xBB).Hash.String(), 10),
	}}

	tr := newTestTracker(ct, st, 0, 0)
	tr.scanRecentActive(context.Background(), 12)

	if calls := st.orphanCalls(); len(calls) != 0 {
		t.Fatalf("expected disabled scan to do nothing, got %v", calls)
	}
}
