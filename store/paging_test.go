package store_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// pagingStore implements just ListBlockProcessingStatus over a canned row
// set, honoring the real keyset contract (height DESC, block_height <
// beforeHeight, limit) so page boundaries land exactly where a test puts
// them. The embedded nil interface panics on anything else.
type pagingStore struct {
	store.Store

	rows []*models.BlockProcessingStatus
}

func (s *pagingStore) ListBlockProcessingStatus(_ context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	sorted := append([]*models.BlockProcessingStatus(nil), s.rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].BlockHeight > sorted[j].BlockHeight })
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

func pagingRows(heights ...uint64) []*models.BlockProcessingStatus {
	out := make([]*models.BlockProcessingStatus, len(heights))
	for i, h := range heights {
		out[i] = &models.BlockProcessingStatus{
			BlockHash:   fmt.Sprintf("blk-%d-%d", h, i),
			BlockHeight: h,
			Status:      models.BlockStatusActive,
		}
	}
	return out
}

func collectDistinct(t *testing.T, s store.Store, minHeight uint64, pageSize int) map[string]int {
	t.Helper()
	visits := make(map[string]int)
	err := store.ForEachBlockProcessing(context.Background(), s, minHeight, pageSize, func(row *models.BlockProcessingStatus) error {
		visits[row.BlockHash]++
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachBlockProcessing: %v", err)
	}
	return visits
}

// TestForEachBlockProcessing_SameHeightRunStraddlesPageBoundary is the
// review finding on the reorg scans: with a `block_height < before` cursor
// over a non-unique key, rows at the boundary height beyond the page end
// were silently skipped. Every row must be visited at least once.
func TestForEachBlockProcessing_SameHeightRunStraddlesPageBoundary(t *testing.T) {
	// Page size 4: page 1 = [50, 49, 49, 49], leaving two more 49s and the
	// tail behind the boundary.
	s := &pagingStore{rows: pagingRows(50, 49, 49, 49, 49, 49, 48, 47)}
	visits := collectDistinct(t, s, 0, 4)
	if len(visits) != len(s.rows) {
		t.Fatalf("visited %d distinct rows, want %d (boundary rows skipped)", len(visits), len(s.rows))
	}
}

// TestForEachBlockProcessing_WholePageSingleHeight: when one height has
// more rows than the page, the pager grows the page instead of skipping.
func TestForEachBlockProcessing_WholePageSingleHeight(t *testing.T) {
	s := &pagingStore{rows: pagingRows(20, 10, 10, 10, 10, 10, 10, 10, 10, 10, 5)}
	visits := collectDistinct(t, s, 0, 2)
	if len(visits) != len(s.rows) {
		t.Fatalf("visited %d distinct rows, want %d", len(visits), len(s.rows))
	}
}

// TestForEachBlockProcessing_MinHeightStops: rows below minHeight are never
// delivered, rows at or above it always are.
func TestForEachBlockProcessing_MinHeightStops(t *testing.T) {
	s := &pagingStore{rows: pagingRows(12, 11, 10, 9, 8)}
	visits := collectDistinct(t, s, 10, 2)
	if len(visits) != 3 {
		t.Fatalf("visited %d distinct rows, want 3 (>= minHeight)", len(visits))
	}
	for hash := range visits {
		if hash == "blk-9-3" || hash == "blk-8-4" {
			t.Fatalf("row below minHeight visited: %s", hash)
		}
	}
}

// TestForEachBlockProcessing_FnErrorStops: fn's error surfaces immediately.
func TestForEachBlockProcessing_FnErrorStops(t *testing.T) {
	s := &pagingStore{rows: pagingRows(3, 2, 1)}
	sentinel := errors.New("stop here")
	calls := 0
	err := store.ForEachBlockProcessing(context.Background(), s, 0, 10, func(*models.BlockProcessingStatus) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times after error, want 1", calls)
	}
}

// TestForEachBlockProcessing_OverflowingHeightErrors: a single height with
// more rows than the growth cap must error loudly, never truncate silently.
func TestForEachBlockProcessing_OverflowingHeightErrors(t *testing.T) {
	heights := make([]uint64, 70000)
	for i := range heights {
		heights[i] = 42
	}
	heights = append([]uint64{100}, heights...)
	s := &pagingStore{rows: pagingRows(heights...)}
	err := store.ForEachBlockProcessing(context.Background(), s, 0, 1024, func(*models.BlockProcessingStatus) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected an overflow error for a height exceeding the page cap")
	}
}
