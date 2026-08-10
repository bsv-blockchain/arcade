//go:build integration

package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// TestListOrphanedBlocksToReconcile_PrioritizesReanchorable pins the work-queue
// prioritization against a live Aerospike: an orphan whose active (canonical)
// block at the same height already has a stored BUMP — so the reconciler can
// re-anchor it right now — surfaces AHEAD of orphans whose canonical block has
// no BUMP, even when it was orphaned more recently, with oldest-orphaned_at
// order preserved WITHIN the un-re-anchorable group. A BUMP stored for the
// ORPHAN's own hash (not the active block) must NOT make it re-anchorable.
//
// The namespace is shared with other integration tests, so heights are chosen
// high/unique and assertions check the RELATIVE order of this test's own rows
// within the (totally ordered) result rather than the absolute queue contents.
func TestListOrphanedBlocksToReconcile_PrioritizesReanchorable(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700000500, 0).UTC()

	const base = uint64(9_000_100)
	// Three un-re-anchorable competitions: the active block at each height has
	// NO stored BUMP. Orphaned oldest→newest across the base+0/1/2 heights.
	names := []string{"reanchor-A", "reanchor-B", "reanchor-C"}
	for i, orphan := range names {
		h := base + uint64(i)
		active := "reanchor-act-" + orphan
		if err := s.UpsertBlockHeaderSeen(ctx, active, h, t0); err != nil {
			t.Fatalf("seed active %s: %v", active, err)
		}
		if err := s.UpsertBlockHeaderSeen(ctx, orphan, h, t0); err != nil {
			t.Fatalf("seed orphan %s: %v", orphan, err)
		}
		if err := s.MarkBlocksOrphaned(ctx, []string{orphan}, t0.Add(time.Duration(i+1)*time.Minute)); err != nil {
			t.Fatalf("orphan %s: %v", orphan, err)
		}
	}
	// A BUMP stored for the ORPHAN's own hash must not count.
	if err := s.InsertBUMP(ctx, "reanchor-A", base, []byte{0x01}); err != nil {
		t.Fatalf("InsertBUMP reanchor-A: %v", err)
	}

	// One re-anchorable competition: the active block HAS a stored BUMP, but it
	// was orphaned LAST (newest orphaned_at).
	const canonHeight = base + 3
	if err := s.UpsertBlockHeaderSeen(ctx, "reanchor-act-canon", canonHeight, t0); err != nil {
		t.Fatalf("seed reanchor-act-canon: %v", err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "reanchor-canon", canonHeight, t0); err != nil {
		t.Fatalf("seed reanchor-canon: %v", err)
	}
	if err := s.MarkBlocksOrphaned(ctx, []string{"reanchor-canon"}, t0.Add(10*time.Minute)); err != nil {
		t.Fatalf("orphan reanchor-canon: %v", err)
	}
	if err := s.InsertBUMP(ctx, "reanchor-act-canon", canonHeight, []byte{0xde, 0xad}); err != nil {
		t.Fatalf("InsertBUMP reanchor-act-canon: %v", err)
	}

	rows, err := s.ListOrphanedBlocksToReconcile(ctx, 1000)
	if err != nil {
		t.Fatalf("ListOrphanedBlocksToReconcile: %v", err)
	}

	// Filter to this test's orphans, preserving the returned (total) order.
	mine := map[string]bool{"reanchor-A": true, "reanchor-B": true, "reanchor-C": true, "reanchor-canon": true}
	var got []string
	for _, r := range rows {
		if r.Status == models.BlockStatusOrphaned && mine[r.BlockHash] {
			got = append(got, r.BlockHash)
		}
	}
	want := []string{"reanchor-canon", "reanchor-A", "reanchor-B", "reanchor-C"}
	if len(got) != len(want) {
		t.Fatalf("filtered queue = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("filtered queue = %v, want %v (re-anchorable first, then oldest orphaned_at)", got, want)
		}
	}
}
