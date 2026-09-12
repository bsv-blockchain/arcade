//go:build integration

package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// TestMarkBlockReconciled_GenerationCheck pins the CAS semantics of the
// reconcile stamp against a live Aerospike (issue #339): the stamp applies
// only while the row is still orphaned with the orphaned_at generation the
// reconciler processed, so a tick finishing after the block-status tracker
// reactivated (or re-orphaned) the row cannot write reconciled_at onto the
// newer state. The namespace is shared with other integration tests, so the
// rows use a unique prefix and unique heights, and assertions read the rows
// back directly rather than through the shared reconcile queue.
func TestMarkBlockReconciled_GenerationCheck(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700000900, 0).UTC()
	const base = uint64(9_000_300)
	a, b := "rb-cas-a", "rb-cas-b"

	for i, hash := range []string{a, b} {
		if err := s.UpsertBlockHeaderSeen(ctx, hash, base+uint64(i), t0); err != nil { //nolint:gosec // tiny loop index
			t.Fatalf("seed %s: %v", hash, err)
		}
	}
	if err := s.MarkBlocksOrphaned(ctx, []string{a}, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkBlocksOrphaned(ctx, []string{b}, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// Stale generation: no-op, row stays unreconciled.
	stamped, err := s.MarkBlockReconciled(ctx, a, t0 /* not a's orphaned_at */, t0.Add(3*time.Minute))
	if err != nil || stamped {
		t.Fatalf("stale-generation stamp must be a no-op, got stamped=%v err=%v", stamped, err)
	}
	if got, gerr := s.GetBlockProcessingStatus(ctx, a); gerr != nil || got.ReconciledAt != nil {
		t.Fatalf("stale stamp must not touch the row, got %+v err=%v", got, gerr)
	}

	// Matching generation: applies.
	stamped, err = s.MarkBlockReconciled(ctx, a, t0.Add(time.Minute), t0.Add(3*time.Minute))
	if err != nil || !stamped {
		t.Fatalf("matching-generation stamp must apply, got stamped=%v err=%v", stamped, err)
	}
	if got, gerr := s.GetBlockProcessingStatus(ctx, a); gerr != nil || got.ReconciledAt == nil {
		t.Fatalf("row must carry reconciled_at after the stamp, got %+v err=%v", got, gerr)
	}

	// Resurrected (active) row: never stamped, even without a generation.
	if err := s.UpsertBlockHeaderSeen(ctx, b, base+1, t0.Add(time.Hour)); err != nil {
		t.Fatalf("resurrect: %v", err)
	}
	stamped, err = s.MarkBlockReconciled(ctx, b, time.Time{}, t0.Add(4*time.Minute))
	if err != nil || stamped {
		t.Fatalf("stamp on a resurrected row must be a no-op, got stamped=%v err=%v", stamped, err)
	}
	if got, gerr := s.GetBlockProcessingStatus(ctx, b); gerr != nil || got.Status != models.BlockStatusActive || got.ReconciledAt != nil {
		t.Fatalf("resurrected row must stay active and clean, got %+v err=%v", got, gerr)
	}

	// Zero generation only requires the row to be orphaned.
	if err := s.MarkBlocksOrphaned(ctx, []string{b}, t0.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	stamped, err = s.MarkBlockReconciled(ctx, b, time.Time{}, t0.Add(6*time.Minute))
	if err != nil || !stamped {
		t.Fatalf("zero-generation stamp on an orphaned row must apply, got stamped=%v err=%v", stamped, err)
	}

	// Missing rows are silently skipped.
	stamped, err = s.MarkBlockReconciled(ctx, "rb-cas-never-seen", t0, t0)
	if err != nil || stamped {
		t.Fatalf("stamp on a missing row must be a no-op, got stamped=%v err=%v", stamped, err)
	}
}
