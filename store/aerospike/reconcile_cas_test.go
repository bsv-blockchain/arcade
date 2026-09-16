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
	if _, err := s.MarkBlocksOrphaned(ctx, []string{a}, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkBlocksOrphaned(ctx, []string{b}, t0.Add(2*time.Minute)); err != nil {
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
	if _, err := s.MarkBlocksOrphaned(ctx, []string{b}, t0.Add(5*time.Minute)); err != nil {
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

// TestMarkBlocksOrphaned_ReorphanRequeues pins the re-orphan contract
// against a live Aerospike (issue #339 review): re-orphaning a row that was
// already reconciled clears reconciled_at so it re-enters the reconciler
// queue, and the returned count is applied status transitions only. The
// namespace is shared, so this uses a unique prefix and unique heights and
// reads the row back directly instead of through the shared queue.
func TestMarkBlocksOrphaned_ReorphanRequeues(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700001000, 0).UTC()
	const height = uint64(9_000_400)
	hash := "rb-requeue-a"

	if err := s.UpsertBlockHeaderSeen(ctx, hash, height, t0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	n, err := s.MarkBlocksOrphaned(ctx, []string{hash, "rb-requeue-missing"}, t0.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("transitions = %d, want 1 (a hash with no row is not a transition)", n)
	}
	ok, err := s.MarkBlockReconciled(ctx, hash, t0.Add(time.Minute), t0.Add(2*time.Minute))
	if err != nil || !ok {
		t.Fatalf("MarkBlockReconciled: ok=%v err=%v", ok, err)
	}

	n, err = s.MarkBlocksOrphaned(ctx, []string{hash}, t0.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("transitions = %d, want 0 (the row was already orphaned)", n)
	}
	row, err := s.GetBlockProcessingStatus(ctx, hash)
	if err != nil {
		t.Fatalf("GetBlockProcessingStatus: %v", err)
	}
	if row.ReconciledAt != nil {
		t.Fatalf("re-orphaning must clear reconciled_at, got %v", row.ReconciledAt)
	}
	if row.OrphanedAt == nil || !row.OrphanedAt.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("re-orphaning must stamp the new generation, got %v", row.OrphanedAt)
	}
	if row.Status != models.BlockStatusOrphaned {
		t.Fatalf("status = %s, want orphaned", row.Status)
	}
}
