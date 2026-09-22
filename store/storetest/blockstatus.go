package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// BlockStatusBackend is the slice of store.Store the block-status suite
// drives: the block_processing writes whose contract is a generation-checked
// transition (issue #339), plus the reads that verify them.
type BlockStatusBackend interface {
	UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error
	MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error
	MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) (int, error)
	MarkBlocksParked(ctx context.Context, blockHashes []string) error
	MarkBlockReconciled(ctx context.Context, blockHash string, orphanedAt, at time.Time) (bool, error)
	ReactivateBlock(ctx context.Context, blockHash string, blockHeight uint64, orphanedAt time.Time) (bool, error)
	GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error)
	ListOrphanedBlocksToReconcile(ctx context.Context, limit int) ([]*models.BlockProcessingStatus, error)
}

// blockSuiteHeight keeps the suite's rows far above any real chain and away
// from the other suites' rows: Aerospike runs it in a namespace shared with
// every other integration test, so hashes carry a per-subtest prefix and the
// suite only ever reads its own rows back — never the shared reconcile queue.
const blockSuiteHeight = uint64(9_300_000)

// RunBlockStatusSuite asserts that a backend's generation-checked
// block_processing writes honor the store.Store contract:
//
//   - ReactivateBlock applies only to a row still orphaned with the
//     generation the caller judged (zero = status only), resets the row to
//     the shape UpsertBlockHeaderSeen's conflict path produces, and leaves
//     missing, active, parked and re-orphaned rows alone.
//   - MarkBlocksOrphaned reports applied transitions only, and re-orphaning
//     clears the previous generation's stamp. On a row already orphaned the
//     generation refresh is forward-only-inclusive: a newer stored
//     generation is kept (stamp state and all), an equal one still refreshes
//     and requeues.
//   - MarkBlockReconciled applies only to a row still orphaned with the
//     generation the caller processed.
//
// Whole-second timestamps throughout: the backends store nanoseconds
// (Pebble, Aerospike), microseconds (Postgres) and milliseconds (MongoDB),
// and the generation compare must hold on the value read back.
// newBackend is called once per subtest.
func RunBlockStatusSuite(t *testing.T, newBackend func(t *testing.T) BlockStatusBackend) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Unix(1_700_100_000, 0).UTC()
	gen1, gen2 := t0.Add(time.Minute), t0.Add(2*time.Minute)

	// seedOrphan inserts an active row with a processed_at milestone and
	// orphans it at gen1.
	seedOrphan := func(t *testing.T, b BlockStatusBackend, hash string, height uint64) {
		t.Helper()
		if err := b.UpsertBlockHeaderSeen(ctx, hash, height, t0); err != nil {
			t.Fatalf("seed %s: %v", hash, err)
		}
		if err := b.MarkBlockProcessed(ctx, hash, height, t0.Add(30*time.Second)); err != nil {
			t.Fatalf("milestone %s: %v", hash, err)
		}
		if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, gen1); err != nil || n != 1 {
			t.Fatalf("orphan %s: transitions=%d err=%v, want 1", hash, n, err)
		}
	}
	row := func(t *testing.T, b BlockStatusBackend, hash string) *models.BlockProcessingStatus {
		t.Helper()
		got, err := b.GetBlockProcessingStatus(ctx, hash)
		if err != nil {
			t.Fatalf("GetBlockProcessingStatus(%s): %v", hash, err)
		}
		return got
	}
	assertOrphanedAt := func(t *testing.T, got *models.BlockProcessingStatus, gen time.Time) {
		t.Helper()
		if got.Status != models.BlockStatusOrphaned || got.OrphanedAt == nil || !got.OrphanedAt.Equal(gen) {
			t.Fatalf("row %s: want orphaned at %v, got status=%s orphanedAt=%v",
				got.BlockHash, gen, got.Status, got.OrphanedAt)
		}
	}
	// queued reports whether hash is on the reconcile queue. Membership, not
	// equality: Aerospike runs this suite in a namespace shared with other
	// integration tests, whose orphaned rows may sit on the same queue.
	queued := func(t *testing.T, b BlockStatusBackend, hash string) bool {
		t.Helper()
		rows, err := b.ListOrphanedBlocksToReconcile(ctx, 1000)
		if err != nil {
			t.Fatalf("ListOrphanedBlocksToReconcile: %v", err)
		}
		for _, r := range rows {
			if r.BlockHash == hash {
				return true
			}
		}
		return false
	}

	t.Run("ReactivateBlock", func(t *testing.T) {
		t.Run("AppliesOnMatchingGeneration", func(t *testing.T) {
			b := newBackend(t)
			const hash = "bs-react-apply"
			seedOrphan(t, b, hash, blockSuiteHeight)

			applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+1, gen1)
			if err != nil || !applied {
				t.Fatalf("ReactivateBlock: applied=%v err=%v, want true", applied, err)
			}
			got := row(t, b, hash)
			if got.Status != models.BlockStatusActive || got.OrphanedAt != nil || got.ReconciledAt != nil {
				t.Fatalf("want active with both marks cleared, got %+v", got)
			}
			if got.BlockHeight != blockSuiteHeight+1 {
				t.Fatalf("block_height = %d, want %d (chaintracks is authoritative)", got.BlockHeight, blockSuiteHeight+1)
			}
			if got.ProcessedAt == nil || !got.HeaderSeenAt.Equal(t0) {
				t.Fatalf("milestones must survive a reactivation, got headerSeenAt=%v processedAt=%v",
					got.HeaderSeenAt, got.ProcessedAt)
			}
			// Again on the now-active row: not a transition.
			applied, err = b.ReactivateBlock(ctx, hash, blockSuiteHeight+1, time.Time{})
			if err != nil || applied {
				t.Fatalf("ReactivateBlock on an active row: applied=%v err=%v, want false", applied, err)
			}
		})

		t.Run("StaleGenerationIsNoOp", func(t *testing.T) {
			b := newBackend(t)
			const hash = "bs-react-stale"
			seedOrphan(t, b, hash, blockSuiteHeight+10)

			applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+10, t0 /* not gen1 */)
			if err != nil || applied {
				t.Fatalf("stale-generation ReactivateBlock: applied=%v err=%v, want false", applied, err)
			}
			assertOrphanedAt(t, row(t, b, hash), gen1)
		})

		t.Run("ZeroGenerationChecksStatusOnly", func(t *testing.T) {
			b := newBackend(t)
			const hash = "bs-react-zero"
			seedOrphan(t, b, hash, blockSuiteHeight+20)

			applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+20, time.Time{})
			if err != nil || !applied {
				t.Fatalf("zero-generation ReactivateBlock on an orphaned row: applied=%v err=%v, want true", applied, err)
			}
			if got := row(t, b, hash); got.Status != models.BlockStatusActive {
				t.Fatalf("status = %s, want active", got.Status)
			}
		})

		t.Run("ReorphanedRowKeepsNewerGeneration", func(t *testing.T) {
			b := newBackend(t)
			const hash = "bs-react-newer"
			seedOrphan(t, b, hash, blockSuiteHeight+30)
			// Reconciled, then orphaned again by a later reorg: the newer
			// generation is queued for its own pass.
			if ok, err := b.MarkBlockReconciled(ctx, hash, gen1, gen1.Add(10*time.Second)); err != nil || !ok {
				t.Fatalf("MarkBlockReconciled: ok=%v err=%v", ok, err)
			}
			if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, gen2); err != nil || n != 0 {
				t.Fatalf("re-orphan: transitions=%d err=%v, want 0 (already orphaned)", n, err)
			}

			// A reactivation judged against the OLD generation must not
			// clear the new one.
			applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+30, gen1)
			if err != nil || applied {
				t.Fatalf("old-generation ReactivateBlock: applied=%v err=%v, want false", applied, err)
			}
			got := row(t, b, hash)
			assertOrphanedAt(t, got, gen2)
			if got.ReconciledAt != nil {
				t.Fatalf("re-orphaning must have cleared the stamp, got %v", got.ReconciledAt)
			}
			// Judged against the new generation, it applies.
			applied, err = b.ReactivateBlock(ctx, hash, blockSuiteHeight+30, gen2)
			if err != nil || !applied {
				t.Fatalf("new-generation ReactivateBlock: applied=%v err=%v, want true", applied, err)
			}
		})

		t.Run("ActiveAndParkedRowsAreNoOps", func(t *testing.T) {
			b := newBackend(t)
			const active, parked = "bs-react-active", "bs-react-parked"
			for _, h := range []string{active, parked} {
				if err := b.UpsertBlockHeaderSeen(ctx, h, blockSuiteHeight+40, t0); err != nil {
					t.Fatalf("seed %s: %v", h, err)
				}
			}
			if err := b.MarkBlocksParked(ctx, []string{parked}); err != nil {
				t.Fatalf("park: %v", err)
			}
			for h, want := range map[string]models.BlockProcessingStatusValue{
				active: models.BlockStatusActive,
				parked: models.BlockStatusParked,
			} {
				applied, err := b.ReactivateBlock(ctx, h, blockSuiteHeight+40, time.Time{})
				if err != nil || applied {
					t.Fatalf("%s: applied=%v err=%v, want false", h, applied, err)
				}
				if got := row(t, b, h); got.Status != want {
					t.Fatalf("%s: status = %s, want %s untouched", h, got.Status, want)
				}
			}
		})

		t.Run("MissingRowIsNotCreated", func(t *testing.T) {
			b := newBackend(t)
			const hash = "bs-react-missing"
			applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+50, time.Time{})
			if err != nil || applied {
				t.Fatalf("ReactivateBlock on a missing row: applied=%v err=%v, want false", applied, err)
			}
			if _, err := b.GetBlockProcessingStatus(ctx, hash); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("a reactivation must never create a row, got err=%v", err)
			}
		})
	})

	t.Run("MarkBlocksOrphaned", func(t *testing.T) {
		t.Run("CountsAppliedTransitionsOnly", func(t *testing.T) {
			b := newBackend(t)
			const a, c = "bs-orph-a", "bs-orph-c"
			for i, h := range []string{a, c} {
				if err := b.UpsertBlockHeaderSeen(ctx, h, blockSuiteHeight+60+uint64(i), t0); err != nil {
					t.Fatalf("seed %s: %v", h, err)
				}
			}
			n, err := b.MarkBlocksOrphaned(ctx, []string{a, c, "bs-orph-missing"}, gen1)
			if err != nil || n != 2 {
				t.Fatalf("transitions = %d err=%v, want 2 (a hash with no row is not a transition)", n, err)
			}
			n, err = b.MarkBlocksOrphaned(ctx, []string{a, c}, gen2)
			if err != nil || n != 0 {
				t.Fatalf("transitions = %d err=%v, want 0 (both already orphaned)", n, err)
			}
			assertOrphanedAt(t, row(t, b, a), gen2) // the generation still refreshes

			// One row resurrected: orphaning both again is one transition.
			if applied, rerr := b.ReactivateBlock(ctx, a, blockSuiteHeight+60, time.Time{}); rerr != nil || !applied {
				t.Fatalf("ReactivateBlock: applied=%v err=%v", applied, rerr)
			}
			n, err = b.MarkBlocksOrphaned(ctx, []string{a, c}, gen2.Add(time.Minute))
			if err != nil || n != 1 {
				t.Fatalf("transitions = %d err=%v, want 1 (only the resurrected row changed status)", n, err)
			}
		})

		t.Run("ReorphanClearsStamp", func(t *testing.T) {
			b := newBackend(t)
			const hash = "bs-orph-requeue"
			seedOrphan(t, b, hash, blockSuiteHeight+70)
			if ok, err := b.MarkBlockReconciled(ctx, hash, gen1, gen1.Add(10*time.Second)); err != nil || !ok {
				t.Fatalf("MarkBlockReconciled: ok=%v err=%v", ok, err)
			}
			if got := row(t, b, hash); got.ReconciledAt == nil {
				t.Fatal("premise: the row must carry reconciled_at")
			}
			if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, gen2); err != nil || n != 0 {
				t.Fatalf("re-orphan: transitions=%d err=%v, want 0", n, err)
			}
			got := row(t, b, hash)
			assertOrphanedAt(t, got, gen2)
			if got.ReconciledAt != nil {
				t.Fatalf("re-orphaning must clear reconciled_at so the new generation is reconciled, got %v", got.ReconciledAt)
			}
		})

		t.Run("DelayedOlderGenerationDoesNotRegress", func(t *testing.T) {
			// A call carrying an older timestamp lands AFTER a newer
			// orphaning (a delayed replica, a slow reorg handler). The
			// refresh must keep the newer generation — and its stamp state —
			// so the newer token still passes the CAS writes and the older
			// one never does.
			b := newBackend(t)
			const hash = "bs-orph-delayed"
			seedOrphan(t, b, hash, blockSuiteHeight+100) // gen1
			if applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+100, gen1); err != nil || !applied {
				t.Fatalf("ReactivateBlock: applied=%v err=%v", applied, err)
			}
			if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, gen2); err != nil || n != 1 {
				t.Fatalf("newer orphaning: transitions=%d err=%v", n, err)
			}
			// The newer generation's reconciliation lands and stamps it.
			if ok, err := b.MarkBlockReconciled(ctx, hash, gen2, gen2.Add(10*time.Second)); err != nil || !ok {
				t.Fatalf("stamp gen2: ok=%v err=%v", ok, err)
			}

			// The delayed older call: no transition, and nothing changes.
			if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, gen1); err != nil || n != 0 {
				t.Fatalf("delayed older call: transitions=%d err=%v, want 0", n, err)
			}
			got := row(t, b, hash)
			assertOrphanedAt(t, got, gen2)
			if got.ReconciledAt == nil {
				t.Fatal("a delayed older call must leave the newer generation's stamp state alone")
			}
			if queued(t, b, hash) {
				t.Fatal("a delayed older call must not requeue a row whose newer generation is reconciled")
			}
			// Tokens: the older one fails both CAS writes; the newer passes.
			if ok, err := b.MarkBlockReconciled(ctx, hash, gen1, gen2.Add(time.Minute)); err != nil || ok {
				t.Fatalf("older token must not stamp: ok=%v err=%v", ok, err)
			}
			if applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+100, gen1); err != nil || applied {
				t.Fatalf("older token must not reactivate: applied=%v err=%v", applied, err)
			}
			if ok, err := b.MarkBlockReconciled(ctx, hash, gen2, gen2.Add(time.Minute)); err != nil || !ok {
				t.Fatalf("newer token must stamp: ok=%v err=%v", ok, err)
			}
			if applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+100, gen2); err != nil || !applied {
				t.Fatalf("newer token must reactivate: applied=%v err=%v", applied, err)
			}
		})

		t.Run("EqualTimestampReorphanRequeues", func(t *testing.T) {
			// The contract does not require a strictly increasing time. A
			// re-orphaning that reuses the stored timestamp is not a
			// transition (the row is already orphaned) but must still clear
			// the stamp and put the row back on the queue — otherwise a
			// reconciled row re-orphaned with an equal token would sit
			// stamped and off the queue for good.
			b := newBackend(t)
			const hash = "bs-orph-equal"
			seedOrphan(t, b, hash, blockSuiteHeight+110) // gen1
			if ok, err := b.MarkBlockReconciled(ctx, hash, gen1, gen1.Add(10*time.Second)); err != nil || !ok {
				t.Fatalf("stamp: ok=%v err=%v", ok, err)
			}
			if queued(t, b, hash) {
				t.Fatal("premise: a reconciled row is off the queue")
			}

			if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, gen1); err != nil || n != 0 {
				t.Fatalf("equal-timestamp re-orphan: transitions=%d err=%v, want 0", n, err)
			}
			got := row(t, b, hash)
			assertOrphanedAt(t, got, gen1)
			if got.ReconciledAt != nil {
				t.Fatalf("an equal-timestamp re-orphan must clear reconciled_at, got %v", got.ReconciledAt)
			}
			if !queued(t, b, hash) {
				t.Fatal("an equal-timestamp re-orphan must put the row back on the reconcile queue")
			}
			// And the (unchanged) token still works for the new pass.
			if ok, err := b.MarkBlockReconciled(ctx, hash, gen1, gen1.Add(time.Minute)); err != nil || !ok {
				t.Fatalf("the token must still stamp the requeued row: ok=%v err=%v", ok, err)
			}
		})
	})

	t.Run("GenerationSurvivesSubMillisecondSpacing", func(t *testing.T) {
		// The generation is the CAS token, so two orphanings inside one
		// millisecond must still be two generations: a reconciler holding
		// the earlier token must fail both CAS writes after the later
		// re-orphan. Microsecond spacing is the coarsest every backend keeps
		// (Postgres timestamptz); the others keep nanoseconds.
		b := newBackend(t)
		const hash = "bs-gen-submilli"
		fine1 := t0.Add(100 * time.Microsecond)
		fine2 := fine1.Add(300 * time.Microsecond) // same millisecond as fine1
		if err := b.UpsertBlockHeaderSeen(ctx, hash, blockSuiteHeight+90, t0); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, fine1); err != nil || n != 1 {
			t.Fatalf("orphan: transitions=%d err=%v", n, err)
		}
		if got := row(t, b, hash); got.OrphanedAt == nil || !got.OrphanedAt.Equal(fine1) {
			t.Fatalf("the generation must round-trip at full precision, got %v want %v", got.OrphanedAt, fine1)
		}
		// The reconciler reads fine1 as its token, then the row is
		// resurrected and orphaned again 300 µs later.
		if applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+90, fine1); err != nil || !applied {
			t.Fatalf("ReactivateBlock: applied=%v err=%v", applied, err)
		}
		if n, err := b.MarkBlocksOrphaned(ctx, []string{hash}, fine2); err != nil || n != 1 {
			t.Fatalf("re-orphan: transitions=%d err=%v", n, err)
		}
		got := row(t, b, hash)
		if got.OrphanedAt == nil || !got.OrphanedAt.Equal(fine2) || got.OrphanedAt.Equal(fine1) {
			t.Fatalf("two orphanings %v apart must be two generations, got %v", fine2.Sub(fine1), got.OrphanedAt)
		}
		if ok, err := b.MarkBlockReconciled(ctx, hash, fine1, fine2.Add(time.Second)); err != nil || ok {
			t.Fatalf("the earlier token must not stamp the later generation: ok=%v err=%v", ok, err)
		}
		if applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+90, fine1); err != nil || applied {
			t.Fatalf("the earlier token must not reactivate the later generation: applied=%v err=%v", applied, err)
		}
		if applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+90, fine2); err != nil || !applied {
			t.Fatalf("the current token must apply: applied=%v err=%v", applied, err)
		}
	})

	t.Run("MarkBlockReconciled", func(t *testing.T) {
		b := newBackend(t)
		const hash = "bs-recon"
		seedOrphan(t, b, hash, blockSuiteHeight+80)

		if ok, err := b.MarkBlockReconciled(ctx, hash, t0 /* not gen1 */, gen2); err != nil || ok {
			t.Fatalf("stale-generation stamp: ok=%v err=%v, want false", ok, err)
		}
		if got := row(t, b, hash); got.ReconciledAt != nil {
			t.Fatalf("stale stamp must not touch the row, got %v", got.ReconciledAt)
		}
		if ok, err := b.MarkBlockReconciled(ctx, hash, gen1, gen2); err != nil || !ok {
			t.Fatalf("matching-generation stamp: ok=%v err=%v, want true", ok, err)
		}
		if got := row(t, b, hash); got.ReconciledAt == nil {
			t.Fatal("row must carry reconciled_at after the stamp")
		}
		// A resurrected row is never stamped, generation or not.
		if applied, err := b.ReactivateBlock(ctx, hash, blockSuiteHeight+80, gen1); err != nil || !applied {
			t.Fatalf("ReactivateBlock: applied=%v err=%v", applied, err)
		}
		if ok, err := b.MarkBlockReconciled(ctx, hash, time.Time{}, gen2); err != nil || ok {
			t.Fatalf("stamp on a resurrected row: ok=%v err=%v, want false", ok, err)
		}
		if got := row(t, b, hash); got.Status != models.BlockStatusActive || got.ReconciledAt != nil {
			t.Fatalf("resurrected row must stay active and clean, got %+v", got)
		}
		if ok, err := b.MarkBlockReconciled(ctx, "bs-recon-missing", time.Time{}, gen2); err != nil || ok {
			t.Fatalf("stamp on a missing row: ok=%v err=%v, want false", ok, err)
		}
	})
}
