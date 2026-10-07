//go:build postgres

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// TestUpdateStatusReturning_AppliedResultContract exercises the atomic CTE.
// Applied is Prev != nil: that pre-image is the row this statement wrote
// over. Current is set only when the txid is known and the lattice skipped
// the write. A missing txid is ErrNotFound with both nil, and no row is
// created.
func TestUpdateStatusReturning_AppliedResultContract(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	t.Run("applied", func(t *testing.T) {
		const txid = "pg-applied-seen"
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Unix(10, 0),
		}); err != nil {
			t.Fatal(err)
		}

		res, err := s.UpdateStatusReturning(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Unix(20, 0),
		})
		if err != nil {
			t.Fatal(err)
		}
		if !res.Applied() || res.Current != nil {
			t.Fatalf("Applied=%v Prev=%+v Current=%+v, want Applied=true", res.Applied(), res.Prev, res.Current)
		}
		if res.Prev.Status != models.StatusAcceptedByNetwork {
			t.Fatalf("Previous=%s, want ACCEPTED_BY_NETWORK", res.Prev.Status)
		}
		got, err := s.GetStatus(ctx, txid)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.Status != models.StatusSeenOnNetwork {
			t.Fatalf("Current=%v, want SEEN_ON_NETWORK", got)
		}
	})

	t.Run("lattice-skip", func(t *testing.T) {
		const txid = "pg-skip-mined"
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusMined, Timestamp: time.Unix(30, 0),
		}); err != nil {
			t.Fatal(err)
		}

		res, err := s.UpdateStatusReturning(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Unix(40, 0),
		})
		if err != nil {
			t.Fatalf("lattice skip must not be an error, got %v", err)
		}
		if res.Prev != nil {
			t.Fatalf("Applied=true with stale pre-image %+v; a skipped SEEN must not look applied", res.Prev)
		}
		if res.Current == nil || res.Current.Status != models.StatusMined {
			t.Fatalf("Applied=false Current=%+v, want MINED", res.Current)
		}
		got, err := s.GetStatus(ctx, txid)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.Status != models.StatusMined {
			t.Fatalf("durable status = %v, want MINED", got)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		const txid = "pg-unknown-txid"
		res, err := s.UpdateStatusReturning(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Unix(50, 0),
		})
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("Applied=false absence = %v, want ErrNotFound", err)
		}
		if res.Prev != nil || res.Current != nil {
			t.Fatalf("unknown txid returned %+v, want an empty result", res)
		}
		got, gerr := s.GetStatus(ctx, txid)
		if gerr != nil {
			t.Fatal(gerr)
		}
		if got != nil {
			t.Fatalf("unknown txid created a row: %+v", got)
		}
	})
}
