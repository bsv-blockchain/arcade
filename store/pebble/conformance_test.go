package pebble

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/storetest"
)

// Pebble is the only backend whose conformance run is untagged, so this is the
// one that actually gates CI. Keep it that way: postgres and aerospike are
// behind //go:build tags that no workflow currently passes.
func TestIterateTrackerRows_Conformance(t *testing.T) {
	storetest.RunTrackerRowsSuite(t, func(t *testing.T) storetest.Backend {
		t.Helper()
		return newTestStore(t)
	})
}

func TestIterateTrackerRows_StatusIndexStaysFresh(t *testing.T) {
	// The walk is driven by idx:tx:status:<status>:*, so a status transition
	// must move the row out of the old status's index. If it did not, a
	// REJECTED row would keep being emitted under its old RECEIVED key —
	// exactly the leak issue #276 exists to prevent.
	s := newTestStore(t)
	ctx := context.Background()

	const txid = "00000000000000000000000000000000000000000000000000000000000000aa"
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID:   txid,
		Status: models.StatusReceived,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID:   txid,
		Status: models.StatusRejected,
	}); err != nil {
		t.Fatalf("update to REJECTED: %v", err)
	}

	scan := store.NewTrackerScan(900_000, true)
	var emitted []store.TrackerRow
	if err := s.IterateTrackerRows(ctx, scan, func(row store.TrackerRow) error {
		emitted = append(emitted, row)
		return nil
	}); err != nil {
		t.Fatalf("IterateTrackerRows: %v", err)
	}
	for _, row := range emitted {
		if row.TxID == txid {
			t.Fatalf("row emitted as %s after transitioning to REJECTED", row.Status)
		}
	}
}
