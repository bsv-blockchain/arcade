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

// The generation-checked block_processing writes (issue #339): ReactivateBlock,
// MarkBlocksOrphaned's applied-transition count, MarkBlockReconciled's CAS.
func TestBlockStatus_Conformance(t *testing.T) {
	storetest.RunBlockStatusSuite(t, func(t *testing.T) storetest.BlockStatusBackend {
		t.Helper()
		return newTestStore(t)
	})
}

// STUMP rows are content-addressed so several merkle-services can deliver
// the same subtree: identical bytes collapse, divergent variants coexist.
func TestStumps_Conformance(t *testing.T) {
	storetest.RunStumpSuite(t, func(t *testing.T) storetest.StumpBackend {
		t.Helper()
		return newTestStore(t)
	})
}

// A double spend is discovered after the row exists, so the competing
// spender must persist through the status-update paths, not only on insert.
func TestCompetingTxs_Conformance(t *testing.T) {
	storetest.RunCompetingTxsSuite(t, func(t *testing.T) storetest.CompetingTxsBackend {
		t.Helper()
		return newTestStore(t)
	})
}

// The last network response must survive across durable retry attempts so
// the reaper's give-up reason can quote it.
func TestRetryReason_Conformance(t *testing.T) {
	storetest.RunRetryReasonSuite(t, func(t *testing.T) storetest.RetryReasonBackend {
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
