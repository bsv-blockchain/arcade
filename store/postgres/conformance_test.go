//go:build postgres

package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/storetest"
)

func TestIterateTrackerRows_Conformance(t *testing.T) {
	storetest.RunTrackerRowsSuite(t, func(t *testing.T) storetest.Backend {
		t.Helper()
		return newTestStore(t)
	})
}

func TestIterateTrackerRows_NullBlockHeightKept(t *testing.T) {
	// block_height is nullable (schema.sql). A MINED row with a NULL height
	// must be kept at any cutoff — the COALESCE disjunct in the query is what
	// makes that true, and a naive `block_height >= $2` would silently drop it
	// because NULL comparisons are never true.
	s := newTestStore(t)
	ctx := context.Background()

	const txid = "00000000000000000000000000000000000000000000000000000000000000bb"
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID:   txid,
		Status: models.StatusMined,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE transactions SET block_height = NULL WHERE txid = $1`, txid); err != nil {
		t.Fatalf("null out block_height: %v", err)
	}

	found := false
	if err := s.IterateTrackerRows(ctx, store.NewTrackerScan(900_000, true), func(row store.TrackerRow) error {
		if row.TxID == txid {
			found = true
			if row.BlockHeight != 0 {
				t.Errorf("NULL block_height should surface as 0, got %d", row.BlockHeight)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("IterateTrackerRows: %v", err)
	}
	if !found {
		t.Fatal("MINED row with NULL block_height was pruned")
	}
}

func TestIterateTrackerRows_PlanHasNoSort(t *testing.T) {
	// The regression guard for issue #276. The query hydration used to run
	// carried ORDER BY timestamp_at DESC, so on a multi-million-row store the
	// planner added an external merge sort over 14 detoasted columns — that
	// sort is what drove arcade and its Postgres out of memory. Hydration
	// builds a map, so any ordering in this plan is pure waste.
	s := newTestStore(t)
	ctx := context.Background()

	statuses := store.TrackerStatuses()
	names := make([]string, len(statuses))
	for i, st := range statuses {
		names[i] = string(st)
	}

	const q = `
EXPLAIN (VERBOSE)
SELECT txid, status, COALESCE(block_height, 0)
FROM transactions
WHERE status = ANY($1)
  AND (status <> $2 OR COALESCE(block_height, 0) = 0 OR block_height >= $3)`

	rows, err := s.pool.Query(ctx, q, names, string(models.StatusMined), int64(899_901))
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}

	got := plan.String()
	if strings.Contains(got, "Sort") {
		t.Errorf("hydration query plan contains a Sort node (issue #276 regression):\n%s", got)
	}
	// The projection must not pull the heavy columns.
	for _, col := range []string{"raw_tx", "merkle_path", "competing_txs", "orphaned_anchors"} {
		if strings.Contains(got, col) {
			t.Errorf("hydration query plan references heavy column %q:\n%s", col, got)
		}
	}
}
