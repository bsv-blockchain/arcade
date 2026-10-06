package storetest

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// CompetingTxsBackend is the slice of store.Store the competing-txs suite
// drives: the status writes the propagator terminalizes through, and the read
// GET /tx serves from.
type CompetingTxsBackend interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error
	BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error
	BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]store.StatusUpdate, error)
	GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)
}

// RunCompetingTxsSuite asserts that a status UPDATE carrying CompetingTxs
// persists them, through every update entry point. A double spend is only
// discovered after the row exists (the network verdict arrives at
// propagation, long after intake inserted the row), so a backend that writes
// competing_txs on insert alone never surfaces the competing spender on
// GET /tx. An update with no CompetingTxs must leave stored ones alone, the
// same partial-update rule every other optional column follows.
//
// Txids carry a per-run prefix so the suite is safe in a namespace shared with
// other integration tests. newBackend is called once per subtest.
func RunCompetingTxsSuite(t *testing.T, newBackend func(t *testing.T) CompetingTxsBackend) {
	t.Helper()

	spender := fmt.Sprintf("%064x", 0x212b)
	other := fmt.Sprintf("%064x", 0x6c57)
	prefix := time.Now().UnixNano()

	updates := map[string]func(ctx context.Context, b CompetingTxsBackend, st *models.TransactionStatus) error{
		"UpdateStatus": func(ctx context.Context, b CompetingTxsBackend, st *models.TransactionStatus) error {
			return b.UpdateStatus(ctx, st)
		},
		"BatchUpdateStatus": func(ctx context.Context, b CompetingTxsBackend, st *models.TransactionStatus) error {
			return b.BatchUpdateStatus(ctx, []*models.TransactionStatus{st})
		},
		"BatchUpdateStatusReturning": func(ctx context.Context, b CompetingTxsBackend, st *models.TransactionStatus) error {
			_, err := b.BatchUpdateStatusReturning(ctx, []*models.TransactionStatus{st})
			return err
		},
	}
	names := make([]string, 0, len(updates))
	for name := range updates {
		names = append(names, name)
	}
	slices.Sort(names)

	for i, name := range names {
		update := updates[name]
		t.Run(name, func(t *testing.T) {
			b := newBackend(t)
			ctx := context.Background()
			txid := fmt.Sprintf("%048x%016x", prefix, i+1)

			if _, _, err := b.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID:   txid,
				Status: models.StatusPendingRetry,
			}); err != nil {
				t.Fatalf("insert: %v", err)
			}

			want := []string{spender, other}
			if err := update(ctx, b, &models.TransactionStatus{
				TxID:         txid,
				Status:       models.StatusRejected,
				StatusCode:   466,
				ExtraInfo:    "UTXO_SPENT (70): already spent by tx " + spender + "[0]",
				CompetingTxs: want,
				Timestamp:    time.Now(),
			}); err != nil {
				t.Fatalf("update to REJECTED: %v", err)
			}
			got, err := b.GetStatus(ctx, txid)
			if err != nil || got == nil {
				t.Fatalf("GetStatus = %v, %v", got, err)
			}
			if got.Status != models.StatusRejected || got.StatusCode != 466 {
				t.Fatalf("status = %s/%d, want REJECTED/466", got.Status, got.StatusCode)
			}
			if !slices.Equal(got.CompetingTxs, want) {
				t.Fatalf("CompetingTxs = %v, want %v", got.CompetingTxs, want)
			}

			// A later write without CompetingTxs (a lattice-refused SEEN
			// callback, a retry-state touch) must not erase them.
			if err = update(ctx, b, &models.TransactionStatus{
				TxID:      txid,
				Status:    models.StatusRejected,
				Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("second update: %v", err)
			}
			got, err = b.GetStatus(ctx, txid)
			if err != nil || got == nil {
				t.Fatalf("GetStatus after second update = %v, %v", got, err)
			}
			if !slices.Equal(got.CompetingTxs, want) {
				t.Fatalf("CompetingTxs after update without them = %v, want %v kept", got.CompetingTxs, want)
			}
		})
	}
}
