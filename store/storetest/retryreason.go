package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// RetryReasonBackend is the slice of store.Store the retry-reason suite drives:
// the durable PENDING_RETRY queue.
type RetryReasonBackend interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error
	SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time, lastReason string) error
	GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error)
	ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error
}

// RunRetryReasonSuite asserts the retry_reason contract: GetReadyRetries
// returns the reason SetPendingRetryFields last wrote, and every
// SetPendingRetryFields replaces it — an empty reason clears it. Carrying a
// reason across attempts is the reaper's job (it re-passes what
// GetReadyRetries returned); the store never merges, so a tx that leaves the
// queue by ANY path (a plain status update to ACCEPTED, a ClearRetryState) and
// is later parked afresh starts with no reason rather than quoting a response
// from its previous stay.
//
// The txid carries a per-run prefix and the suite reads back only its own row,
// so it is safe in a namespace shared with other integration tests.
func RunRetryReasonSuite(t *testing.T, newBackend func(t *testing.T) RetryReasonBackend) {
	t.Helper()
	b := newBackend(t)
	ctx := context.Background()
	txid := fmt.Sprintf("%048x%016x", time.Now().UnixNano(), 0x7e)
	raw := []byte{0x01, 0x02}
	due := time.Now().Add(-time.Second)

	if _, _, err := b.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	read := func(step string) string {
		t.Helper()
		ready, err := b.GetReadyRetries(ctx, time.Now(), 10_000)
		if err != nil {
			t.Fatalf("%s: GetReadyRetries: %v", step, err)
		}
		for _, r := range ready {
			if r.TxID == txid {
				return r.LastReason
			}
		}
		t.Fatalf("%s: parked row %s not returned by GetReadyRetries", step, txid)
		return ""
	}

	const first = "PROCESSING (4): [ProcessTransaction][x] failed to validate transaction"
	const second = "UTXO_SPENT (70): x:0 utxo already spent by tx y[0]"

	if err := b.SetPendingRetryFields(ctx, txid, raw, due, first); err != nil {
		t.Fatalf("park: %v", err)
	}
	if got := read("after park"); got != first {
		t.Fatalf("LastReason = %q, want %q", got, first)
	}

	if err := b.SetPendingRetryFields(ctx, txid, raw, due, second); err != nil {
		t.Fatalf("reschedule with reason: %v", err)
	}
	if got := read("after reschedule with reason"); got != second {
		t.Fatalf("LastReason = %q, want the newer %q", got, second)
	}

	// Leave the queue the way an accepted rebroadcast does — a plain status
	// update, not ClearRetryState — then park afresh having heard nothing.
	// ACCEPTED_BY_NETWORK → PENDING_RETRY is allowed by the lattice.
	if err := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()}); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if err := b.SetPendingRetryFields(ctx, txid, raw, due, ""); err != nil {
		t.Fatalf("re-park after accept: %v", err)
	}
	if got := read("after accept and re-park"); got != "" {
		t.Fatalf("LastReason = %q after leaving the queue and re-parking with none, want empty", got)
	}

	// Same through ClearRetryState.
	if err := b.SetPendingRetryFields(ctx, txid, raw, due, first); err != nil {
		t.Fatalf("park again: %v", err)
	}
	if err := b.ClearRetryState(ctx, txid, models.StatusAcceptedByNetwork, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if err := b.SetPendingRetryFields(ctx, txid, raw, due, ""); err != nil {
		t.Fatalf("re-park after clear: %v", err)
	}
	if got := read("after clear and re-park"); got != "" {
		t.Fatalf("LastReason = %q after ClearRetryState and re-parking with none, want empty", got)
	}
}
