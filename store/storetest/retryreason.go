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
	SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time, lastReason string) error
	GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error)
	ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error
}

// RunRetryReasonSuite asserts that the last network response recorded for a
// parked tx survives across durable attempts: GetReadyRetries returns it, a
// reschedule with an empty reason keeps it, a non-empty one replaces it, and
// ClearRetryState drops it so a later re-park starts clean. Without this the
// reaper's give-up reason depends only on the final attempt, which on a
// transport failure says "no peer ever answered" even when every earlier
// attempt drew a concrete Teranode line.
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

	if err := b.SetPendingRetryFields(ctx, txid, raw, due, ""); err != nil {
		t.Fatalf("reschedule without reason: %v", err)
	}
	if got := read("after reschedule without reason"); got != first {
		t.Fatalf("LastReason = %q after an attempt that heard nothing, want %q kept", got, first)
	}

	if err := b.SetPendingRetryFields(ctx, txid, raw, due, second); err != nil {
		t.Fatalf("reschedule with reason: %v", err)
	}
	if got := read("after reschedule with reason"); got != second {
		t.Fatalf("LastReason = %q, want the newer %q", got, second)
	}

	if err := b.ClearRetryState(ctx, txid, models.StatusAcceptedByNetwork, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	// A reorg-style re-entry to the queue must not inherit the old reason.
	// ACCEPTED_BY_NETWORK → PENDING_RETRY is allowed by the lattice.
	if err := b.SetPendingRetryFields(ctx, txid, raw, due, ""); err != nil {
		t.Fatalf("re-park: %v", err)
	}
	if got := read("after clear and re-park"); got != "" {
		t.Fatalf("LastReason = %q after ClearRetryState, want empty", got)
	}
}
