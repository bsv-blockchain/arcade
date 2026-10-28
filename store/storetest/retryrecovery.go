package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// RetryRecoveryBackend is the slice of store.Store the retry-recovery suite
// drives: the durable PENDING_RETRY queue and the writes that leave it.
type RetryRecoveryBackend interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error
	BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error
	GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)
	SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time, lastReason string) error
	GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error)
	SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error)
}

// RunRetryRecoverySuite asserts that a positive network verdict retires a
// row's retry bookkeeping (models.Status.ClearsRetryState): once a parked
// transaction is ACCEPTED_BY_NETWORK, SEEN_* or MINED, GET /tx must show
// neither the park reason in extraInfo nor a nextRetryAt, and the reaper must
// not see it as ready. Before this contract a transaction that recovered from
// PENDING_RETRY carried "parent not yet accepted … retryable" and a retry
// schedule all the way to MINED (hosted-service report, issue 3).
//
// Txids carry a per-run prefix and the suite reads back only its own rows, so
// it is safe in a namespace shared with other integration tests.
func RunRetryRecoverySuite(t *testing.T, newBackend func(t *testing.T) RetryRecoveryBackend) {
	t.Helper()
	b := newBackend(t)
	ctx := context.Background()

	const parkReason = "parent not yet accepted by the network after 5 propagation attempts: retryable — parked for durable rebroadcast by the propagation reaper; last error: TX_MISSING_PARENT (34)"
	const netLine = "TX_MISSING_PARENT (34): parent missing"
	base := time.Now().UnixNano()
	newTxid := func(tag byte) string { return fmt.Sprintf("%048x%016x", base, tag) }

	get := func(t *testing.T, txid string) *models.TransactionStatus {
		t.Helper()
		row, err := b.GetStatus(ctx, txid)
		if err != nil {
			t.Fatalf("GetStatus: %v", err)
		}
		if row == nil {
			t.Fatalf("GetStatus: row %s missing", txid)
		}
		return row
	}
	ready := func(t *testing.T, txid string) bool {
		t.Helper()
		rows, err := b.GetReadyRetries(ctx, time.Now(), 10_000)
		if err != nil {
			t.Fatalf("GetReadyRetries: %v", err)
		}
		for _, r := range rows {
			if r.TxID == txid {
				return true
			}
		}
		return false
	}
	// park takes a fresh row the way the propagator does: a PENDING_RETRY
	// status write carrying the park reason, then the retry bins.
	park := func(t *testing.T, txid string) {
		t.Helper()
		if _, _, err := b.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived, Timestamp: time.Now()}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if err := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusPendingRetry, ExtraInfo: parkReason, Timestamp: time.Now()}); err != nil {
			t.Fatalf("park status: %v", err)
		}
		if err := b.SetPendingRetryFields(ctx, txid, []byte{0x01, 0x02}, time.Now().Add(-time.Second), netLine); err != nil {
			t.Fatalf("park fields: %v", err)
		}
		row := get(t, txid)
		if row.Status != models.StatusPendingRetry || row.ExtraInfo != parkReason || row.NextRetryAt.IsZero() {
			t.Fatalf("park precondition not met: status=%s extraInfo=%q nextRetryAt=%v", row.Status, row.ExtraInfo, row.NextRetryAt)
		}
		if !ready(t, txid) {
			t.Fatalf("parked row %s not returned by GetReadyRetries", txid)
		}
	}
	assertCleared := func(t *testing.T, step, txid string, wantStatus models.Status, wantExtra string) {
		t.Helper()
		row := get(t, txid)
		if row.Status != wantStatus {
			t.Errorf("%s: status = %s, want %s", step, row.Status, wantStatus)
		}
		if row.ExtraInfo != wantExtra {
			t.Errorf("%s: extraInfo = %q, want %q", step, row.ExtraInfo, wantExtra)
		}
		if !row.NextRetryAt.IsZero() {
			t.Errorf("%s: nextRetryAt = %v, want zero", step, row.NextRetryAt)
		}
		if ready(t, txid) {
			t.Errorf("%s: row still returned by GetReadyRetries", step)
		}
	}

	t.Run("accepted clears the park", func(t *testing.T) {
		txid := newTxid(0xa1)
		park(t, txid)
		if err := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()}); err != nil {
			t.Fatalf("accept: %v", err)
		}
		assertCleared(t, "ACCEPTED_BY_NETWORK", txid, models.StatusAcceptedByNetwork, "")
	})

	t.Run("batch accepted clears the park", func(t *testing.T) {
		txid := newTxid(0xa2)
		park(t, txid)
		if err := b.BatchUpdateStatus(ctx, []*models.TransactionStatus{{TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()}}); err != nil {
			t.Fatalf("batch accept: %v", err)
		}
		assertCleared(t, "batch ACCEPTED_BY_NETWORK", txid, models.StatusAcceptedByNetwork, "")
	})

	t.Run("mined clears the park", func(t *testing.T) {
		txid := newTxid(0xa3)
		park(t, txid)
		if _, _, err := b.SetMinedByTxIDs(ctx, fmt.Sprintf("%064x", base), 10, []string{txid}); err != nil {
			t.Fatalf("mine: %v", err)
		}
		assertCleared(t, "MINED", txid, models.StatusMined, "")
	})

	t.Run("seen clears a rejection reason", func(t *testing.T) {
		// SEEN_ON_NETWORK may recover a REJECTED tx (a peer accepted after
		// another refused); the stale rejection must not survive it.
		txid := newTxid(0xa4)
		if _, _, err := b.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived, Timestamp: time.Now()}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if err := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusRejected, StatusCode: 466, ExtraInfo: "UTXO_SPENT (70): already spent", Timestamp: time.Now()}); err != nil {
			t.Fatalf("reject: %v", err)
		}
		if err := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Now()}); err != nil {
			t.Fatalf("seen: %v", err)
		}
		assertCleared(t, "SEEN_ON_NETWORK", txid, models.StatusSeenOnNetwork, "")
	})

	t.Run("a positive write keeps the extra info it carries", func(t *testing.T) {
		txid := newTxid(0xa5)
		park(t, txid)
		if err := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusAcceptedByNetwork, ExtraInfo: "note", Timestamp: time.Now()}); err != nil {
			t.Fatalf("accept with note: %v", err)
		}
		assertCleared(t, "ACCEPTED_BY_NETWORK with note", txid, models.StatusAcceptedByNetwork, "note")
	})

	t.Run("a negative write keeps the retry fields", func(t *testing.T) {
		// Only the give-up path (ClearRetryState) retires the queue on the way
		// to REJECTED; a plain REJECTED write replaces the reason and nothing else.
		txid := newTxid(0xa6)
		park(t, txid)
		if err := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusRejected, ExtraInfo: "giving up", Timestamp: time.Now()}); err != nil {
			t.Fatalf("reject: %v", err)
		}
		row := get(t, txid)
		if row.Status != models.StatusRejected || row.ExtraInfo != "giving up" {
			t.Errorf("REJECTED: status=%s extraInfo=%q, want REJECTED/\"giving up\"", row.Status, row.ExtraInfo)
		}
		if row.NextRetryAt.IsZero() {
			t.Errorf("REJECTED: nextRetryAt cleared by a non-positive write")
		}
	})
}
