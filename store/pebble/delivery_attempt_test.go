package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// TestRecordDeliveryAttempt_RoundTrip pins the delivery-attempt bookkeeping
// (issue #249): attempts accumulates across POSTs, last-attempt fields are
// overwritten each time, the retry-scheduling fields are untouched, and an
// unknown submission is a silent no-op (never a phantom row).
func TestRecordDeliveryAttempt_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	sub := &models.Submission{
		SubmissionID:  "sub-attempts",
		TxID:          "aa11",
		CallbackURL:   "https://receiver.example/cb",
		CallbackToken: "tok",
	}
	if err := s.InsertSubmission(ctx, sub); err != nil {
		t.Fatalf("InsertSubmission: %v", err)
	}

	t1 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	if err := s.RecordDeliveryAttempt(ctx, sub.SubmissionID, t1, "status 403"); err != nil {
		t.Fatalf("first attempt: %v", err)
	}
	t2 := t1.Add(time.Minute)
	if err := s.RecordDeliveryAttempt(ctx, sub.SubmissionID, t2, "delivered"); err != nil {
		t.Fatalf("second attempt: %v", err)
	}

	subs, err := s.GetSubmissionsByTxID(ctx, sub.TxID)
	if err != nil || len(subs) != 1 {
		t.Fatalf("GetSubmissionsByTxID: %v (%d subs)", err, len(subs))
	}
	got := subs[0]
	if got.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", got.Attempts)
	}
	if got.LastResult != "delivered" {
		t.Errorf("LastResult = %q, want delivered", got.LastResult)
	}
	if got.LastAttemptAt == nil || !got.LastAttemptAt.Equal(t2) {
		t.Errorf("LastAttemptAt = %v, want %v", got.LastAttemptAt, t2)
	}
	if got.RetryCount != 0 || got.NextRetryAt != nil {
		t.Errorf("retry-scheduling state must be untouched: %+v", got)
	}

	// Unknown submission: no error, no phantom row.
	if err := s.RecordDeliveryAttempt(ctx, "sub-missing", t1, "status 500"); err != nil {
		t.Fatalf("missing submission should no-op, got %v", err)
	}
}
