package sqlite

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

func setupTestDB(t *testing.T) (string, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	cleanup := func() {
		os.RemoveAll(tmpDir)
	}
	return dbPath, cleanup
}

func TestStatusStore_InsertAndUpdate(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStatusStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := t.Context()
	txid := "abc123"

	status1 := &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now().Add(-10 * time.Second),
	}

	if err := store.InsertStatus(ctx, status1); err != nil {
		t.Fatalf("Failed to insert status: %v", err)
	}

	status2 := &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSentToNetwork,
		Timestamp: time.Now(),
	}

	if err := store.UpdateStatus(ctx, status2); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	current, err := store.GetStatus(ctx, txid)
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	if current == nil {
		t.Fatal("Expected current status, got nil")
	}

	if current.Status != models.StatusSentToNetwork {
		t.Errorf("Expected status %s, got %s", models.StatusSentToNetwork, current.Status)
	}
}

func TestStatusStore_GetStatusesSince(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStatusStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := t.Context()
	now := time.Now()

	statuses := []*models.TransactionStatus{
		{
			TxID:      "tx1",
			Timestamp: now.Add(-60 * time.Second),
		},
		{
			TxID:      "tx2",
			Status:    models.StatusSentToNetwork,
			Timestamp: now.Add(-30 * time.Second),
		},
		{
			TxID:      "tx3",
			Status:    models.StatusSeenOnNetwork,
			Timestamp: now.Add(-10 * time.Second),
		},
	}

	for _, status := range statuses {
		if err := store.InsertStatus(ctx, status); err != nil {
			t.Fatalf("Failed to insert status: %v", err)
		}
	}

	since := now.Add(-40 * time.Second)
	recent, err := store.GetStatusesSince(ctx, since)
	if err != nil {
		t.Fatalf("Failed to get statuses since: %v", err)
	}

	if len(recent) != 2 {
		t.Errorf("Expected 2 recent statuses, got %d", len(recent))
	}
}

func TestStatusStore_WithBlockData(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStatusStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := t.Context()
	txid := "mined123"

	status := &models.TransactionStatus{
		TxID:        txid,
		Status:      models.StatusMined,
		Timestamp:   time.Now(),
		BlockHash:   "00000000000000000001",
		BlockHeight: 800000,
		MerklePath:  "proof123",
		ExtraInfo:   "some extra data",
	}

	if err := store.InsertStatus(ctx, status); err != nil {
		t.Fatalf("Failed to insert status: %v", err)
	}

	retrieved, err := store.GetStatus(ctx, txid)
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	if retrieved.BlockHash != status.BlockHash {
		t.Errorf("Expected block hash %s, got %s", status.BlockHash, retrieved.BlockHash)
	}

	if retrieved.BlockHeight != status.BlockHeight {
		t.Errorf("Expected block height %d, got %d", status.BlockHeight, retrieved.BlockHeight)
	}

	if retrieved.MerklePath != status.MerklePath {
		t.Errorf("Expected merkle path %s, got %s", status.MerklePath, retrieved.MerklePath)
	}

	if len(retrieved.CompetingTxs) != 0 {
		t.Errorf("Expected 0 competing txs on fresh insert, got %d", len(retrieved.CompetingTxs))
	}

	updateWithCompeting := &models.TransactionStatus{
		TxID:         txid,
		Status:       models.StatusDoubleSpendAttempted,
		Timestamp:    time.Now(),
		CompetingTxs: []string{"competitor1"},
	}

	if err := store.UpdateStatus(ctx, updateWithCompeting); err != nil {
		t.Fatalf("Failed to update with competing tx: %v", err)
	}

	retrieved, err = store.GetStatus(ctx, txid)
	if err != nil {
		t.Fatalf("Failed to get status after update: %v", err)
	}

	if len(retrieved.CompetingTxs) != 1 {
		t.Errorf("Expected 1 competing tx after first update, got %d", len(retrieved.CompetingTxs))
	}

	updateWithAnotherCompeting := &models.TransactionStatus{
		TxID:         txid,
		Status:       models.StatusDoubleSpendAttempted,
		Timestamp:    time.Now(),
		CompetingTxs: []string{"competitor2"},
	}

	if err := store.UpdateStatus(ctx, updateWithAnotherCompeting); err != nil {
		t.Fatalf("Failed to update with second competing tx: %v", err)
	}

	retrieved, err = store.GetStatus(ctx, txid)
	if err != nil {
		t.Fatalf("Failed to get status after second update: %v", err)
	}

	if len(retrieved.CompetingTxs) != 2 {
		t.Errorf("Expected 2 competing txs after second update, got %d", len(retrieved.CompetingTxs))
	}
}

func TestSubmissionStore_InsertAndGet(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewSubmissionStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := t.Context()
	txid := "tx456"

	sub := &models.Submission{
		SubmissionID:      "sub123",
		TxID:              txid,
		CallbackURL:       "https://example.com/callback",
		CallbackToken:     "secret123",
		FullStatusUpdates: false,
		RetryCount:        0,
		CreatedAt:         time.Now(),
	}

	if err := store.InsertSubmission(ctx, sub); err != nil {
		t.Fatalf("Failed to insert submission: %v", err)
	}

	submissions, err := store.GetSubmissionsByTxID(ctx, txid)
	if err != nil {
		t.Fatalf("Failed to get submissions: %v", err)
	}

	if len(submissions) != 1 {
		t.Errorf("Expected 1 submission, got %d", len(submissions))
	}

	retrieved := submissions[0]
	if retrieved.SubmissionID != sub.SubmissionID {
		t.Errorf("Expected submission ID %s, got %s", sub.SubmissionID, retrieved.SubmissionID)
	}

	if retrieved.CallbackURL != sub.CallbackURL {
		t.Errorf("Expected callback URL %s, got %s", sub.CallbackURL, retrieved.CallbackURL)
	}

	if retrieved.CallbackToken != sub.CallbackToken {
		t.Errorf("Expected callback token %s, got %s", sub.CallbackToken, retrieved.CallbackToken)
	}
}

func TestSubmissionStore_UpdateDeliveryStatus(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewSubmissionStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := t.Context()

	sub := &models.Submission{
		SubmissionID:      "sub456",
		TxID:              "tx789",
		CallbackURL:       "https://example.com",
		CallbackToken:     "",
		FullStatusUpdates: false,
		RetryCount:        0,
		CreatedAt:         time.Now(),
	}

	if err := store.InsertSubmission(ctx, sub); err != nil {
		t.Fatalf("Failed to insert submission: %v", err)
	}

	nextRetry := time.Now().Add(5 * time.Minute)
	err = store.UpdateDeliveryStatus(ctx, "sub456", models.StatusSentToNetwork, 3, &nextRetry)
	if err != nil {
		t.Fatalf("Failed to update delivery status: %v", err)
	}

	submissions, err := store.GetSubmissionsByTxID(ctx, "tx789")
	if err != nil {
		t.Fatalf("Failed to get submissions: %v", err)
	}

	if len(submissions) != 1 {
		t.Fatal("Expected 1 submission")
	}

	updated := submissions[0]
	if updated.LastDeliveredStatus != models.StatusSentToNetwork {
		t.Errorf("Expected last delivered status %s, got %s", models.StatusSentToNetwork, updated.LastDeliveredStatus)
	}

	if updated.RetryCount != 3 {
		t.Errorf("Expected retry count 3, got %d", updated.RetryCount)
	}

	if updated.NextRetryAt == nil {
		t.Fatal("Expected next retry at to be set")
	}
}

func TestSubmissionStore_MultipleSubmissions(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewSubmissionStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := t.Context()
	txid := "tx_multi"

	submissions := []*models.Submission{
		{
			SubmissionID:      "sub1",
			TxID:              txid,
			CallbackURL:       "https://webhook1.com",
			CallbackToken:     "token1",
			FullStatusUpdates: false,
			CreatedAt:         time.Now(),
		},
		{
			SubmissionID:      "sub2",
			TxID:              txid,
			CallbackURL:       "https://webhook2.com",
			CallbackToken:     "token2",
			FullStatusUpdates: true,
		},
		{
			SubmissionID:      "sub3",
			TxID:              txid,
			CallbackURL:       "",
			CallbackToken:     "sse_token",
			FullStatusUpdates: false,
			CreatedAt:         time.Now(),
		},
	}

	for _, sub := range submissions {
		if err := store.InsertSubmission(ctx, sub); err != nil {
			t.Fatalf("Failed to insert submission: %v", err)
		}
	}

	retrieved, err := store.GetSubmissionsByTxID(ctx, txid)
	if err != nil {
		t.Fatalf("Failed to get submissions: %v", err)
	}

	if len(retrieved) != 3 {
		t.Errorf("Expected 3 submissions, got %d", len(retrieved))
	}

	urls := make(map[string]bool)
	for _, sub := range retrieved {
		if sub.CallbackURL != "" {
			urls[sub.CallbackURL] = true
		}
	}

	if !urls["https://webhook1.com"] || !urls["https://webhook2.com"] {
		t.Error("Not all callback URLs were retrieved correctly")
	}
}
