package sqlite

import (
	"bytes"
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
		_ = os.RemoveAll(tmpDir)
	}
	return dbPath, cleanup
}

func TestStore_GetOrInsertAndUpdate(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	ctx := t.Context()
	txid := "abc123"

	status1 := &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now().Add(-10 * time.Second),
	}

	result, inserted, err := store.GetOrInsertStatus(ctx, status1)
	if err != nil {
		t.Fatalf("Failed to insert status: %v", err)
	}
	if !inserted {
		t.Error("Expected inserted to be true for new transaction")
	}
	if result.Status != models.StatusReceived {
		t.Errorf("Expected status %s, got %s", models.StatusReceived, result.Status)
	}

	status2 := &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSentToNetwork,
		Timestamp: time.Now(),
	}

	if updateErr := store.UpdateStatus(ctx, status2); updateErr != nil {
		t.Fatalf("Failed to update status: %v", updateErr)
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

func TestStore_GetOrInsertStatus_Duplicate(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	ctx := t.Context()
	txid := "duplicate123"

	// First insert
	status1 := &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now(),
	}

	result1, inserted1, err := store.GetOrInsertStatus(ctx, status1)
	if err != nil {
		t.Fatalf("Failed to insert status: %v", err)
	}
	if !inserted1 {
		t.Error("Expected inserted to be true for first insert")
	}
	if result1.Status != models.StatusReceived {
		t.Errorf("Expected status %s, got %s", models.StatusReceived, result1.Status)
	}

	// Update the status to something else
	updateStatus := &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	}
	if err := store.UpdateStatus(ctx, updateStatus); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// Second insert attempt (duplicate) - should return existing status
	status2 := &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now(),
	}

	result2, inserted2, err := store.GetOrInsertStatus(ctx, status2)
	if err != nil {
		t.Fatalf("Failed on duplicate insert: %v", err)
	}
	if inserted2 {
		t.Error("Expected inserted to be false for duplicate")
	}
	// Should return the current status (SEEN_ON_NETWORK), not RECEIVED
	if result2.Status != models.StatusSeenOnNetwork {
		t.Errorf("Expected existing status %s, got %s", models.StatusSeenOnNetwork, result2.Status)
	}
}

func TestStore_GetStatusesSince(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

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
		if _, _, err := store.GetOrInsertStatus(ctx, status); err != nil {
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

//nolint:gocyclo // comprehensive test with multiple scenarios
func TestStore_WithBlockData(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	ctx := t.Context()
	txid := "mined123"
	blockHash := "00000000000000000001"
	blockHeight := uint64(800000)
	merklePath := []byte("proof123")

	// Insert the transaction status
	status := &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusReceived,
		Timestamp: time.Now(),
		ExtraInfo: "some extra data",
	}

	if _, _, err := store.GetOrInsertStatus(ctx, status); err != nil {
		t.Fatalf("Failed to insert status: %v", err)
	}

	// Insert merkle path (this is where block_height and merkle_path are stored)
	if merkErr := store.InsertMerklePath(ctx, txid, blockHash, blockHeight, merklePath); merkErr != nil {
		t.Fatalf("Failed to insert merkle path: %v", merkErr)
	}

	// Set mined status (this joins merkle_paths to transactions)
	minedStatuses, minedErr := store.SetMinedByBlockHash(ctx, blockHash)
	if minedErr != nil {
		t.Fatalf("Failed to set mined by block hash: %v", minedErr)
	}
	if len(minedStatuses) != 1 {
		t.Fatalf("Expected 1 mined status, got %d", len(minedStatuses))
	}

	retrieved, getErr := store.GetStatus(ctx, txid)
	if getErr != nil {
		t.Fatalf("Failed to get status: %v", getErr)
	}

	if retrieved.BlockHash != blockHash {
		t.Errorf("Expected block hash %s, got %s", blockHash, retrieved.BlockHash)
	}

	if retrieved.BlockHeight != blockHeight {
		t.Errorf("Expected block height %d, got %d", blockHeight, retrieved.BlockHeight)
	}

	if !bytes.Equal(retrieved.MerklePath, merklePath) {
		t.Errorf("Expected merkle path %s, got %s", merklePath, retrieved.MerklePath)
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

	if updErr := store.UpdateStatus(ctx, updateWithCompeting); updErr != nil {
		t.Fatalf("Failed to update with competing tx: %v", updErr)
	}

	retrieved, getErr2 := store.GetStatus(ctx, txid)
	if getErr2 != nil {
		t.Fatalf("Failed to get status after update: %v", getErr2)
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

	if upd2Err := store.UpdateStatus(ctx, updateWithAnotherCompeting); upd2Err != nil {
		t.Fatalf("Failed to update with second competing tx: %v", upd2Err)
	}

	retrieved, getErr3 := store.GetStatus(ctx, txid)
	if getErr3 != nil {
		t.Fatalf("Failed to get status after second update: %v", getErr3)
	}

	if len(retrieved.CompetingTxs) != 2 {
		t.Errorf("Expected 2 competing txs after second update, got %d", len(retrieved.CompetingTxs))
	}
}

func TestStore_InsertAndGetSubmission(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

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

	if insSubErr := store.InsertSubmission(ctx, sub); insSubErr != nil {
		t.Fatalf("Failed to insert submission: %v", insSubErr)
	}

	submissions, getSubErr := store.GetSubmissionsByTxID(ctx, txid)
	if getSubErr != nil {
		t.Fatalf("Failed to get submissions: %v", getSubErr)
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

func TestStore_UpdateDeliveryStatus(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

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

	if insErr2 := store.InsertSubmission(ctx, sub); insErr2 != nil {
		t.Fatalf("Failed to insert submission: %v", insErr2)
	}

	nextRetry := time.Now().Add(5 * time.Minute)
	updateErr := store.UpdateDeliveryStatus(ctx, "sub456", models.StatusSentToNetwork, 3, &nextRetry)
	if updateErr != nil {
		t.Fatalf("Failed to update delivery status: %v", updateErr)
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

func TestStore_MultipleSubmissions(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

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
		if loopErr := store.InsertSubmission(ctx, sub); loopErr != nil {
			t.Fatalf("Failed to insert submission: %v", loopErr)
		}
	}

	retrieved, retrieveErr := store.GetSubmissionsByTxID(ctx, txid)
	if retrieveErr != nil {
		t.Fatalf("Failed to get submissions: %v", retrieveErr)
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
