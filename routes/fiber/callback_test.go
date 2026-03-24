package fiber

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gofiber/fiber/v2"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// mockStore implements store.Store for testing. Only the methods used by
// handleStump are fully implemented; the rest panic to catch accidental use.
type mockStore struct {
	store.Store
	updateStatusFn func(ctx context.Context, status *models.TransactionStatus) error
	insertStumpFn  func(ctx context.Context, stump *models.Stump) error
}

func (m *mockStore) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, status)
	}
	return nil
}

func (m *mockStore) InsertStump(ctx context.Context, stump *models.Stump) error {
	if m.insertStumpFn != nil {
		return m.insertStumpFn(ctx, stump)
	}
	return nil
}

// mockPublisher implements events.Publisher for testing.
type mockPublisher struct {
	events.Publisher
	published []*models.TransactionStatus
}

func (m *mockPublisher) Publish(_ context.Context, status *models.TransactionStatus) error {
	m.published = append(m.published, status)
	return nil
}

func newTestApp(h *CallbackHandler) *fiber.App {
	app := fiber.New()
	h.Register(app)
	return app
}

func postCallback(app *fiber.App, msg models.CallbackMessage) (*http.Response, error) {
	body, _ := json.Marshal(msg)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return app.Test(req)
}

// buildTestStumpBinary creates a minimal valid BRC-74 binary STUMP containing
// a single level-0 leaf with the given txid hash marked as the tracked tx.
func buildTestStumpBinary(txHash *chainhash.Hash) []byte {
	isTxid := true
	mp := &transaction.MerklePath{
		BlockHeight: 100,
		Path: [][]*transaction.PathElement{
			{
				{Offset: 0, Hash: txHash, Txid: &isTxid},
			},
		},
	}
	return mp.Bytes()
}

func TestHandleStump_UpdatesStatusToMined(t *testing.T) {
	var updatedStatuses []*models.TransactionStatus
	var storedStump *models.Stump

	ms := &mockStore{
		updateStatusFn: func(_ context.Context, status *models.TransactionStatus) error {
			updatedStatuses = append(updatedStatuses, status)
			return nil
		},
		insertStumpFn: func(_ context.Context, stump *models.Stump) error {
			storedStump = stump
			return nil
		},
	}
	pub := &mockPublisher{}

	// Create a TxTracker and register a txid
	tracker := store.NewTxTracker()
	txHash, _ := chainhash.NewHashFromHex("abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1")
	tracker.AddHash(*txHash, models.StatusSeenOnNetwork)

	handler := NewCallbackHandler(CallbackConfig{
		Store:          ms,
		EventPublisher: pub,
		TxTracker:      tracker,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	stumpBinary := buildTestStumpBinary(txHash)

	app := newTestApp(handler)
	msg := models.CallbackMessage{
		Type:         models.CallbackStump,
		BlockHash:    "block456",
		SubtreeIndex: 1,
		Stump:        models.HexBytes(stumpBinary),
	}

	resp, err := postCallback(app, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify status was updated to MINED for the tracked txid
	if len(updatedStatuses) != 1 {
		t.Fatalf("expected 1 UpdateStatus call, got %d", len(updatedStatuses))
	}
	if updatedStatuses[0].Status != models.StatusMined {
		t.Errorf("expected status MINED, got %s", updatedStatuses[0].Status)
	}
	if updatedStatuses[0].TxID != txHash.String() {
		t.Errorf("expected txid %s, got %s", txHash.String(), updatedStatuses[0].TxID)
	}
	if updatedStatuses[0].BlockHash != "block456" {
		t.Errorf("expected blockHash block456, got %s", updatedStatuses[0].BlockHash)
	}

	// Verify event was published
	if len(pub.published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(pub.published))
	}
	if pub.published[0].Status != models.StatusMined {
		t.Errorf("expected published status MINED, got %s", pub.published[0].Status)
	}

	// Verify STUMP was stored by (blockHash, subtreeIndex)
	if storedStump == nil {
		t.Fatal("expected InsertStump to be called")
	}
	if storedStump.BlockHash != "block456" {
		t.Errorf("expected stump blockHash block456, got %s", storedStump.BlockHash)
	}
	if storedStump.SubtreeIndex != 1 {
		t.Errorf("expected stump subtreeIndex 1, got %d", storedStump.SubtreeIndex)
	}
}

func TestHandleStump_NoTrackedTxs_StillStoresStump(t *testing.T) {
	var storedStump *models.Stump

	ms := &mockStore{
		insertStumpFn: func(_ context.Context, stump *models.Stump) error {
			storedStump = stump
			return nil
		},
	}
	pub := &mockPublisher{}

	// Empty tracker — no tracked txs
	tracker := store.NewTxTracker()

	txHash, _ := chainhash.NewHashFromHex("abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1")
	stumpBinary := buildTestStumpBinary(txHash)

	handler := NewCallbackHandler(CallbackConfig{
		Store:          ms,
		EventPublisher: pub,
		TxTracker:      tracker,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	app := newTestApp(handler)
	msg := models.CallbackMessage{
		Type:         models.CallbackStump,
		BlockHash:    "block789",
		SubtreeIndex: 0,
		Stump:        models.HexBytes(stumpBinary),
	}

	resp, err := postCallback(app, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// No status updates since no tracked txs
	if len(pub.published) != 0 {
		t.Errorf("expected no published events, got %d", len(pub.published))
	}

	// STUMP should still be stored
	if storedStump == nil {
		t.Fatal("expected InsertStump to be called")
	}
	if storedStump.BlockHash != "block789" {
		t.Errorf("expected stump blockHash block789, got %s", storedStump.BlockHash)
	}
}

func TestHandleSeenOnNetwork_BatchedTxIDs(t *testing.T) {
	var updatedTxIDs []string
	ms := &mockStore{
		updateStatusFn: func(_ context.Context, status *models.TransactionStatus) error {
			updatedTxIDs = append(updatedTxIDs, status.TxID)
			return nil
		},
	}
	pub := &mockPublisher{}
	tracker := store.NewTxTracker()

	handler := NewCallbackHandler(CallbackConfig{
		Store:          ms,
		EventPublisher: pub,
		TxTracker:      tracker,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	app := newTestApp(handler)
	msg := models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxIDs: []string{"aaa", "bbb", "ccc"},
	}

	resp, err := postCallback(app, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if len(updatedTxIDs) != 3 {
		t.Fatalf("expected 3 UpdateStatus calls, got %d", len(updatedTxIDs))
	}
	if updatedTxIDs[0] != "aaa" || updatedTxIDs[1] != "bbb" || updatedTxIDs[2] != "ccc" {
		t.Errorf("unexpected txids: %v", updatedTxIDs)
	}
	if len(pub.published) != 3 {
		t.Fatalf("expected 3 published events, got %d", len(pub.published))
	}
}

func TestHandleSeenOnNetwork_SingleTxIDBatch(t *testing.T) {
	var updatedTxIDs []string
	ms := &mockStore{
		updateStatusFn: func(_ context.Context, status *models.TransactionStatus) error {
			updatedTxIDs = append(updatedTxIDs, status.TxID)
			return nil
		},
	}
	pub := &mockPublisher{}

	handler := NewCallbackHandler(CallbackConfig{
		Store:          ms,
		EventPublisher: pub,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	app := newTestApp(handler)
	msg := models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxIDs: []string{"abc"},
	}

	resp, err := postCallback(app, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if len(updatedTxIDs) != 1 || updatedTxIDs[0] != "abc" {
		t.Errorf("expected [abc], got %v", updatedTxIDs)
	}
	if len(pub.published) != 1 {
		t.Errorf("expected 1 published event, got %d", len(pub.published))
	}
}

func TestHandleSeenOnNetwork_ScalarFallback(t *testing.T) {
	var updatedTxIDs []string
	ms := &mockStore{
		updateStatusFn: func(_ context.Context, status *models.TransactionStatus) error {
			updatedTxIDs = append(updatedTxIDs, status.TxID)
			return nil
		},
	}
	pub := &mockPublisher{}

	handler := NewCallbackHandler(CallbackConfig{
		Store:          ms,
		EventPublisher: pub,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	app := newTestApp(handler)
	msg := models.CallbackMessage{
		Type: models.CallbackSeenOnNetwork,
		TxID: "abc",
	}

	resp, err := postCallback(app, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if len(updatedTxIDs) != 1 || updatedTxIDs[0] != "abc" {
		t.Errorf("expected [abc], got %v", updatedTxIDs)
	}
	if len(pub.published) != 1 {
		t.Errorf("expected 1 published event, got %d", len(pub.published))
	}
}

func TestHandleSeenOnNetwork_EmptyTxIDsWithScalar(t *testing.T) {
	var updatedTxIDs []string
	ms := &mockStore{
		updateStatusFn: func(_ context.Context, status *models.TransactionStatus) error {
			updatedTxIDs = append(updatedTxIDs, status.TxID)
			return nil
		},
	}
	pub := &mockPublisher{}

	handler := NewCallbackHandler(CallbackConfig{
		Store:          ms,
		EventPublisher: pub,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	app := newTestApp(handler)
	msg := models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxID:  "abc",
		TxIDs: []string{},
	}

	resp, err := postCallback(app, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Empty txids should fall back to scalar txid
	if len(updatedTxIDs) != 1 || updatedTxIDs[0] != "abc" {
		t.Errorf("expected [abc], got %v", updatedTxIDs)
	}
}

func TestHandleSeenMultipleNodes_BatchedTxIDs(t *testing.T) {
	ms := &mockStore{}
	pub := &mockPublisher{}

	handler := NewCallbackHandler(CallbackConfig{
		Store:          ms,
		EventPublisher: pub,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	app := newTestApp(handler)
	msg := models.CallbackMessage{
		Type:  models.CallbackSeenMultipleNodes,
		TxIDs: []string{"tx1", "tx2"},
	}

	resp, err := postCallback(app, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if len(pub.published) != 2 {
		t.Fatalf("expected 2 published events, got %d", len(pub.published))
	}
	if pub.published[0].TxID != "tx1" || pub.published[1].TxID != "tx2" {
		t.Errorf("unexpected txids: %s, %s", pub.published[0].TxID, pub.published[1].TxID)
	}
	for _, s := range pub.published {
		if s.ExtraInfo != "seen by multiple nodes" {
			t.Errorf("expected ExtraInfo 'seen by multiple nodes', got %q", s.ExtraInfo)
		}
	}
}

// Ensure unused interface methods don't cause issues at compile time.
var _ store.Store = (*mockStore)(nil)
var _ events.Publisher = (*mockPublisher)(nil)
