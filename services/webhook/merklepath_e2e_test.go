package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/internal/synthblock"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store/pebble"
)

// These tests exercise the real push path end to end: a real pebble store holds
// a mined block's compound BUMP, and the webhook dispatch/reaper code enriches
// the outgoing callback body with the per-tx merkle proof. Verification is
// cryptographic — the received merklePath must recompute the block's merkle
// root for the delivered txid (synthblock.VerifyMerklePathHex).

const (
	e2eBlockHash = "000000000000000000000000000000000000000000000000000000000000beef"
	e2eHeight    = 870123
)

func newWebhookPebble(t *testing.T) *pebble.Store {
	t.Helper()
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// mineBlock seeds numTxs rows, transitions them to MINED under blockHash, and
// (when insertBump) stores the block's compound BUMP so enrichment can find it.
func mineBlock(t *testing.T, st *pebble.Store, insertBump bool) synthblock.Block {
	t.Helper()
	ctx := context.Background()
	blk, err := synthblock.Build(8, e2eHeight)
	if err != nil {
		t.Fatalf("synthblock.Build: %v", err)
	}
	for _, txid := range blk.Txids {
		if _, _, err := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("seed row %s: %v", txid, err)
		}
	}
	if _, _, err := st.SetMinedByTxIDs(ctx, e2eBlockHash, e2eHeight, blk.Txids); err != nil {
		t.Fatalf("SetMinedByTxIDs: %v", err)
	}
	if insertBump {
		if err := st.InsertBUMP(ctx, e2eBlockHash, e2eHeight, blk.BumpBytes); err != nil {
			t.Fatalf("InsertBUMP: %v", err)
		}
	}
	return blk
}

// captureReceiver is an httptest callback endpoint that decodes each POST body
// and forwards it on a channel.
func captureReceiver(t *testing.T) (*httptest.Server, <-chan map[string]any) {
	t.Helper()
	bodies := make(chan map[string]any, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, bodies
}

func recvBody(t *testing.T, bodies <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case b := <-bodies:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for webhook POST")
		return nil
	}
}

func TestWebhook_E2E_MinedDeliversMerklePath(t *testing.T) {
	st := newWebhookPebble(t)
	blk := mineBlock(t, st, true)
	srv, bodies := captureReceiver(t)

	subTxid := blk.Txids[3]
	if err := st.InsertSubmission(context.Background(), &models.Submission{
		SubmissionID:  "sub-live",
		TxID:          subTxid,
		CallbackURL:   srv.URL,
		CallbackToken: "tok-live",
	}); err != nil {
		t.Fatalf("InsertSubmission: %v", err)
	}

	svc := New(config.WebhookConfig{}, config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil)

	// Drive the exact bulk MINED event bump_builder publishes.
	svc.handleUpdate(context.Background(), &models.TransactionStatus{
		Status:      models.StatusMined,
		BlockHash:   e2eBlockHash,
		BlockHeight: e2eHeight,
		Timestamp:   time.Now().UTC(),
		TxIDs:       blk.Txids,
	})

	body := recvBody(t, bodies)
	if body["txid"] != subTxid {
		t.Fatalf("txid: got %v want %s", body["txid"], subTxid)
	}
	if body["txStatus"] != string(models.StatusMined) {
		t.Fatalf("txStatus: got %v want MINED", body["txStatus"])
	}
	if body["blockHash"] != e2eBlockHash {
		t.Fatalf("blockHash: got %v want %s", body["blockHash"], e2eBlockHash)
	}
	mp, ok := body["merklePath"].(string)
	if !ok || mp == "" {
		t.Fatalf("merklePath missing from webhook body: %v", body["merklePath"])
	}
	if err := synthblock.VerifyMerklePathHex(mp, subTxid, blk.Root); err != nil {
		t.Fatalf("delivered merklePath does not verify: %v", err)
	}
}

func TestWebhook_E2E_ReaperRedeliversMerklePath(t *testing.T) {
	st := newWebhookPebble(t)
	blk := mineBlock(t, st, true)
	srv, bodies := captureReceiver(t)

	subTxid := blk.Txids[1]
	if err := st.InsertSubmission(context.Background(), &models.Submission{
		SubmissionID:  "sub-reaper",
		TxID:          subTxid,
		CallbackURL:   srv.URL,
		CallbackToken: "tok-reaper",
	}); err != nil {
		t.Fatalf("InsertSubmission: %v", err)
	}
	// Put the submission in ready-for-retry state at MINED: LastDeliveredStatus
	// already advanced, NextRetryAt in the past.
	past := time.Now().Add(-time.Minute)
	if err := st.UpdateDeliveryStatus(context.Background(), "sub-reaper", models.StatusMined, 1, &past); err != nil {
		t.Fatalf("UpdateDeliveryStatus: %v", err)
	}

	svc := New(config.WebhookConfig{}, config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil)

	svc.reapOnce(context.Background())

	body := recvBody(t, bodies)
	if body["txStatus"] != string(models.StatusMined) {
		t.Fatalf("redelivery must keep LastDeliveredStatus MINED, got %v", body["txStatus"])
	}
	mp, ok := body["merklePath"].(string)
	if !ok || mp == "" {
		t.Fatalf("reaper redelivery missing merklePath: %v", body["merklePath"])
	}
	if err := synthblock.VerifyMerklePathHex(mp, subTxid, blk.Root); err != nil {
		t.Fatalf("redelivered merklePath does not verify: %v", err)
	}
}

func TestWebhook_E2E_MissingBUMPStillDelivers(t *testing.T) {
	st := newWebhookPebble(t)
	blk := mineBlock(t, st, false) // rows MINED, but no compound BUMP stored
	srv, bodies := captureReceiver(t)

	subTxid := blk.Txids[2]
	if err := st.InsertSubmission(context.Background(), &models.Submission{
		SubmissionID:  "sub-nobump",
		TxID:          subTxid,
		CallbackURL:   srv.URL,
		CallbackToken: "tok-nobump",
	}); err != nil {
		t.Fatalf("InsertSubmission: %v", err)
	}

	svc := New(config.WebhookConfig{}, config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil)

	svc.handleUpdate(context.Background(), &models.TransactionStatus{
		Status:      models.StatusMined,
		BlockHash:   e2eBlockHash,
		BlockHeight: e2eHeight,
		Timestamp:   time.Now().UTC(),
		TxIDs:       blk.Txids,
	})

	body := recvBody(t, bodies)
	// Delivery must still happen (best-effort proof), just without merklePath.
	if body["txid"] != subTxid || body["txStatus"] != string(models.StatusMined) {
		t.Fatalf("expected MINED delivery for %s, got %v", subTxid, body)
	}
	if mp, present := body["merklePath"]; present {
		t.Fatalf("merklePath should be absent when the BUMP is missing, got %v", mp)
	}
}
