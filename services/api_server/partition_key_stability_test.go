package api_server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

// parentChildWithParentLowest builds a parent/child pair whose parent txid
// sorts BELOW the child's, so the family key (min of the two) is the
// parent's. That distinction is what makes the assertion discriminating: if
// the child sorted lowest, a buggy fallback to "key by my own txid" would
// coincidentally equal the family key and the test would pass while the
// child still landed on the wrong partition. LockTime is a free nonce.
func parentChildWithParentLowest(t *testing.T) (parent, child *sdkTx.Transaction) {
	t.Helper()
	for nonce := uint32(0); nonce < 1000; nonce++ {
		p := sdkTx.NewTransaction()
		p.LockTime = nonce
		p.AddOutput(&sdkTx.TransactionOutput{Satoshis: 0, LockingScript: &script.Script{}})
		ph := p.TxID()

		c := sdkTx.NewTransaction()
		c.Inputs = append(c.Inputs, &sdkTx.TransactionInput{
			SourceTXID:       ph,
			SourceTxOutIndex: 0,
			SequenceNumber:   sdkTx.DefaultSequenceNumber,
		})
		if ph.String() < c.TxID().String() {
			return p, c
		}
	}
	t.Fatal("no parent/child pair found with parent txid < child txid")
	return nil, nil
}

// TestFamilyPartitionKeys_StableUnderPartialDedup pins the stability property
// familyPartitionKeys' own doc comment promises:
//
//	"a client retrying the same family with the batch shuffled maps to the
//	 same partition as the original messages"
//
// That holds for shuffling, but the keys are computed over toPublish — the
// POST-DEDUP slice — so it does not hold when a family member drops out at
// the Phase 3 GetOrInsertStatus CAS. And a member dropping out is the normal
// case, not an edge case: the family key is min(family ∩ toPublish), and min
// over a shrinking set moves.
//
// The scenario here is the standard wallet pattern — send the parent, then
// send parent+child together a moment later (re-sending the parent for
// safety):
//
//  1. POST {P}          → P published, keyed P, lands on partition h(P)
//  2. POST {P, C}       → P dedups out at RECEIVED, so toPublish = {C}
//  3. familyPartitionKeys([C], [[P]]) finds no in-batch source for C
//  4. C is keyed by its OWN txid → partition h(C), which is not h(P)
//
// C's dispatcher has never seen P, so handleAdmit does not hold it behind
// its parent. C broadcasts concurrently with P, draws TX_MISSING_PARENT,
// burns retry_max_attempts × retry_backoff_ms (10s at defaults), parks at
// PENDING_RETRY, and waits at least stalePendingRetryAge (5 minutes, a
// hardcoded const) for the reaper. Under #151's single partition this child
// was held cleanly and accepted in milliseconds.
//
// The same shape occurs on the partial-produce path: SendMessages fails for
// a subset, the client retries the identical body, the members that already
// landed dedup out, and the remainder recomputes onto a different partition
// than the in-flight siblings it is supposed to be co-located with.
//
// Computing the keys over `parsed` (the full submitted batch) instead fixes
// this. The key is only a hash input — it need not be a published txid — so
// a deduped-out member remaining as the family representative is correct.
func TestFamilyPartitionKeys_StableUnderPartialDedup(t *testing.T) {
	parentTx, childTx := parentChildWithParentLowest(t)
	parentTxid := parentTx.TxID().String()
	childTxid := childTx.TxID().String()

	// The family key both submissions must agree on. By construction the
	// parent sorts lowest, so this differs from the child's own txid.
	familyKey := parentTxid

	// The parent is already known at RECEIVED — it was submitted moments ago
	// and is still in flight on its partition. The child is new.
	ms := &mockStore{
		getOrInsertFn: func(status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			if status.TxID == parentTxid {
				return &models.TransactionStatus{
					TxID:      parentTxid,
					Status:    models.StatusReceived,
					Timestamp: time.Now(),
				}, false, nil // found, not inserted → deduped out of toPublish
			}
			return nil, true, nil // inserted → published
		},
	}

	broker := &kafka.RecordingBroker{}
	srv, router := setupServerWithStore(broker, ms)
	defer close(srv.submissionStop)

	body := append(append([]byte(nil), parentTx.Bytes()...), childTx.Bytes()...)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	broker.Lock()
	defer broker.Unlock()
	if len(broker.Batches) != 1 {
		t.Fatalf("expected 1 SendBatch, got %d", len(broker.Batches))
	}
	keys := make(map[string]string, 1)
	for _, kv := range broker.Batches[0] {
		val, err := json.Marshal(kv.Value)
		if err != nil {
			t.Fatalf("marshal recorded value: %v", err)
		}
		var envelope struct {
			TXID string `json:"txid"`
		}
		if err := json.Unmarshal(val, &envelope); err != nil {
			t.Fatalf("unmarshal recorded envelope: %v", err)
		}
		keys[envelope.TXID] = kv.Key
	}

	got, published := keys[childTxid]
	if !published {
		t.Fatalf("child was not published; recorded keys = %v", keys)
	}
	if got != familyKey {
		t.Fatalf("child key = %q, want family key %q.\n"+
			"The parent deduped out of toPublish, so familyPartitionKeys saw no in-batch "+
			"source for the child and fell back to its own txid — routing it to a partition "+
			"its still-in-flight parent is not on. The dispatcher there cannot hold it behind "+
			"its parent, so it draws TX_MISSING_PARENT and parks at PENDING_RETRY for at "+
			"least stalePendingRetryAge (5m) instead of being admitted in milliseconds.",
			got, familyKey)
	}
}
