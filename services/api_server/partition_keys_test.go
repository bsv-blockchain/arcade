package api_server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/kafka"
)

// txidA < txidB < txidC … lexicographically; realistic 64-hex shapes so the
// tests read like production data.
var (
	txidA = strings.Repeat("a", 64)
	txidB = strings.Repeat("b", 64)
	txidC = strings.Repeat("c", 64)
	txidD = strings.Repeat("d", 64)
	txidE = strings.Repeat("e", 64)
	// An out-of-batch ancestor (confirmed on chain, never submitted here).
	txidZ = strings.Repeat("0", 63) + "f"
)

func assertKeys(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("key count = %d, want %d (got %v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("keys[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestFamilyPartitionKeys_IsolatedTxsKeyByOwnTxid pins the degenerate
// case: txs with no in-batch relatives keep today's key (their own
// txid), so single-tx submissions and independent batch members hash
// exactly as before #295.
func TestFamilyPartitionKeys_IsolatedTxsKeyByOwnTxid(t *testing.T) {
	keys := familyPartitionKeys(
		[]string{txidB, txidC},
		[][]string{{txidZ}, {txidZ}},
	)
	assertKeys(t, keys, []string{txidB, txidC})
}

// TestFamilyPartitionKeys_StarFamilySharesOneKey pins the headline #295
// use case: two parents and their children submitted in one batch all
// share one partition key — the lexicographically smallest family
// member — so per-partition Kafka order preserves parent-before-child.
func TestFamilyPartitionKeys_StarFamilySharesOneKey(t *testing.T) {
	// D and E are parents; A, B, C are children spending them (A spends
	// both — the fan-in that joins the two parents into one family).
	keys := familyPartitionKeys(
		[]string{txidD, txidE, txidA, txidB, txidC},
		[][]string{{txidZ}, {txidZ}, {txidD, txidE}, {txidD}, {txidE}},
	)
	assertKeys(t, keys, []string{txidA, txidA, txidA, txidA, txidA})
}

// TestFamilyPartitionKeys_ChainIsTransitive pins transitivity: a → b → c
// (grandparent/parent/child) is one family even though c never
// references a directly.
func TestFamilyPartitionKeys_ChainIsTransitive(t *testing.T) {
	keys := familyPartitionKeys(
		[]string{txidA, txidB, txidC},
		[][]string{{txidZ}, {txidA}, {txidB}},
	)
	assertKeys(t, keys, []string{txidA, txidA, txidA})
}

// TestFamilyPartitionKeys_DisjointFamiliesStaySeparate pins that
// unrelated families get different keys — this is what lets independent
// traffic spread across partitions.
func TestFamilyPartitionKeys_DisjointFamiliesStaySeparate(t *testing.T) {
	keys := familyPartitionKeys(
		[]string{txidA, txidB, txidC, txidD},
		[][]string{{txidZ}, {txidA}, {txidZ}, {txidC}},
	)
	assertKeys(t, keys, []string{txidA, txidA, txidC, txidC})
}

// TestFamilyPartitionKeys_OutOfBatchParentDoesNotJoin pins that spending
// an outpoint of a tx NOT in this batch never links two txs: two
// siblings spending different outputs of the same confirmed ancestor
// are independent for ordering purposes (the ancestor is already on
// chain; neither needs to wait for the other).
func TestFamilyPartitionKeys_OutOfBatchParentDoesNotJoin(t *testing.T) {
	keys := familyPartitionKeys(
		[]string{txidB, txidC},
		[][]string{{txidZ}, {txidZ}},
	)
	if keys[0] == keys[1] {
		t.Fatalf("siblings of an out-of-batch parent must not share a key, both got %q", keys[0])
	}
}

// TestFamilyPartitionKeys_OrderIndependent pins the root-choice
// property that makes resubmission safe: reordering the batch must not
// change any tx's key, otherwise a client retry with a shuffled batch
// would remap the family to a different partition than the original
// messages.
func TestFamilyPartitionKeys_OrderIndependent(t *testing.T) {
	forward := familyPartitionKeys(
		[]string{txidD, txidB, txidC},
		[][]string{{txidZ}, {txidD}, {txidB}},
	)
	reversed := familyPartitionKeys(
		[]string{txidC, txidB, txidD},
		[][]string{{txidB}, {txidD}, {txidZ}},
	)
	if forward[0] != reversed[2] || forward[1] != reversed[1] || forward[2] != reversed[0] {
		t.Fatalf("keys changed under reordering: forward=%v reversed=%v", forward, reversed)
	}
	if forward[0] != txidB {
		t.Errorf("family key = %q, want lexicographically smallest member %q", forward[0], txidB)
	}
}

// TestFamilyPartitionKeys_SelfAndEmptyRefsIgnored pins the defensive
// guards (mirroring dispatcher handleAdmit): an empty input txid or a
// self-reference must not join anything or panic.
func TestFamilyPartitionKeys_SelfAndEmptyRefsIgnored(t *testing.T) {
	keys := familyPartitionKeys(
		[]string{txidA, txidB},
		[][]string{{"", txidA}, {""}},
	)
	assertKeys(t, keys, []string{txidA, txidB})
}

// TestFamilyPartitionKeys_DuplicateTxidDoesNotPanic pins the store==nil
// intake path where dedup is skipped and the same txid can appear twice
// in one batch: both occurrences get the same key and nothing panics.
func TestFamilyPartitionKeys_DuplicateTxidDoesNotPanic(t *testing.T) {
	keys := familyPartitionKeys(
		[]string{txidA, txidA, txidB},
		[][]string{{txidZ}, {txidZ}, {txidA}},
	)
	if keys[0] != keys[1] {
		t.Errorf("duplicate txid occurrences diverged: %q vs %q", keys[0], keys[1])
	}
	if keys[2] != keys[0] {
		t.Errorf("child of duplicated parent got %q, want family key %q", keys[2], keys[0])
	}
}

// TestFamilyPartitionKeys_EmptyBatch pins the trivial bound.
func TestFamilyPartitionKeys_EmptyBatch(t *testing.T) {
	if keys := familyPartitionKeys(nil, nil); len(keys) != 0 {
		t.Fatalf("expected empty result for empty batch, got %v", keys)
	}
}

// --- intake wiring ---

// TestHandleSubmitTransactions_FamilyKeyedPublish pins the #295 intake
// wiring: a batch containing a parent, its child, and an unrelated tx
// must publish the parent and child under one shared partition key (the
// lexicographically smallest family member's txid) and the unrelated tx
// under its own txid. Without family keying, a multi-partition topic
// would hash parent and child to different partitions and lose their
// relative order.
func TestHandleSubmitTransactions_FamilyKeyedPublish(t *testing.T) {
	parentTx := sdkTx.NewTransaction()
	parentTx.AddOutput(&sdkTx.TransactionOutput{Satoshis: 0, LockingScript: &script.Script{}})
	parentHash := parentTx.TxID()
	parentTxid := parentHash.String()

	childTx := sdkTx.NewTransaction()
	childTx.Inputs = append(childTx.Inputs, &sdkTx.TransactionInput{
		SourceTXID:       parentHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   sdkTx.DefaultSequenceNumber,
	})
	childTxid := childTx.TxID().String()

	loner := sdkTx.NewTransaction()
	lonerTxid := loner.TxID().String()

	familyKey := parentTxid
	if childTxid < familyKey {
		familyKey = childTxid
	}

	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker)

	body := append(append(append([]byte(nil), parentTx.Bytes()...), childTx.Bytes()...), loner.Bytes()...)
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
	keys := make(map[string]string, 3)
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
	if keys[parentTxid] != familyKey {
		t.Errorf("parent key = %q, want family key %q", keys[parentTxid], familyKey)
	}
	if keys[childTxid] != familyKey {
		t.Errorf("child key = %q, want family key %q", keys[childTxid], familyKey)
	}
	if keys[lonerTxid] != lonerTxid {
		t.Errorf("unrelated tx key = %q, want its own txid %q", keys[lonerTxid], lonerTxid)
	}
}
