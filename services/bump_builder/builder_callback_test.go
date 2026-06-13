package bump_builder

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// These tests cover the datahub-independent callback path added for issue #195:
// when merkle-service enriches BLOCK_PROCESSED with merkleRoot + subtreeCount +
// subtreeHashes + coinbaseBump, bump-builder must build and validate the
// compound BUMP using those fields directly, making ZERO datahub calls. They
// also pin the fallback to the existing datahub path when the enrichment is
// absent or inconsistent.

// recordingDatahub is an httptest server that counts /block/<hash> GETs and
// rejects them with 500. Callback-path tests wire it as arcade's only datahub
// so a regression that wrongly falls back is caught two ways: the counter goes
// non-zero AND the build fails (the 500 surfaces as a handleMessage error).
type recordingDatahub struct {
	server     *httptest.Server
	blockFetch atomic.Int64
}

func newRecordingDatahub(t *testing.T) *recordingDatahub {
	t.Helper()
	d := &recordingDatahub{}
	d.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/block/") {
			d.blockFetch.Add(1)
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(d.server.Close)
	return d
}

func (d *recordingDatahub) BlockFetches() int64 { return d.blockFetch.Load() }

// makeBlockProcessedMsgWithEnrichment builds a BLOCK_PROCESSED kafka.Message
// carrying the four enrichment fields, encoding merkleRoot/subtreeHashes as
// display-order hex (chainhash.Hash.String()) exactly as merkle-service does.
func makeBlockProcessedMsgWithEnrichment(t *testing.T, blockHash string, merkleRoot chainhash.Hash, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) *kafka.Message {
	t.Helper()
	hashesHex := make([]string, len(subtreeHashes))
	for i := range subtreeHashes {
		hashesHex[i] = subtreeHashes[i].String()
	}
	cb := models.CallbackMessage{
		Type:          models.CallbackBlockProcessed,
		BlockHash:     blockHash,
		MerkleRoot:    merkleRoot.String(),
		SubtreeCount:  len(subtreeHashes),
		SubtreeHashes: hashesHex,
		CoinbaseBUMP:  models.HexBytes(coinbaseBUMP),
	}
	data, err := json.Marshal(cb)
	if err != nil {
		t.Fatalf("marshal enriched callback: %v", err)
	}
	return &kafka.Message{Topic: "arcade.block_processed", Value: data}
}

// twoSubtreeEnrichmentFixture builds a 2-subtree block with a non-empty
// coinbase BUMP and returns the stored STUMPs' inputs plus the chainhash.Hash
// merkle root the compound will produce. Reuses the same helpers the
// datahub-path multi-subtree tests use; the coinbase BUMP is a minimal valid
// BRC-74 path (required by the callback gate). The returned root is whatever
// BuildCompoundBUMP yields for these exact inputs, so a callback carrying it
// validates by construction.
func twoSubtreeEnrichmentFixture(t *testing.T, ms *mockStore, blockHash string) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, root chainhash.Hash) {
	t.Helper()
	const (
		txid1 = testTxidHex
		txid2 = "3333333333333333333333333333333333333333333333333333333333333333"
		sib1  = "5555555555555555555555555555555555555555555555555555555555555555"
		sib2  = "7777777777777777777777777777777777777777777777777777777777777777"
		cbTx  = "4444444444444444444444444444444444444444444444444444444444444444"
	)
	stumps := []*models.Stump{
		{BlockHash: blockHash, SubtreeIndex: 0, StumpData: makeTwoLeafSTUMP(txid1, sib1)},
		{BlockHash: blockHash, SubtreeIndex: 1, StumpData: makeTwoLeafSTUMP(txid2, sib2)},
	}
	for _, s := range stumps {
		if err := ms.InsertStump(context.Background(), s); err != nil {
			t.Fatalf("InsertStump: %v", err)
		}
	}
	subtreeHashes = []chainhash.Hash{
		subtreeRootFromTwoLeafSTUMP(t, txid1, sib1),
		subtreeRootFromTwoLeafSTUMP(t, txid2, sib2),
	}
	coinbaseBUMP = makeMinimalSTUMP(cbTx)

	rootBytes := expectedCompoundRoot(t, stumps, subtreeHashes, coinbaseBUMP)
	rh, err := chainhash.NewHash(rootBytes)
	if err != nil {
		t.Fatalf("NewHash(root): %v", err)
	}
	return subtreeHashes, coinbaseBUMP, *rh
}

// TestBuilder_HandleMessage_CallbackPath_NoDatahubCall is the core acceptance
// test: a BLOCK_PROCESSED carrying all enrichment fields builds + validates the
// compound BUMP, marks txs MINED, prunes STUMPs, and makes ZERO datahub calls.
func TestBuilder_HandleMessage_CallbackPath_NoDatahubCall(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	subtreeHashes, coinbaseBUMP, root := twoSubtreeEnrichmentFixture(t, ms, blockHash)

	datahub := newRecordingDatahub(t)
	b := newTestBuilder(ms, datahub.server.URL)

	before := testutil.ToFloat64(metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("callback"))

	msg := makeBlockProcessedMsgWithEnrichment(t, blockHash, root, subtreeHashes, coinbaseBUMP)
	if err := b.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage (callback path): %v", err)
	}

	if got := datahub.BlockFetches(); got != 0 {
		t.Errorf("datahub /block fetches = %d, want 0 (callback path must not consult datahub)", got)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected compound BUMP to be stored")
	}
	if len(ms.minedCalls) != 1 {
		t.Errorf("expected 1 SetMinedByTxIDs call, got %d", len(ms.minedCalls))
	}
	if len(ms.deletedBlocks) != 1 || ms.deletedBlocks[0] != blockHash {
		t.Errorf("expected STUMPs for %s pruned, got %v", blockHash, ms.deletedBlocks)
	}
	if after := testutil.ToFloat64(metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("callback")); after != before+1 {
		t.Errorf("block_data_source{source=callback} = %v, want %v", after, before+1)
	}
}

// TestBuilder_HandleMessage_CallbackPath_RootMismatchRefuses pins the safety
// net: when the assembled compound's root does not match the authoritative
// callback merkleRoot, bump-builder refuses to persist — no BUMP stored, no txs
// MINED, STUMPs left intact for a later retry — and returns an error. It must
// not consult the datahub.
func TestBuilder_HandleMessage_CallbackPath_RootMismatchRefuses(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	subtreeHashes, coinbaseBUMP, _ := twoSubtreeEnrichmentFixture(t, ms, blockHash)

	datahub := newRecordingDatahub(t)
	b := newTestBuilder(ms, datahub.server.URL)

	// Wrong merkle root (all 0xab) — valid hex, but not what the compound folds to.
	wrongRoot := mustHash(t, "abababababababababababababababababababababababababababababababab")
	msg := makeBlockProcessedMsgWithEnrichment(t, blockHash, wrongRoot, subtreeHashes, coinbaseBUMP)

	if err := b.handleMessage(context.Background(), msg); err == nil {
		t.Fatal("expected root-mismatch error, got nil")
	}

	if got := datahub.BlockFetches(); got != 0 {
		t.Errorf("datahub /block fetches = %d, want 0", got)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; ok {
		t.Error("BUMP must not be stored on root mismatch")
	}
	if len(ms.minedCalls) != 0 {
		t.Errorf("no txs should be MINED on root mismatch, got %d calls", len(ms.minedCalls))
	}
	if len(ms.deletedBlocks) != 0 {
		t.Errorf("STUMPs must remain for retry on root mismatch, got deletes: %v", ms.deletedBlocks)
	}
}

// TestBuilder_HandleMessage_MissingEnrichmentFallsBackToDatahub pins that a
// legacy BLOCK_PROCESSED (no enrichment fields) still builds via the datahub —
// the rollout-safe fallback — and records source=datahub.
func TestBuilder_HandleMessage_MissingEnrichmentFallsBackToDatahub(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txid := testTxidHex
	sib := "2222222222222222222222222222222222222222222222222222222222222222"
	ms.addStump(blockHash, 0, makeTwoLeafSTUMP(txid, sib))

	subtreeHashes := []chainhash.Hash{subtreeRootFromTwoLeafSTUMP(t, txid, sib)}
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: makeTwoLeafSTUMP(txid, sib)}},
		subtreeHashes, nil)
	datahub := newDatahubServer(root, subtreeHashes)
	defer datahub.Close()
	b := newTestBuilder(ms, datahub.URL)

	before := testutil.ToFloat64(metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("datahub"))

	// makeBlockProcessedMsg carries no enrichment fields.
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage (fallback): %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP built via datahub fallback")
	}
	if after := testutil.ToFloat64(metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("datahub")); after != before+1 {
		t.Errorf("block_data_source{source=datahub} = %v, want %v", after, before+1)
	}
}

// TestBuilder_HandleMessage_CallbackInconsistentSubtreeCountFallsBack pins the
// consistency guard: when the callback's subtreeCount disagrees with the
// subtreeHashes length, bump-builder rejects the enrichment and falls back to
// the datahub (which has its own validators) rather than trusting a malformed
// callback.
func TestBuilder_HandleMessage_CallbackInconsistentSubtreeCountFallsBack(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txid := testTxidHex
	sib := "2222222222222222222222222222222222222222222222222222222222222222"
	ms.addStump(blockHash, 0, makeTwoLeafSTUMP(txid, sib))

	subtreeHashes := []chainhash.Hash{subtreeRootFromTwoLeafSTUMP(t, txid, sib)}
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: makeTwoLeafSTUMP(txid, sib)}},
		subtreeHashes, nil)
	datahub := newDatahubServer(root, subtreeHashes)
	defer datahub.Close()
	b := newTestBuilder(ms, datahub.URL)

	before := testutil.ToFloat64(metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("datahub"))

	// Enrichment present but SubtreeCount (99) contradicts subtreeHashes (1).
	cb := models.CallbackMessage{
		Type:          models.CallbackBlockProcessed,
		BlockHash:     blockHash,
		MerkleRoot:    subtreeHashes[0].String(),
		SubtreeCount:  99,
		SubtreeHashes: []string{subtreeHashes[0].String()},
		CoinbaseBUMP:  models.HexBytes(makeMinimalSTUMP(testTxidHex)),
	}
	data, err := json.Marshal(cb)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := &kafka.Message{Topic: "arcade.block_processed", Value: data}

	if err := b.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage (inconsistent count → fallback): %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP built via datahub fallback after rejecting inconsistent callback")
	}
	if after := testutil.ToFloat64(metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("datahub")); after != before+1 {
		t.Errorf("block_data_source{source=datahub} = %v, want %v", after, before+1)
	}
}
