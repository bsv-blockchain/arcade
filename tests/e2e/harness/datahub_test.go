//go:build e2e

package harness

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/bump"
)

// TestDatahub_StageAndServe is a fast self-test on the in-process
// datahub. It stages a synthetic block + subtree, GETs them via the
// host-side URL, and asserts arcade's bump.FetchBlockDataForBUMP can
// round-trip the same block through the binary parser. This confirms
// the harness produces wire-compatible bytes — both arcade and the
// merkle-service container see the same shape.
func TestDatahub_StageAndServe(t *testing.T) {
	d, err := NewDatahub(t)
	if err != nil {
		t.Fatalf("new datahub: %v", err)
	}

	// Build 8 synthetic txs → one subtree → one block.
	txs := BuildTxs(8, 0)
	txids := TxIDs(txs)
	subtreeBin := SubtreeBinary(txids)
	subtreeRoot := SubtreeRoot(txids)

	coinbase := BuildCoinbase(1)
	coinbaseHash := *coinbase.TxIDChainHash()
	merkleRoot := MerkleRootFromCoinbaseAndSubtree(coinbaseHash, subtreeRoot)

	prev := chainhash.Hash{}
	blockBin, blockHash, err := BuildBlockBinary(
		prev, merkleRoot, /*height=*/ 1, /*ts=*/ 0, /*bits=*/ nil, /*nonce=*/ 0,
		[]chainhash.Hash{subtreeRoot}, coinbase, nil,
		/*txCount=*/ uint64(len(txs)+1), /*sizeBytes=*/ 1,
	)
	if err != nil {
		t.Fatalf("build block: %v", err)
	}
	d.StageBlock(blockHash, blockBin)
	d.StageSubtree(subtreeRoot, subtreeBin)

	// Sanity: GET /subtree/<root> returns exactly the bytes we staged.
	resp, err := http.Get(d.LocalURL() + "/subtree/" + subtreeRoot.String())
	if err != nil {
		t.Fatalf("GET subtree: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(body) != len(subtreeBin) {
		t.Errorf("subtree: status=%d len=%d want OK len=%d", resp.StatusCode, len(body), len(subtreeBin))
	}

	// Round-trip the block through arcade's parser. It exercises
	// parseBlockBinary indirectly via FetchBlockDataForBUMP.
	hashes, _, gotMerkle, err := bump.FetchBlockDataForBUMP(
		context.Background(),
		[]string{d.LocalURL()},
		blockHash.String(),
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("FetchBlockDataForBUMP: %v", err)
	}
	if len(hashes) != 1 || hashes[0].String() != subtreeRoot.String() {
		t.Errorf("subtree round-trip: got %v, want [%s]", hashes, subtreeRoot)
	}
	if gotMerkle == nil || gotMerkle.String() != merkleRoot.String() {
		t.Errorf("merkle root round-trip: got %v, want %s", gotMerkle, merkleRoot)
	}
}

// TestDatahub_NotFound returns 404 for a hash that wasn't staged.
func TestDatahub_NotFound(t *testing.T) {
	d, err := NewDatahub(t)
	if err != nil {
		t.Fatalf("new datahub: %v", err)
	}
	resp, err := http.Get(d.LocalURL() + "/block/deadbeef")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status=%d want 404", resp.StatusCode)
	}
}
