//go:build e2e

package harness

import (
	"context"
	"encoding/binary"
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
		prev, merkleRoot /*height=*/, 1 /*ts=*/, 0 /*bits=*/, nil /*nonce=*/, 0,
		[]chainhash.Hash{subtreeRoot}, coinbase, nil,
		/*txCount=*/ uint64(len(txs)+1) /*sizeBytes=*/, 1,
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

// fakeHeader builds a syntactically valid 80-byte header whose prevHash
// points at parent, with nonce varying the hash. Returns bytes + hash.
func fakeHeader(t *testing.T, parent chainhash.Hash, nonce uint32) ([]byte, chainhash.Hash) {
	t.Helper()
	h := make([]byte, 80)
	binary.LittleEndian.PutUint32(h[0:4], 1) // version
	copy(h[4:36], parent[:])                 // prevHash
	// merkleRoot [36:68], time [68:72], bits [72:76] left zero.
	binary.LittleEndian.PutUint32(h[76:80], nonce)
	return h, chainhash.DoubleHashH(h)
}

// TestDatahub_HeadersBackwardWalk pins the /headers/<hash>?n=N contract
// go-chaintracks' SyncFromRemoteTip depends on: concatenated 80-byte
// headers, newest first, walking child→parent, stopping at the first
// unstaged parent; 404 for an unknown start hash.
func TestDatahub_HeadersBackwardWalk(t *testing.T) {
	d, err := NewDatahub(t)
	if err != nil {
		t.Fatalf("new datahub: %v", err)
	}

	// Chain: g <- a <- b <- c, with g's parent NOT staged.
	gBytes, gHash := fakeHeader(t, chainhash.Hash{}, 0)
	aBytes, aHash := fakeHeader(t, gHash, 1)
	bBytes, bHash := fakeHeader(t, aHash, 2)
	cBytes, cHash := fakeHeader(t, bHash, 3)
	for _, hdr := range []struct {
		hash  chainhash.Hash
		bytes []byte
	}{{gHash, gBytes}, {aHash, aBytes}, {bHash, bBytes}, {cHash, cBytes}} {
		d.StageHeader(hdr.hash, hdr.bytes)
	}

	// Full walk from c over HTTP: c, b, a, g (g's parent is unstaged → stop).
	resp, err := http.Get(d.LocalURL() + "/headers/" + cHash.String() + "?n=1000")
	if err != nil {
		t.Fatalf("GET headers: %v", err)
	}
	payload, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
	if len(payload) != 4*80 {
		t.Fatalf("expected 4 headers (320 bytes), got %d bytes", len(payload))
	}
	for i, want := range [][]byte{cBytes, bBytes, aBytes, gBytes} {
		got := payload[i*80 : (i+1)*80]
		if string(got) != string(want) {
			t.Fatalf("header %d mismatch (walk must be newest-first child→parent)", i)
		}
	}

	// n caps the walk.
	if payload, ok := d.headersBackward(cHash.String(), 2); !ok || len(payload) != 2*80 {
		t.Fatalf("n=2 walk: ok=%v len=%d, want 160 bytes", ok, len(payload))
	}

	// Unknown start hash → 404.
	resp, err = http.Get(d.LocalURL() + "/headers/" + chainhash.Hash{0xff}.String())
	if err != nil {
		t.Fatalf("GET unknown: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown hash: status=%d want 404", resp.StatusCode)
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
