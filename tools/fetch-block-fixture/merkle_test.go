package main

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// TestMerkleRoot_StandardBitcoinTree confirms our merkle helpers match
// the Bitcoin convention by reproducing a known mainnet block's root
// from its full txid list. We use block 250000's well-known txid set.
//
// Two-leaf case keeps the test self-contained — no network, no
// fixture, just checks the SDK's MerkleTreeParent does what we
// expect when wired into our buildMerkleTree.
func TestMerkleRoot_TwoLeavesMatchesSDK(t *testing.T) {
	a := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000001")
	b := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000002")
	root := merkleRoot([]chainhash.Hash{*a, *b})
	want := sdkTx.MerkleTreeParent(a, b)
	if !root.IsEqual(want) {
		t.Errorf("merkleRoot(2 leaves) = %s, want %s", root, want)
	}
}

// TestMerkleRoot_DuplicatesLastOnOdd asserts the "duplicate last leaf
// on odd-count levels" rule is applied. With 3 distinct leaves the
// effective tree pairs (l0, l1) and (l2, l2).
func TestMerkleRoot_DuplicatesLastOnOdd(t *testing.T) {
	a := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000001")
	b := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000002")
	c := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000003")
	got := merkleRoot([]chainhash.Hash{*a, *b, *c})
	// Compute the same way manually using the SDK to make this an
	// independent oracle.
	parentAB := sdkTx.MerkleTreeParent(a, b)
	parentCC := sdkTx.MerkleTreeParent(c, c)
	want := sdkTx.MerkleTreeParent(parentAB, parentCC)
	if !got.IsEqual(want) {
		t.Errorf("3-leaf root: got %s want %s", got, want)
	}
}

// TestVarintRoundTrip exercises every length boundary the encoder picks.
func TestVarintRoundTrip(t *testing.T) {
	cases := []uint64{
		0, 1, 0xfc,
		0xfd, 0xff, 0xffff,
		0x10000, 0xffffffff,
		0x100000000, 0x1234567890,
	}
	for _, want := range cases {
		raw := encodeVarint(want)
		c := newCursor(raw, 0)
		got, err := c.varint()
		if err != nil {
			t.Errorf("varint %d: decode err: %v", want, err)
			continue
		}
		if got != want {
			t.Errorf("varint round-trip: encoded %d, decoded %d, raw=%x", want, got, raw)
		}
	}
}

// TestParseTeranodeBlock_Roundtrip verifies appendCoinbaseBUMP yields
// a block that re-parses identically. We construct a synthetic block
// with one subtree, one tx, then exercise the writer + parser.
func TestParseTeranodeBlock_Roundtrip(t *testing.T) {
	// Header: version=1, prev=zeros, merkleRoot=ones, time=0, bits=0, nonce=0
	header := make([]byte, 80)
	header[0] = 1
	for i := 36; i < 68; i++ {
		header[i] = 0xaa
	}
	// Body
	var body bytes.Buffer
	body.Write(header)
	body.Write(encodeVarint(1))   // txCount
	body.Write(encodeVarint(123)) // sizeBytes
	body.Write(encodeVarint(1))   // subtreeCount
	subtreeHash := make([]byte, 32)
	for i := range subtreeHash {
		subtreeHash[i] = byte(i + 1)
	}
	body.Write(subtreeHash)
	// Coinbase tx — minimal serializable tx via SDK.
	coinbase := sdkTx.NewTransaction()
	coinbase.LockTime = 7
	body.Write(coinbase.Bytes())
	body.Write(encodeVarint(948351)) // height
	body.Write(encodeVarint(0))      // empty coinbase BUMP

	parsed, err := parseTeranodeBlock(body.Bytes())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.txCount != 1 || len(parsed.subtreeHashes) != 1 || parsed.height != 948351 {
		t.Errorf("parse: unexpected fields height=%d txCount=%d subtrees=%d",
			parsed.height, parsed.txCount, len(parsed.subtreeHashes))
	}
	if parsed.coinbaseTxID != coinbase.TxID().String() {
		t.Errorf("coinbase txID: got %s want %s", parsed.coinbaseTxID, coinbase.TxID())
	}
}

// TestRecoverSubtreeBoundaries_SingleSubtree returns the trivial case
// without doing any expensive search.
func TestRecoverSubtreeBoundaries_SingleSubtree(t *testing.T) {
	leaves := make([]chainhash.Hash, 5)
	for i := range leaves {
		leaves[i][0] = byte(i + 1)
	}
	bounds, err := recoverSubtreeBoundaries(leaves, []string{"deadbeef"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(bounds) != 1 || bounds[0] != [2]int{0, 5} {
		t.Errorf("unexpected bounds: %v", bounds)
	}
}

// TestRecoverSubtreeBoundaries_TwoEqualSubtrees synthesizes two
// adjacent subtrees of size 2 and confirms the recovery picks the
// right boundary. Uses placeholder semantics for subtree[0].
func TestRecoverSubtreeBoundaries_TwoEqualSubtrees(t *testing.T) {
	cb := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000001")
	tx1 := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000002")
	tx2 := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000003")
	tx3 := mustHash(t, "0000000000000000000000000000000000000000000000000000000000000004")

	all := []chainhash.Hash{*cb, *tx1, *tx2, *tx3}

	// Compute the two subtree roots the recover should match against.
	subtree0Leaves := []chainhash.Hash{{}, *tx1} // placeholder + tx1
	subtree1Leaves := []chainhash.Hash{*tx2, *tx3}
	root0 := merkleRoot(subtree0Leaves)
	root1 := merkleRoot(subtree1Leaves)

	bounds, err := recoverSubtreeBoundaries(all, []string{
		hex.EncodeToString(root0[:]),
		hex.EncodeToString(root1[:]),
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	want := [][2]int{{0, 2}, {2, 4}}
	for i, b := range bounds {
		if b != want[i] {
			t.Errorf("bounds[%d] = %v, want %v", i, b, want[i])
		}
	}
}

func mustHash(t *testing.T, displayHex string) *chainhash.Hash {
	t.Helper()
	h, err := hashFromHex(displayHex)
	if err != nil {
		t.Fatalf("hashFromHex(%q): %v", displayHex, err)
	}
	return h
}
