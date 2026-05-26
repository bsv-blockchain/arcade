package bump

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/arcade/models"
)

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(s), "\n", ""))
	if err != nil {
		t.Fatalf("decode hex: %v", err)
	}
	return b
}

func mustHashFromHex(t *testing.T, s string) chainhash.Hash {
	t.Helper()
	h, err := chainhash.NewHashFromHex(strings.TrimSpace(s))
	if err != nil {
		t.Fatalf("parse hash: %v", err)
	}
	return *h
}

// TestBuildCompoundBUMP_Issue167_SingleSubtreeNonPow2 reproduces issue #167 using
// the exact inputs captured from the production failure on mainnet block 950675
// (39,657 txs, reported by datahub as a single subtree). The block tree is
// height 16 — taller than the subtree-0 STUMP (height 15) because the coinbase
// sits outside the subtree — so the single-subtree path must fold the subtree
// root up to the block root using the coinbase BUMP's left-spine sibling.
//
// Before the fix, BuildCompoundBUMP returned the subtree root instead of the
// block root, so ValidateCompoundRoot rejected the compound and the block's
// tracked transactions were stuck at SEEN_MULTIPLE_NODES forever.
//
// Fixtures (issue167_fixtures_test.go) are the real STUMP, coinbase BUMP,
// subtree hash, and header merkle root logged by the bump-builder failure dump.
func TestBuildCompoundBUMP_Issue167_SingleSubtreeNonPow2(t *testing.T) {
	stumpData := mustDecodeHex(t, issue167StumpHex)
	coinbaseBUMP := mustDecodeHex(t, issue167CoinbaseBUMPHex)
	headerRoot := mustHashFromHex(t, issue167HeaderMerkleRoot)
	subtreeHashes := []chainhash.Hash{mustHashFromHex(t, issue167SubtreeHash0)}

	stumps := []*models.Stump{
		{BlockHash: "00000000000000000eb6ea115a5cb140ffeae7b673b4c5b6f3fac62e0adae3c5", SubtreeIndex: 0, StumpData: stumpData},
	}

	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, coinbaseBUMP)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP returned error: %v", err)
	}
	if len(txids) == 0 {
		t.Fatal("expected tracked txids in the compound, got none")
	}

	// The compound's computed root MUST equal the canonical block-header merkle
	// root. Before the fix this failed with
	// "computed root e0360bc5… != block header merkle root 32d293e3…".
	if err := ValidateCompoundRoot(compound, &headerRoot); err != nil {
		t.Fatalf("compound BUMP does not reconcile to block header root: %v", err)
	}

	// Every tracked tx's minimal path must also verify to the header root.
	verified := 0
	for _, leaf := range compound.Path[0] {
		if leaf.Txid == nil || !*leaf.Txid || leaf.Hash == nil {
			continue
		}
		minimal := ExtractMinimalPath(compound, leaf.Offset)
		got, err := minimal.ComputeRoot(leaf.Hash)
		if err != nil {
			t.Fatalf("ComputeRoot for tracked tx %s: %v", leaf.Hash, err)
		}
		if !got.IsEqual(&headerRoot) {
			t.Fatalf("tracked tx %s: minimal-path root %s != header root %s", leaf.Hash, got, &headerRoot)
		}
		verified++
	}
	if verified == 0 {
		t.Fatal("expected at least one tracked tx to verify against the header root")
	}
}
