package synthblock

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/arcade/bump"
)

// TestBuild_MinimalPathsVerify is the load-bearing sanity check: for a range of
// block sizes, every txid's minimal path extracted from the compound (via the
// SAME bump.ExtractMinimalPath the store uses) must recompute the block root.
func TestBuild_MinimalPathsVerify(t *testing.T) {
	for _, numTxs := range []int{1, 2, 4, 8, 16} {
		blk, err := Build(numTxs, 870123)
		if err != nil {
			t.Fatalf("Build(%d): %v", numTxs, err)
		}
		if len(blk.Txids) != numTxs {
			t.Fatalf("Build(%d): got %d txids", numTxs, len(blk.Txids))
		}

		// Whole-compound proof for each tx.
		for _, txid := range blk.Txids {
			if err := VerifyMerklePath(blk.BumpBytes, txid, blk.Root); err != nil {
				t.Errorf("compound verify numTxs=%d: %v", numTxs, err)
			}
		}

		// Per-tx minimal path, mirroring store enrichment: index once, extract per tx.
		compound, offsets, err := bump.IndexCompound(blk.BumpBytes)
		if err != nil {
			t.Fatalf("IndexCompound(%d): %v", numTxs, err)
		}
		for _, txid := range blk.Txids {
			th, err := chainhash.NewHashFromHex(txid)
			if err != nil {
				t.Fatalf("parse txid %s: %v", txid, err)
			}
			off, ok := offsets[*th]
			if !ok {
				t.Fatalf("numTxs=%d: txid %s missing from offset index", numTxs, txid)
			}
			minimal := bump.ExtractMinimalPath(compound, off).Bytes()
			if err := VerifyMerklePath(minimal, txid, blk.Root); err != nil {
				t.Errorf("minimal verify numTxs=%d txid=%s: %v", numTxs, txid, err)
			}
		}
	}
}

func TestBuild_RejectsNonPowerOfTwo(t *testing.T) {
	for _, n := range []int{0, 3, 5, 6, 7, 100} {
		if _, err := Build(n, 1); err == nil {
			t.Errorf("Build(%d) should have failed", n)
		}
	}
}

func TestVerify_DetectsWrongRoot(t *testing.T) {
	blk, err := Build(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	bad := "00" + blk.Root[2:]
	if err := VerifyMerklePath(blk.BumpBytes, blk.Txids[0], bad); err == nil {
		t.Fatal("expected root mismatch to be detected")
	}
}
