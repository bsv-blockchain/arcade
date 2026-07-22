package synthblock

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/arcade/bump"
)

// TestBuild_MinimalPathsVerify is the load-bearing sanity check: for a range of
// block sizes, every txid's minimal path extracted from the compound (via the
// SAME bump.CompoundIndex walk the store enrichment uses) must recompute the
// block root and match the unindexed extractor byte-for-byte.
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
			if vErr := VerifyMerklePath(blk.BumpBytes, txid, blk.Root); vErr != nil {
				t.Errorf("compound verify numTxs=%d: %v", numTxs, vErr)
			}
		}

		// Per-tx minimal path, mirroring store enrichment: index once, extract per tx.
		idx, err := bump.IndexCompound(blk.BumpBytes)
		if err != nil {
			t.Fatalf("IndexCompound(%d): %v", numTxs, err)
		}
		for _, txid := range blk.Txids {
			minimal := idx.MinimalPathBytes(txid)
			if len(minimal) == 0 {
				t.Fatalf("numTxs=%d: no minimal path for txid %s", numTxs, txid)
			}
			// The indexed walk must be byte-identical to the unindexed extractor.
			if legacy := bump.ExtractMinimalPathForTx(blk.BumpBytes, txid); !bytes.Equal(minimal, legacy) {
				t.Errorf("numTxs=%d txid=%s: indexed path differs from ExtractMinimalPathForTx", numTxs, txid)
			}
			if vErr := VerifyMerklePath(minimal, txid, blk.Root); vErr != nil {
				t.Errorf("minimal verify numTxs=%d txid=%s: %v", numTxs, txid, vErr)
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
