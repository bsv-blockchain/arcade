package bump

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/models"
)

// Two merkle-services deliver the same subtree, each tracking a different tx
// (the second service missed the first tx's registration during an outage).
// The compound must cover both tracked txs, carry the Txid marker on each,
// and validate against the block root.
func TestBuildCompoundBUMP_MergesSameSubtreeVariants(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)

	// Subtree 1 arrives as two variants: one tracking leaf 0, one leaf 2.
	// Subtree 0 arrives once, tracking leaf 1.
	stumps := []*models.Stump{
		{BlockHash: "blockhash", SubtreeIndex: 0, StumpData: buildSTUMP(allLeaves[0], 1, 990000)},
		{BlockHash: "blockhash", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 0, 990000)},
		{BlockHash: "blockhash", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 2, 990000)},
	}

	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, &blockRoot)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	if vErr := ValidateCompoundRoot(compound, &blockRoot); vErr != nil {
		t.Fatalf("merged compound does not validate: %v", vErr)
	}

	// Level-0 of subtree 1 now carries all four leaves (each variant brings
	// its tracked leaf plus sibling), and txids lists every level-0 hash once.
	wantTracked := map[string]bool{allLeaves[1][0].String(): true, allLeaves[1][2].String(): true, allLeaves[0][1].String(): true}
	seen := map[string]int{}
	for _, id := range txids {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("txid %s listed %d times", id, n)
		}
	}
	for id := range wantTracked {
		if seen[id] != 1 {
			t.Fatalf("tracked tx %s missing from txids %v", id, txids)
		}
	}

	parsed, err := transaction.NewMerklePathFromBinary(compound.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	tracked := 0
	for _, leaf := range parsed.Path[0] {
		if leaf.Hash == nil {
			continue
		}
		if leaf.Txid != nil && *leaf.Txid {
			tracked++
			if !wantTracked[leaf.Hash.String()] {
				t.Fatalf("unexpected Txid marker on %s", leaf.Hash)
			}
		}
		minimal := ExtractMinimalPath(parsed, leaf.Offset)
		root, err := minimal.ComputeRoot(leaf.Hash)
		if err != nil {
			t.Fatalf("ComputeRoot(%s): %v", leaf.Hash, err)
		}
		if *root != blockRoot {
			t.Fatalf("root for %s = %s, want %s", leaf.Hash, root, blockRoot)
		}
	}
	if tracked != len(wantTracked) {
		t.Fatalf("tracked markers = %d, want %d", tracked, len(wantTracked))
	}
}

// Identical STUMPs from several services (the common case) merge to exactly
// the single-STUMP result.
func TestBuildCompoundBUMP_IdenticalVariantsAreOneSTUMP(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)
	data := buildSTUMP(allLeaves[1], 3, 990000)
	single, singleTxIDs, err := BuildCompoundBUMP(
		[]*models.Stump{{BlockHash: "b", SubtreeIndex: 1, StumpData: data}}, subtreeHashes, nil, &blockRoot)
	if err != nil {
		t.Fatal(err)
	}
	dup, dupTxIDs, err := BuildCompoundBUMP([]*models.Stump{
		{BlockHash: "b", SubtreeIndex: 1, StumpData: data},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: data},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: data},
	}, subtreeHashes, nil, &blockRoot)
	if err != nil {
		t.Fatal(err)
	}
	if string(single.Bytes()) != string(dup.Bytes()) {
		t.Fatal("three identical deliveries must build the same compound as one")
	}
	if strings.Join(singleTxIDs, ",") != strings.Join(dupTxIDs, ",") {
		t.Fatalf("txids differ: %v vs %v", singleTxIDs, dupTxIDs)
	}
}

// Variants that disagree on a hash at the same slot cannot both be right;
// building from them must fail rather than produce a compound that validates
// for one leaf set and silently drops the other.
func TestBuildCompoundBUMP_ConflictingVariantsRejected(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)
	// A STUMP built from subtree 0's leaves but labelled subtree 1 carries
	// different hashes at the same (level, offset) slots.
	stumps := []*models.Stump{
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 0, 990000)},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[0], 0, 990000)},
	}
	if _, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, &blockRoot); err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("conflicting variants must be rejected, got err=%v", err)
	}
}
