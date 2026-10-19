package bump

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
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

// Variants that disagree on a hash at the same slot cannot both be right.
// When one of them folds to the subtree root, the other is dropped before
// the merge and the block builds from the good one.
func TestBuildCompoundBUMP_ConflictingVariantOffRootIsDropped(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)
	// A STUMP built from subtree 0's leaves but labelled subtree 1 carries
	// different hashes at the same (level, offset) slots.
	stumps := []*models.Stump{
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 0, 990000)},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[0], 0, 990000)},
	}
	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, &blockRoot)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	if vErr := ValidateCompoundRoot(compound, &blockRoot); vErr != nil {
		t.Fatalf("compound does not validate: %v", vErr)
	}
	for _, id := range txids {
		if id == allLeaves[0][0].String() || id == allLeaves[0][1].String() {
			t.Fatalf("leaf %s of the off-root variant made it into txids %v", id, txids)
		}
	}
}

// When neither conflicting variant can be told apart by the subtree root,
// the merge still refuses to build rather than produce a compound that
// validates for one leaf set and silently drops the other.
func TestBuildCompoundBUMP_ConflictingVariantsRejected(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)
	subtreeHashes[1] = chainhash.DoubleHashH([]byte("not the subtree root"))
	stumps := []*models.Stump{
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 0, 990000)},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[0], 0, 990000)},
	}
	if _, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, &blockRoot); err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("conflicting variants must be rejected, got err=%v", err)
	}
}

// A variant that shares no slot with the good one passes the per-slot
// disagreement check, so a misbehaving merkle-service could slip leaves the
// subtree does not contain into the compound. Each variant of a multi-variant
// subtree is checked against the block's subtree root; one that does not
// fold to it is dropped and the build proceeds from the variants that do.
func TestBuildCompoundBUMP_DisjointBadVariantIsDropped(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)
	// Same shape as subtree 1, different hashes: its STUMP for offset 2
	// occupies slots (L0: 2,3; L1: 0) that the good variant for offset 0
	// (L0: 0,1; L1: 1) does not.
	fake := make([]chainhash.Hash, 4)
	for i := range fake {
		fake[i] = chainhash.DoubleHashH([]byte{0xfa, byte(i)})
	}
	stumps := []*models.Stump{
		{BlockHash: "b", SubtreeIndex: 0, StumpData: buildSTUMP(allLeaves[0], 1, 990000)},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 0, 990000)},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(fake, 2, 990000)},
	}
	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, &blockRoot)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	if vErr := ValidateCompoundRoot(compound, &blockRoot); vErr != nil {
		t.Fatalf("compound does not validate: %v", vErr)
	}
	for _, id := range txids {
		for _, f := range fake {
			if id == f.String() {
				t.Fatalf("leaf %s from the bad variant made it into txids %v", id, txids)
			}
		}
	}
	parsed, err := transaction.NewMerklePathFromBinary(compound.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, leaf := range parsed.Path[0] {
		if leaf.Hash == nil {
			continue
		}
		root, err := ExtractMinimalPath(parsed, leaf.Offset).ComputeRoot(leaf.Hash)
		if err != nil || *root != blockRoot {
			t.Fatalf("proof for %s does not hold: root=%v err=%v", leaf.Hash, root, err)
		}
	}
}

// When no variant of a subtree folds to its root, nothing is dropped: the
// check cannot tell which one is wrong, and the compound-root validation
// still guards what gets persisted.
func TestBuildCompoundBUMP_NoVariantMatchingKeepsAll(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)
	subtreeHashes[1] = chainhash.DoubleHashH([]byte("not the subtree root"))
	stumps := []*models.Stump{
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 0, 990000)},
		{BlockHash: "b", SubtreeIndex: 1, StumpData: buildSTUMP(allLeaves[1], 2, 990000)},
	}
	_, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, &blockRoot)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	if len(txids) != 4 {
		t.Fatalf("both variants must be kept when none matches, txids = %v", txids)
	}
}

// Subtree 0's variants fold to the coinbase-corrected root only after the
// placeholder swap; both legitimate shapes (one carrying the coinbase slot,
// one minimal) must survive the root check and be merged.
func TestBuildCompoundBUMP_Subtree0VariantsSurviveRootCheckWithCoinbase(t *testing.T) {
	allLeaves, trueAllLeaves, subtreeHashes, coinbaseTxID, trueBlockRoot := setupCoinbaseBlock(2, 4)
	cbBUMP := buildCoinbaseBUMP(trueAllLeaves[0], coinbaseTxID, 990000, subtreeHashes)
	stumps := []*models.Stump{
		{BlockHash: "b", SubtreeIndex: 0, StumpData: buildSTUMP(allLeaves[0], 1, 990000)}, // carries offset 0
		{BlockHash: "b", SubtreeIndex: 0, StumpData: buildSTUMP(allLeaves[0], 3, 990000)}, // minimal
	}
	// Both must match on their own: if neither did, nothing would be
	// dropped and the build below would pass without proving the check.
	corrected := correctedSubtree0Root(cbBUMP, len(subtreeHashes))
	if corrected == nil {
		t.Fatal("corrected subtree-0 root unavailable")
	}
	for i, st := range stumps {
		if !variantFoldsToRoot(st, corrected, 2, &coinbaseTxID, cbBUMP) {
			t.Fatalf("variant %d does not fold to the corrected subtree-0 root", i)
		}
	}
	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, cbBUMP, &trueBlockRoot)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	if vErr := ValidateCompoundRoot(compound, &trueBlockRoot); vErr != nil {
		t.Fatalf("compound does not validate: %v", vErr)
	}
	have := map[string]bool{}
	for _, id := range txids {
		have[id] = true
	}
	for _, want := range []chainhash.Hash{allLeaves[0][1], allLeaves[0][3]} {
		if !have[want.String()] {
			t.Fatalf("tracked leaf %s dropped; txids %v", want, txids)
		}
	}
}
