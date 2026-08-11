package bump

// Regression tests for issue #234: blocks whose FINAL subtree is shorter than
// half the first subtree's capacity have their final root height-LIFTED by
// teranode (one self-hash per missing level) before top-tree composition.
// The compound builder must (a) derive the lift from the header merkle root,
// (b) accept the final subtree's naturally-shorter STUMP and represent the
// lift levels as duplicate-flag elements, and (c) seed a no-STUMP final slot
// with the lifted root. Sibling of merkle-service#179.

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/models"
)

// liftHash self-hashes h k times — teranode's RootHashPadded lift step.
func liftHash(h chainhash.Hash, k int) chainhash.Hash {
	for i := 0; i < k; i++ {
		h = *transaction.MerkleTreeParent(&h, &h)
	}
	return h
}

// liftedBlockRoot composes the canonical (teranode-style) block merkle root:
// subtree roots with the final one lifted to the first subtree's height,
// folded with duplicate-last-on-odd padding.
func liftedBlockRoot(subtreeLeaves [][]chainhash.Hash) (chainhash.Hash, []chainhash.Hash, int) {
	n := len(subtreeLeaves)
	roots := make([]chainhash.Hash, n)
	for i, leaves := range subtreeLeaves {
		roots[i] = computeMerkleRootFromLeaves(leaves)
	}
	height := func(leafCount int) int {
		h := 0
		for 1<<h < leafCount {
			h++
		}
		return h
	}
	lift := height(len(subtreeLeaves[0])) - height(len(subtreeLeaves[n-1]))
	lifted := append([]chainhash.Hash(nil), roots...)
	lifted[n-1] = liftHash(roots[n-1], lift)
	var root chainhash.Hash
	if n == 1 {
		root = lifted[0]
	} else {
		root = computeMerkleRootFromLeaves(lifted)
	}
	return root, roots, lift
}

// TestBuildCompoundBUMP_LiftedFinalSubtree covers the lifted shapes end to
// end. Each case asserts the compound validates against the canonical
// (lifted) header root; cases with a tracked tx in the final subtree also
// fold that tx's path explicitly.
func TestBuildCompoundBUMP_LiftedFinalSubtree(t *testing.T) {
	const blockHeight = 954978

	cases := []struct {
		name       string
		shapes     []int // leaves per subtree; first is a power of two
		stumpFinal bool  // tracked tx in the final subtree?
		expectLift int
	}{
		{"8+3_lift1_finalSeeded", []int{8, 3}, false, 1},
		{"8+3_lift1_finalStump", []int{8, 3}, true, 1},
		{"8+2_lift2_finalStump", []int{8, 2}, true, 2},
		{"8x3+2_lift2_multiTopLayers", []int{8, 8, 8, 2}, true, 2},
		{"8+8_uniform_noLift", []int{8, 8}, true, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := len(tc.shapes)
			subtreeLeaves := make([][]chainhash.Hash, n)
			seed := 0
			for i, count := range tc.shapes {
				subtreeLeaves[i] = generateTxHashes(count + seed)[seed:]
				seed += count
			}

			headerRoot, rawRoots, lift := liftedBlockRoot(subtreeLeaves)
			if lift != tc.expectLift {
				t.Fatalf("fixture lift = %d, want %d", lift, tc.expectLift)
			}

			// STUMP for a tracked tx in subtree 0 (offset 1), and optionally
			// one in the final subtree (offset 0) at its NATURAL height —
			// exactly what merkle-service delivers.
			stumps := []*models.Stump{
				{BlockHash: "blk", SubtreeIndex: 0, StumpData: buildSTUMP(subtreeLeaves[0], 1, blockHeight)},
			}
			if tc.stumpFinal {
				stumps = append(stumps, &models.Stump{
					BlockHash: "blk", SubtreeIndex: n - 1,
					StumpData: buildSTUMP(subtreeLeaves[n-1], 0, blockHeight),
				})
			}

			subtreeHashes := append([]chainhash.Hash(nil), rawRoots...)
			compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, &headerRoot)
			if err != nil {
				t.Fatalf("BuildCompoundBUMP: %v", err)
			}
			if len(txids) == 0 {
				t.Fatal("expected tracked txids")
			}

			if vErr := ValidateCompoundRoot(compound, &headerRoot); vErr != nil {
				t.Fatalf("compound does not fold to the canonical lifted root: %v", vErr)
			}

			// Fold the sub0 tracked tx explicitly.
			leaf0 := subtreeLeaves[0][1]
			if got, err := compound.ComputeRoot(&leaf0); err != nil || !got.IsEqual(&headerRoot) {
				t.Fatalf("sub0 tx path: got %v err %v, want %s", got, err, headerRoot)
			}
			// And the final-subtree tracked tx when present.
			if tc.stumpFinal {
				leafF := subtreeLeaves[n-1][0]
				if got, err := compound.ComputeRoot(&leafF); err != nil || !got.IsEqual(&headerRoot) {
					t.Fatalf("final-subtree tx path: got %v err %v, want %s", got, err, headerRoot)
				}
			}
		})
	}
}

// TestBuildCompoundBUMP_LiftedFinalSubtree_WithCoinbase mirrors the full
// production shape: coinbase placeholder at subtree-0 leaf 0, a full-height
// coinbase BUMP whose block climb uses the LIFTED top tree (as teranode
// ships it), placeholder-based subtreeHashes, and STUMPs in both subtrees.
func TestBuildCompoundBUMP_LiftedFinalSubtree_WithCoinbase(t *testing.T) {
	const blockHeight = 954978
	placeholder := chainhash.Hash{}
	for i := range placeholder {
		placeholder[i] = 0xFF
	}
	coinbaseTxID := generateTxHashes(1)[0]

	// Shapes {8, 2}: lift 2. Subtree 0 carries the placeholder at leaf 0.
	sub0 := generateTxHashes(9)[1:9]
	sub0[0] = placeholder
	trueSub0 := append([]chainhash.Hash(nil), sub0...)
	trueSub0[0] = coinbaseTxID
	subF := generateTxHashes(11)[9:11]

	rawRoots := []chainhash.Hash{
		computeMerkleRootFromLeaves(sub0), // placeholder-based (as supplied)
		computeMerkleRootFromLeaves(subF),
	}
	correctedRoots := []chainhash.Hash{
		computeMerkleRootFromLeaves(trueSub0),
		liftHash(rawRoots[1], 2),
	}
	headerRoot := computeMerkleRootFromLeaves(correctedRoots)

	// Coinbase BUMP: subtree-internal path over the TRUE leaves plus the
	// block climb across the corrected+lifted top tree.
	cbBUMP := buildCoinbaseBUMP(sub0, coinbaseTxID, blockHeight, correctedRoots)

	stumps := []*models.Stump{
		{BlockHash: "blk", SubtreeIndex: 0, StumpData: buildFullSTUMP(sub0, 1, blockHeight)},
		{BlockHash: "blk", SubtreeIndex: 1, StumpData: buildSTUMP(subF, 1, blockHeight)},
	}

	subtreeHashes := append([]chainhash.Hash(nil), rawRoots...)
	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, cbBUMP, &headerRoot)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	if vErr := ValidateCompoundRoot(compound, &headerRoot); vErr != nil {
		t.Fatalf("compound does not fold to the canonical lifted root: %v", vErr)
	}
	// Coinbase and a final-subtree tx must both fold to the header root.
	if got, err := compound.ComputeRoot(&coinbaseTxID); err != nil || !got.IsEqual(&headerRoot) {
		t.Fatalf("coinbase path: got %v err %v, want %s", got, err, headerRoot)
	}
	leafF := subF[1]
	if got, err := compound.ComputeRoot(&leafF); err != nil || !got.IsEqual(&headerRoot) {
		t.Fatalf("final-subtree tx path: got %v err %v, want %s", got, err, headerRoot)
	}
}

// TestBuildCompoundBUMP_LiftedShape_NilHeaderFailsSafe locks two properties:
// nil headerMerkleRoot preserves the legacy uniform-height behavior, and the
// lifted fixtures genuinely exercise the lift (the legacy composition must
// NOT fold to the canonical root — otherwise the tests above would pass for
// trivial reasons).
func TestBuildCompoundBUMP_LiftedShape_NilHeaderFailsSafe(t *testing.T) {
	subtreeLeaves := [][]chainhash.Hash{
		generateTxHashes(8),
		generateTxHashes(11)[8:11],
	}
	headerRoot, rawRoots, lift := liftedBlockRoot(subtreeLeaves)
	if lift != 1 {
		t.Fatalf("fixture lift = %d, want 1", lift)
	}

	stumps := []*models.Stump{
		{BlockHash: "blk", SubtreeIndex: 0, StumpData: buildSTUMP(subtreeLeaves[0], 1, 954978)},
	}
	subtreeHashes := append([]chainhash.Hash(nil), rawRoots...)

	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil, nil)
	if err != nil {
		t.Fatalf("legacy build should still construct: %v", err)
	}
	if vErr := ValidateCompoundRoot(compound, &headerRoot); vErr == nil {
		t.Fatal("legacy (un-lifted) composition unexpectedly matched the lifted root — fixtures do not exercise the lift")
	}
}
