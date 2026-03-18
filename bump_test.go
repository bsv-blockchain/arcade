package arcade

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// --- Test Helpers ---

// generateTxHashes produces n deterministic transaction hashes (SHA256 of big-endian index).
func generateTxHashes(n int) []chainhash.Hash {
	hashes := make([]chainhash.Hash, n)
	for i := range n {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		hashes[i] = *hash
	}
	return hashes
}

// buildMerkleTree computes all levels of a merkle tree from leaves.
// Returns tree[level][offset] where tree[0] = leaves and tree[len-1] = [root].
// Handles odd-count levels by duplicating the last hash.
func buildMerkleTree(leaves []chainhash.Hash) [][]chainhash.Hash {
	if len(leaves) == 0 {
		return nil
	}

	tree := [][]chainhash.Hash{leaves}
	current := leaves

	for len(current) > 1 {
		if len(current)%2 == 1 {
			current = append(current, current[len(current)-1]) // duplicate last
		}
		var next []chainhash.Hash
		for i := 0; i < len(current); i += 2 {
			parent := transaction.MerkleTreeParent(&current[i], &current[i+1])
			next = append(next, *parent)
		}
		tree = append(tree, next)
		current = next
	}

	return tree
}

// computeMerkleRoot returns the root of a merkle tree from leaves.
func computeMerkleRoot(leaves []chainhash.Hash) chainhash.Hash {
	tree := buildMerkleTree(leaves)
	return tree[len(tree)-1][0]
}

// buildSTUMP constructs a minimal STUMP (subtree-level merkle path) for a transaction
// at the given offset in a subtree, serialized to BRC-74 binary.
// The path includes levels 0 through height-1 (the root is NOT included — it's what we compute).
func buildSTUMP(leaves []chainhash.Hash, txOffset uint64, blockHeight uint32) []byte {
	tree := buildMerkleTree(leaves)
	// tree has len(tree) levels: tree[0]=leaves, tree[len-1]=[root]
	// The MerklePath should have len(tree)-1 levels (exclude root)
	numLevels := len(tree) - 1
	if numLevels < 1 {
		numLevels = 1
	}

	mp := &transaction.MerklePath{
		BlockHeight: blockHeight,
		Path:        make([][]*transaction.PathElement, numLevels),
	}

	offset := txOffset
	for level := 0; level < numLevels; level++ {
		// Add the tx leaf itself at level 0
		if level == 0 {
			txHash := tree[0][offset]
			isTxid := true
			mp.AddLeaf(0, &transaction.PathElement{
				Offset: offset,
				Hash:   &txHash,
				Txid:   &isTxid,
			})
		}

		// Add the sibling
		sibOffset := offset ^ 1
		levelHashes := tree[level]
		// Handle duplication for odd levels
		if len(levelHashes)%2 == 1 {
			levelHashes = append(levelHashes, levelHashes[len(levelHashes)-1])
		}
		if sibOffset < uint64(len(levelHashes)) {
			h := levelHashes[sibOffset]
			mp.AddLeaf(level, &transaction.PathElement{
				Offset: sibOffset,
				Hash:   &h,
			})
		}

		offset = offset >> 1
	}

	return mp.Bytes()
}

// computeBlockMerkleRoot computes the block-level merkle root from subtree roots.
func computeBlockMerkleRoot(subtreeLeaves [][]chainhash.Hash) chainhash.Hash {
	subtreeRoots := make([]chainhash.Hash, len(subtreeLeaves))
	for i, leaves := range subtreeLeaves {
		subtreeRoots[i] = computeMerkleRoot(leaves)
	}
	if len(subtreeRoots) == 1 {
		return subtreeRoots[0]
	}
	return computeMerkleRoot(subtreeRoots)
}

// buildCoinbaseBUMP constructs a coinbase BUMP for the coinbase transaction in subtree 0.
// The coinbaseTxID replaces the placeholder at offset 0, and the BUMP is built from the
// resulting subtree leaves. Returns BRC-74 binary.
func buildCoinbaseBUMP(subtree0Leaves []chainhash.Hash, coinbaseTxID chainhash.Hash, blockHeight uint32) []byte {
	// Build the true subtree 0 with coinbase replacing placeholder
	trueLeaves := make([]chainhash.Hash, len(subtree0Leaves))
	copy(trueLeaves, subtree0Leaves)
	trueLeaves[0] = coinbaseTxID
	return buildSTUMP(trueLeaves, 0, blockHeight) // coinbase is at offset 0
}

// --- Sanity Tests for Helpers ---

func TestBuildMerkleTree_SanityCheck(t *testing.T) {
	// Verify our buildMerkleTree matches go-sdk's ComputeMissingHashes
	leaves := generateTxHashes(4)

	// Build tree with our helper
	ourRoot := computeMerkleRoot(leaves)

	// Build tree with go-sdk: height = log2(4) = 2 levels in the path
	height := int(math.Ceil(math.Log2(float64(len(leaves)))))
	mp := &transaction.MerklePath{
		BlockHeight: 100,
		Path:        make([][]*transaction.PathElement, height),
	}
	for i, h := range leaves {
		hashCopy := h
		isTxid := true
		mp.AddLeaf(0, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
			Txid:   &isTxid,
		})
	}
	mp.ComputeMissingHashes()
	sdkRoot, err := mp.ComputeRoot(&leaves[0])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}

	if ourRoot != *sdkRoot {
		t.Fatalf("buildMerkleTree root %s != go-sdk root %s", ourRoot, sdkRoot)
	}
}

func TestBuildMerkleTree_OddCount(t *testing.T) {
	leaves := generateTxHashes(3)
	ourRoot := computeMerkleRoot(leaves)

	// Build tree with go-sdk (which handles duplication internally)
	// For 3 leaves, padded to 4, height = 2
	height := int(math.Ceil(math.Log2(float64(len(leaves) + 1)))) // ceil for odd
	mp := &transaction.MerklePath{
		BlockHeight: 100,
		Path:        make([][]*transaction.PathElement, height),
	}
	for i, h := range leaves {
		hashCopy := h
		isTxid := (i == 0)
		mp.AddLeaf(0, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
			Txid:   &isTxid,
		})
	}
	// Add duplicate marker for odd count
	dup := true
	mp.AddLeaf(0, &transaction.PathElement{
		Offset:    uint64(len(leaves)),
		Duplicate: &dup,
	})
	mp.ComputeMissingHashes()
	sdkRoot, err := mp.ComputeRoot(&leaves[0])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}

	if ourRoot != *sdkRoot {
		t.Fatalf("buildMerkleTree root (odd) %s != go-sdk root %s", ourRoot, sdkRoot)
	}
}

// --- Single-Subtree Tests ---

func TestAssembleBUMP_SingleSubtree_2txs_Offset1(t *testing.T) {
	leaves := generateTxHashes(2)
	expectedRoot := computeMerkleRoot(leaves)
	subtreeHashes := []chainhash.Hash{expectedRoot}

	stump := buildSTUMP(leaves, 1, 800000)
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&leaves[1])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != expectedRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, expectedRoot)
	}
}

func TestAssembleBUMP_SingleSubtree_4txs_Offset0(t *testing.T) {
	leaves := generateTxHashes(4)
	expectedRoot := computeMerkleRoot(leaves)
	subtreeHashes := []chainhash.Hash{expectedRoot}

	stump := buildSTUMP(leaves, 0, 800001)
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&leaves[0])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != expectedRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, expectedRoot)
	}
}

func TestAssembleBUMP_SingleSubtree_8txs_LastOffset(t *testing.T) {
	leaves := generateTxHashes(8)
	expectedRoot := computeMerkleRoot(leaves)
	subtreeHashes := []chainhash.Hash{expectedRoot}

	stump := buildSTUMP(leaves, 7, 800002)
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&leaves[7])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != expectedRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, expectedRoot)
	}
}

func TestAssembleBUMP_SingleSubtree_16txs_Middle(t *testing.T) {
	leaves := generateTxHashes(16)
	expectedRoot := computeMerkleRoot(leaves)
	subtreeHashes := []chainhash.Hash{expectedRoot}

	stump := buildSTUMP(leaves, 5, 800003)
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&leaves[5])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != expectedRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, expectedRoot)
	}
}

// --- Multi-Subtree Tests ---

// multiSubtreeTestSetup creates a block with numSubtrees subtrees of subtreeSize txs each.
// Returns all subtree leaves, subtree root hashes, and the expected block merkle root.
func multiSubtreeTestSetup(numSubtrees, subtreeSize int) (allLeaves [][]chainhash.Hash, subtreeHashes []chainhash.Hash, blockRoot chainhash.Hash) {
	allLeaves = make([][]chainhash.Hash, numSubtrees)
	subtreeHashes = make([]chainhash.Hash, numSubtrees)

	// Generate distinct tx hashes per subtree
	offset := 0
	for s := range numSubtrees {
		leaves := make([]chainhash.Hash, subtreeSize)
		for i := range subtreeSize {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(offset+i+1000*s))
			h := sha256.Sum256(buf[:])
			hash, _ := chainhash.NewHash(h[:])
			leaves[i] = *hash
		}
		allLeaves[s] = leaves
		subtreeHashes[s] = computeMerkleRoot(leaves)
		offset += subtreeSize
	}

	blockRoot = computeMerkleRoot(subtreeHashes)
	return
}

func TestAssembleBUMP_2Subtrees_TrackedInSubtree1(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)

	txOffset := uint64(2)
	stump := buildSTUMP(allLeaves[1], txOffset, 900000)

	result, _, err := AssembleBUMP(stump, 1, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&allLeaves[1][txOffset])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != blockRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, blockRoot)
	}
}

func TestAssembleBUMP_4Subtrees_TrackedInSubtree2(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(4, 4)

	txOffset := uint64(1)
	stump := buildSTUMP(allLeaves[2], txOffset, 900001)

	result, _, err := AssembleBUMP(stump, 2, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&allLeaves[2][txOffset])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != blockRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, blockRoot)
	}
}

func TestAssembleBUMP_8Subtrees_TrackedInSubtree5(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(8, 4)

	txOffset := uint64(3)
	stump := buildSTUMP(allLeaves[5], txOffset, 900002)

	// Verify subtree root layer has expected height (3 for 8 subtrees)
	subtreeRootLayer := int(math.Ceil(math.Log2(float64(len(subtreeHashes)))))
	if subtreeRootLayer != 3 {
		t.Fatalf("expected subtreeRootLayer=3, got %d", subtreeRootLayer)
	}

	result, _, err := AssembleBUMP(stump, 5, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&allLeaves[5][txOffset])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != blockRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, blockRoot)
	}
}

func TestAssembleBUMP_2Subtrees_DifferentSizes(t *testing.T) {
	// Subtree 0: 8 txs, Subtree 1: 4 txs
	leaves0 := generateTxHashes(8)
	// Use different seed for subtree 1
	leaves1 := make([]chainhash.Hash, 4)
	for i := range 4 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(100+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		leaves1[i] = *hash
	}

	subtreeHashes := []chainhash.Hash{
		computeMerkleRoot(leaves0),
		computeMerkleRoot(leaves1),
	}
	blockRoot := computeMerkleRoot(subtreeHashes)

	// Track tx in smaller subtree (subtree 1)
	txOffset := uint64(2)
	stump := buildSTUMP(leaves1, txOffset, 900003)

	result, _, err := AssembleBUMP(stump, 1, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&leaves1[txOffset])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != blockRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, blockRoot)
	}
}

// --- Coinbase Placeholder Tests ---

func TestAssembleBUMP_Subtree0_CoinbaseReplacement(t *testing.T) {
	placeholder := chainhash.Hash{0xff, 0xff, 0xff, 0xff}
	coinbaseTxID := generateTxHashes(1)[0]

	subtree0Leaves := make([]chainhash.Hash, 4)
	subtree0Leaves[0] = placeholder
	for i := 1; i < 4; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(200+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		subtree0Leaves[i] = *hash
	}

	subtree1Leaves := make([]chainhash.Hash, 4)
	for i := range 4 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(300+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		subtree1Leaves[i] = *hash
	}

	stump := buildSTUMP(subtree0Leaves, 1, 950000) // tracked tx at offset 1

	trueSubtree0Leaves := make([]chainhash.Hash, 4)
	copy(trueSubtree0Leaves, subtree0Leaves)
	trueSubtree0Leaves[0] = coinbaseTxID

	subtreeHashes := []chainhash.Hash{
		computeMerkleRoot(subtree0Leaves),
		computeMerkleRoot(subtree1Leaves),
	}

	trueBlockRoot := computeBlockMerkleRoot([][]chainhash.Hash{trueSubtree0Leaves, subtree1Leaves})

	cbBUMP := buildCoinbaseBUMP(subtree0Leaves, coinbaseTxID, 950000)
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, cbBUMP)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&subtree0Leaves[1])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != trueBlockRoot {
		t.Fatalf("root mismatch with coinbase replacement: got %s, want %s", root, trueBlockRoot)
	}
}

func TestAssembleBUMP_Subtree0_CoinbaseReplacement_Offset3(t *testing.T) {
	placeholder := chainhash.Hash{0xff, 0xff, 0xff, 0xff}
	coinbaseTxID := generateTxHashes(1)[0]

	subtree0Leaves := make([]chainhash.Hash, 4)
	subtree0Leaves[0] = placeholder
	for i := 1; i < 4; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(400+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		subtree0Leaves[i] = *hash
	}

	subtree1Leaves := generateTxHashes(4)

	stump := buildSTUMP(subtree0Leaves, 3, 950001)

	trueSubtree0Leaves := make([]chainhash.Hash, 4)
	copy(trueSubtree0Leaves, subtree0Leaves)
	trueSubtree0Leaves[0] = coinbaseTxID

	subtreeHashes := []chainhash.Hash{
		computeMerkleRoot(subtree0Leaves),
		computeMerkleRoot(subtree1Leaves),
	}

	trueBlockRoot := computeBlockMerkleRoot([][]chainhash.Hash{trueSubtree0Leaves, subtree1Leaves})

	cbBUMP := buildCoinbaseBUMP(subtree0Leaves, coinbaseTxID, 950001)
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, cbBUMP)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&subtree0Leaves[3])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != trueBlockRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, trueBlockRoot)
	}
}

func TestAssembleBUMP_Subtree0_NoCoinbase(t *testing.T) {
	placeholder := chainhash.Hash{0xff, 0xff, 0xff, 0xff}
	coinbaseTxID := generateTxHashes(1)[0]

	subtree0Leaves := make([]chainhash.Hash, 4)
	subtree0Leaves[0] = placeholder
	for i := 1; i < 4; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(500+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		subtree0Leaves[i] = *hash
	}
	subtree1Leaves := generateTxHashes(4)

	stump := buildSTUMP(subtree0Leaves, 1, 950002)

	trueSubtree0Leaves := make([]chainhash.Hash, 4)
	copy(trueSubtree0Leaves, subtree0Leaves)
	trueSubtree0Leaves[0] = coinbaseTxID

	subtreeHashes := []chainhash.Hash{
		computeMerkleRoot(subtree0Leaves),
		computeMerkleRoot(subtree1Leaves),
	}
	trueBlockRoot := computeBlockMerkleRoot([][]chainhash.Hash{trueSubtree0Leaves, subtree1Leaves})

	// AssembleBUMP WITHOUT coinbase — should produce a BUMP, but root won't match true root
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&subtree0Leaves[1])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root == trueBlockRoot {
		t.Fatal("expected root to NOT match true block root when coinbase is nil (placeholder differs)")
	}
}

func TestAssembleBUMP_4Subtrees_Subtree0_CoinbaseReplacement(t *testing.T) {
	placeholder := chainhash.Hash{0xff, 0xff, 0xff, 0xff}
	coinbaseTxID := generateTxHashes(1)[0]

	allLeaves := make([][]chainhash.Hash, 4)
	allLeaves[0] = make([]chainhash.Hash, 4)
	allLeaves[0][0] = placeholder
	for i := 1; i < 4; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(600+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		allLeaves[0][i] = *hash
	}
	for s := 1; s < 4; s++ {
		allLeaves[s] = make([]chainhash.Hash, 4)
		for i := range 4 {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(600+s*100+i))
			h := sha256.Sum256(buf[:])
			hash, _ := chainhash.NewHash(h[:])
			allLeaves[s][i] = *hash
		}
	}

	subtreeHashes := make([]chainhash.Hash, 4)
	for s := range 4 {
		subtreeHashes[s] = computeMerkleRoot(allLeaves[s])
	}

	stump := buildSTUMP(allLeaves[0], 2, 950003)

	trueAllLeaves := make([][]chainhash.Hash, 4)
	for s := range 4 {
		trueAllLeaves[s] = make([]chainhash.Hash, len(allLeaves[s]))
		copy(trueAllLeaves[s], allLeaves[s])
	}
	trueAllLeaves[0][0] = coinbaseTxID

	trueBlockRoot := computeBlockMerkleRoot(trueAllLeaves)

	cbBUMP := buildCoinbaseBUMP(allLeaves[0], coinbaseTxID, 950003)
	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, cbBUMP)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&allLeaves[0][2])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != trueBlockRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, trueBlockRoot)
	}
}

// --- Edge Cases ---

func TestAssembleBUMP_OddSubtreeSize(t *testing.T) {
	leaves := generateTxHashes(3)
	expectedRoot := computeMerkleRoot(leaves)
	subtreeHashes := []chainhash.Hash{expectedRoot}

	stump := buildSTUMP(leaves, 1, 960000)

	result, _, err := AssembleBUMP(stump, 0, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&leaves[1])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != expectedRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, expectedRoot)
	}
}

func TestAssembleBUMP_TwoTxs_DifferentSubtrees_SameRoot(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(4, 4)

	// Track tx in subtree 1
	stump1 := buildSTUMP(allLeaves[1], 2, 970000)
	result1, _, err := AssembleBUMP(stump1, 1, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP (subtree 1) failed: %v", err)
	}

	// Track tx in subtree 3
	stump3 := buildSTUMP(allLeaves[3], 0, 970000)
	result3, _, err := AssembleBUMP(stump3, 3, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP (subtree 3) failed: %v", err)
	}

	root1, err := result1.ComputeRoot(&allLeaves[1][2])
	if err != nil {
		t.Fatalf("ComputeRoot (subtree 1) failed: %v", err)
	}
	root3, err := result3.ComputeRoot(&allLeaves[3][0])
	if err != nil {
		t.Fatalf("ComputeRoot (subtree 3) failed: %v", err)
	}

	if *root1 != blockRoot {
		t.Fatalf("subtree 1 root mismatch: got %s, want %s", root1, blockRoot)
	}
	if *root3 != blockRoot {
		t.Fatalf("subtree 3 root mismatch: got %s, want %s", root3, blockRoot)
	}
	if *root1 != *root3 {
		t.Fatalf("roots should be equal: %s vs %s", root1, root3)
	}
}

func TestAssembleBUMP_TwoTxs_SameSubtree_SameRoot(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)

	// Track two txs in subtree 1 at different offsets
	stumpA := buildSTUMP(allLeaves[1], 0, 970001)
	resultA, _, err := AssembleBUMP(stumpA, 1, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP (offset 0) failed: %v", err)
	}

	stumpB := buildSTUMP(allLeaves[1], 3, 970001)
	resultB, _, err := AssembleBUMP(stumpB, 1, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP (offset 3) failed: %v", err)
	}

	rootA, err := resultA.ComputeRoot(&allLeaves[1][0])
	if err != nil {
		t.Fatalf("ComputeRoot (offset 0) failed: %v", err)
	}
	rootB, err := resultB.ComputeRoot(&allLeaves[1][3])
	if err != nil {
		t.Fatalf("ComputeRoot (offset 3) failed: %v", err)
	}

	if *rootA != blockRoot {
		t.Fatalf("offset 0 root mismatch: got %s, want %s", rootA, blockRoot)
	}
	if *rootB != blockRoot {
		t.Fatalf("offset 3 root mismatch: got %s, want %s", rootB, blockRoot)
	}
}

func TestAssembleBUMP_LargeBlock_16Subtrees_32Txs(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(16, 32)

	// Track a tx deep in subtree 11, at offset 17
	txOffset := uint64(17)
	stump := buildSTUMP(allLeaves[11], txOffset, 980000)

	result, _, err := AssembleBUMP(stump, 11, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}

	root, err := result.ComputeRoot(&allLeaves[11][txOffset])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	if *root != blockRoot {
		t.Fatalf("root mismatch: got %s, want %s", root, blockRoot)
	}
}
