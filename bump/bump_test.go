package bump

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/models"
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
func buildMerkleTree(leaves []chainhash.Hash) [][]chainhash.Hash {
	if len(leaves) == 0 {
		return nil
	}

	tree := [][]chainhash.Hash{leaves}
	current := leaves

	for len(current) > 1 {
		if len(current)%2 == 1 {
			current = append(current, current[len(current)-1])
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
func computeMerkleRootFromLeaves(leaves []chainhash.Hash) chainhash.Hash {
	tree := buildMerkleTree(leaves)
	return tree[len(tree)-1][0]
}

// buildSTUMP constructs a minimal STUMP (subtree-level merkle path) for a transaction
// at the given offset in a subtree, serialized to BRC-74 binary.
func buildSTUMP(leaves []chainhash.Hash, txOffset uint64, blockHeight uint32) []byte {
	tree := buildMerkleTree(leaves)
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
		if level == 0 {
			txHash := tree[0][offset]
			isTxid := true
			mp.Path[0] = append(mp.Path[0], &transaction.PathElement{
				Offset: offset,
				Hash:   &txHash,
				Txid:   &isTxid,
			})
		}

		sibOffset := offset ^ 1
		levelHashes := tree[level]
		if len(levelHashes)%2 == 1 {
			levelHashes = append(levelHashes, levelHashes[len(levelHashes)-1])
		}
		if sibOffset < uint64(len(levelHashes)) {
			h := levelHashes[sibOffset]
			mp.Path[level] = append(mp.Path[level], &transaction.PathElement{
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
		subtreeRoots[i] = computeMerkleRootFromLeaves(leaves)
	}
	if len(subtreeRoots) == 1 {
		return subtreeRoots[0]
	}
	return computeMerkleRootFromLeaves(subtreeRoots)
}

// buildCoinbaseBUMP constructs a coinbase BUMP for the coinbase transaction in subtree 0.
func buildCoinbaseBUMP(subtree0Leaves []chainhash.Hash, coinbaseTxID chainhash.Hash, blockHeight uint32) []byte {
	trueLeaves := make([]chainhash.Hash, len(subtree0Leaves))
	copy(trueLeaves, subtree0Leaves)
	trueLeaves[0] = coinbaseTxID
	return buildSTUMP(trueLeaves, 0, blockHeight)
}

// buildFullSTUMP constructs a STUMP containing ALL level-0 hashes for a subtree.
func buildFullSTUMP(leaves []chainhash.Hash, txOffset uint64, blockHeight uint32) []byte {
	tree := buildMerkleTree(leaves)
	numLevels := len(tree) - 1
	if numLevels < 1 {
		numLevels = 1
	}

	mp := &transaction.MerklePath{
		BlockHeight: blockHeight,
		Path:        make([][]*transaction.PathElement, numLevels),
	}

	for i, h := range leaves {
		hashCopy := h
		isTxid := (uint64(i) == txOffset)
		mp.Path[0] = append(mp.Path[0], &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
			Txid:   &isTxid,
		})
	}

	offset := txOffset
	for level := 1; level < numLevels; level++ {
		offset = offset >> 1
		sibOffset := offset ^ 1
		levelHashes := tree[level]
		if len(levelHashes)%2 == 1 {
			levelHashes = append(levelHashes, levelHashes[len(levelHashes)-1])
		}
		if sibOffset < uint64(len(levelHashes)) {
			h := levelHashes[sibOffset]
			mp.Path[level] = append(mp.Path[level], &transaction.PathElement{
				Offset: sibOffset,
				Hash:   &h,
			})
		}
	}

	return mp.Bytes()
}

// multiSubtreeTestSetup creates a block with numSubtrees subtrees of subtreeSize txs each.
func multiSubtreeTestSetup(numSubtrees, subtreeSize int) (allLeaves [][]chainhash.Hash, subtreeHashes []chainhash.Hash, blockRoot chainhash.Hash) {
	allLeaves = make([][]chainhash.Hash, numSubtrees)
	subtreeHashes = make([]chainhash.Hash, numSubtrees)

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
		subtreeHashes[s] = computeMerkleRootFromLeaves(leaves)
		offset += subtreeSize
	}

	blockRoot = computeMerkleRootFromLeaves(subtreeHashes)
	return
}

// setupCoinbaseBlock creates a block with a coinbase placeholder at subtree 0, offset 0.
func setupCoinbaseBlock(numSubtrees, subtreeSize int) (
	allLeaves [][]chainhash.Hash,
	trueAllLeaves [][]chainhash.Hash,
	subtreeHashes []chainhash.Hash,
	coinbaseTxID chainhash.Hash,
	trueBlockRoot chainhash.Hash,
) {
	placeholder := chainhash.Hash{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	coinbaseTxID = generateTxHashes(1)[0]

	allLeaves = make([][]chainhash.Hash, numSubtrees)
	trueAllLeaves = make([][]chainhash.Hash, numSubtrees)

	for s := range numSubtrees {
		allLeaves[s] = make([]chainhash.Hash, subtreeSize)
		trueAllLeaves[s] = make([]chainhash.Hash, subtreeSize)
		for i := range subtreeSize {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(5000+s*100+i))
			h := sha256.Sum256(buf[:])
			hash, _ := chainhash.NewHash(h[:])
			allLeaves[s][i] = *hash
			trueAllLeaves[s][i] = *hash
		}
	}

	allLeaves[0][0] = placeholder
	trueAllLeaves[0][0] = coinbaseTxID

	subtreeHashes = make([]chainhash.Hash, numSubtrees)
	for s := range numSubtrees {
		subtreeHashes[s] = computeMerkleRootFromLeaves(allLeaves[s])
	}

	trueBlockRoot = computeBlockMerkleRoot(trueAllLeaves)
	return
}

// --- Sanity Tests for Helpers ---

func TestBuildMerkleTree_SanityCheck(t *testing.T) {
	leaves := generateTxHashes(4)
	ourRoot := computeMerkleRootFromLeaves(leaves)

	height := int(math.Ceil(math.Log2(float64(len(leaves)))))
	mp := &transaction.MerklePath{
		BlockHeight: 100,
		Path:        make([][]*transaction.PathElement, height),
	}
	for i, h := range leaves {
		hashCopy := h
		isTxid := true
		addLeaf(mp, 0, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
			Txid:   &isTxid,
		})
	}
	computeMissingHashes(mp)
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
	ourRoot := computeMerkleRootFromLeaves(leaves)

	// For 3 leaves, our buildMerkleTree duplicates the last to get 4,
	// so height = log2(4) = 2. Verify root via a STUMP round-trip.
	stump := buildSTUMP(leaves, 0, 100)
	result, _, err := AssembleBUMP(stump, 0, []chainhash.Hash{ourRoot}, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP failed: %v", err)
	}
	sdkRoot, err := result.ComputeRoot(&leaves[0])
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}

	if ourRoot != *sdkRoot {
		t.Fatalf("buildMerkleTree root (odd) %s != assembled root %s", ourRoot, sdkRoot)
	}
}

// --- Single-Subtree Tests ---

func TestAssembleBUMP_SingleSubtree_2txs_Offset1(t *testing.T) {
	leaves := generateTxHashes(2)
	expectedRoot := computeMerkleRootFromLeaves(leaves)
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
	expectedRoot := computeMerkleRootFromLeaves(leaves)
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
	expectedRoot := computeMerkleRootFromLeaves(leaves)
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
	expectedRoot := computeMerkleRootFromLeaves(leaves)
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
	leaves0 := generateTxHashes(8)
	leaves1 := make([]chainhash.Hash, 4)
	for i := range 4 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(100+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		leaves1[i] = *hash
	}

	subtreeHashes := []chainhash.Hash{
		computeMerkleRootFromLeaves(leaves0),
		computeMerkleRootFromLeaves(leaves1),
	}
	blockRoot := computeMerkleRootFromLeaves(subtreeHashes)

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

	stump := buildSTUMP(subtree0Leaves, 1, 950000)

	trueSubtree0Leaves := make([]chainhash.Hash, 4)
	copy(trueSubtree0Leaves, subtree0Leaves)
	trueSubtree0Leaves[0] = coinbaseTxID

	subtreeHashes := []chainhash.Hash{
		computeMerkleRootFromLeaves(subtree0Leaves),
		computeMerkleRootFromLeaves(subtree1Leaves),
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
		computeMerkleRootFromLeaves(subtree0Leaves),
		computeMerkleRootFromLeaves(subtree1Leaves),
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
		computeMerkleRootFromLeaves(subtree0Leaves),
		computeMerkleRootFromLeaves(subtree1Leaves),
	}
	trueBlockRoot := computeBlockMerkleRoot([][]chainhash.Hash{trueSubtree0Leaves, subtree1Leaves})

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
		subtreeHashes[s] = computeMerkleRootFromLeaves(allLeaves[s])
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
	expectedRoot := computeMerkleRootFromLeaves(leaves)
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

	stump1 := buildSTUMP(allLeaves[1], 2, 970000)
	result1, _, err := AssembleBUMP(stump1, 1, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("AssembleBUMP (subtree 1) failed: %v", err)
	}

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

// --- Compound BUMP Tests ---

func TestBuildCompoundBUMP_AllTxsExtractable(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)

	stump0 := buildFullSTUMP(allLeaves[0], 0, 990000)
	stump1 := buildFullSTUMP(allLeaves[1], 0, 990000)

	stumps := []*models.Stump{
		{BlockHash: "blockhash", SubtreeIndex: 0, StumpData: stump0},
		{BlockHash: "blockhash", SubtreeIndex: 1, StumpData: stump1},
	}

	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}

	if len(txids) != 8 {
		t.Fatalf("expected 8 tracked txids, got %d", len(txids))
	}

	bumpData := compound.Bytes()

	for s := range 2 {
		for i := range 4 {
			txHash := allLeaves[s][i]
			txid := txHash.String()

			parsed, parseErr := transaction.NewMerklePathFromBinary(bumpData)
			if parseErr != nil {
				t.Fatalf("failed to parse compound BUMP: %v", parseErr)
			}

			var txOffset uint64
			found := false
			for _, leaf := range parsed.Path[0] {
				if leaf.Hash != nil && *leaf.Hash == txHash {
					txOffset = leaf.Offset
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("subtree %d tx %d (%s) not found in compound BUMP level 0", s, i, txid)
			}

			minimal := ExtractMinimalPath(parsed, txOffset)
			root, rootErr := minimal.ComputeRoot(&txHash)
			if rootErr != nil {
				t.Fatalf("ComputeRoot failed for subtree %d tx %d: %v", s, i, rootErr)
			}
			if *root != blockRoot {
				t.Fatalf("root mismatch for subtree %d tx %d: got %s, want %s", s, i, root, blockRoot)
			}
		}
	}
}

func TestAssembleBUMP_LargeBlock_16Subtrees_32Txs(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(16, 32)

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

// --- Coinbase Replacement in Compound BUMP Tests ---

func TestBuildCompoundBUMP_CoinbaseReplacement(t *testing.T) {
	allLeaves, trueAllLeaves, subtreeHashes, coinbaseTxID, trueBlockRoot := setupCoinbaseBlock(2, 4)
	placeholder := allLeaves[0][0]

	stump0 := buildFullSTUMP(allLeaves[0], 1, 1000000)
	stump1 := buildFullSTUMP(allLeaves[1], 2, 1000000)

	stumps := []*models.Stump{
		{BlockHash: "block1", SubtreeIndex: 0, StumpData: stump0},
		{BlockHash: "block1", SubtreeIndex: 1, StumpData: stump1},
	}

	cbBUMP := buildCoinbaseBUMP(allLeaves[0], coinbaseTxID, 1000000)

	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, cbBUMP)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}

	if len(txids) == 0 {
		t.Fatal("expected tracked txids, got none")
	}

	bumpData := compound.Bytes()
	parsed, err := transaction.NewMerklePathFromBinary(bumpData)
	if err != nil {
		t.Fatalf("failed to parse compound BUMP: %v", err)
	}
	for _, leaf := range parsed.Path[0] {
		if leaf.Offset == 0 {
			if leaf.Hash != nil && *leaf.Hash == placeholder {
				t.Fatal("placeholder still present at level 0 offset 0")
			}
			if leaf.Hash == nil || *leaf.Hash != coinbaseTxID {
				t.Fatalf("expected coinbase txid at level 0 offset 0, got %v", leaf.Hash)
			}
			break
		}
	}

	for s := range 2 {
		for i := range 4 {
			txHash := trueAllLeaves[s][i]
			parsed, err := transaction.NewMerklePathFromBinary(bumpData)
			if err != nil {
				t.Fatalf("failed to parse compound BUMP: %v", err)
			}
			var txOffset uint64
			found := false
			for _, leaf := range parsed.Path[0] {
				if leaf.Hash != nil && *leaf.Hash == txHash {
					txOffset = leaf.Offset
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("subtree %d tx %d not found in compound BUMP", s, i)
			}
			minimal := ExtractMinimalPath(parsed, txOffset)
			root, err := minimal.ComputeRoot(&txHash)
			if err != nil {
				t.Fatalf("ComputeRoot failed for subtree %d tx %d: %v", s, i, err)
			}
			if *root != trueBlockRoot {
				t.Fatalf("root mismatch for subtree %d tx %d: got %s, want %s", s, i, root, trueBlockRoot)
			}
		}
	}
}

func TestBuildCompoundBUMP_CoinbaseReplacement_SingleSubtree(t *testing.T) {
	allLeaves, trueAllLeaves, subtreeHashes, coinbaseTxID, trueBlockRoot := setupCoinbaseBlock(1, 4)
	placeholder := allLeaves[0][0]

	stump0 := buildFullSTUMP(allLeaves[0], 2, 1000001)

	stumps := []*models.Stump{
		{BlockHash: "block2", SubtreeIndex: 0, StumpData: stump0},
	}

	cbBUMP := buildCoinbaseBUMP(allLeaves[0], coinbaseTxID, 1000001)

	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, cbBUMP)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}

	bumpData := compound.Bytes()
	parsed, err := transaction.NewMerklePathFromBinary(bumpData)
	if err != nil {
		t.Fatalf("failed to parse compound BUMP: %v", err)
	}

	for _, leaf := range parsed.Path[0] {
		if leaf.Offset == 0 {
			if leaf.Hash != nil && *leaf.Hash == placeholder {
				t.Fatal("placeholder still present at level 0 offset 0")
			}
			break
		}
	}

	for i := range 4 {
		txHash := trueAllLeaves[0][i]
		parsed, err := transaction.NewMerklePathFromBinary(bumpData)
		if err != nil {
			t.Fatalf("failed to parse compound BUMP: %v", err)
		}
		var txOffset uint64
		found := false
		for _, leaf := range parsed.Path[0] {
			if leaf.Hash != nil && *leaf.Hash == txHash {
				txOffset = leaf.Offset
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("tx %d not found in compound BUMP", i)
		}
		minimal := ExtractMinimalPath(parsed, txOffset)
		root, err := minimal.ComputeRoot(&txHash)
		if err != nil {
			t.Fatalf("ComputeRoot failed for tx %d: %v", i, err)
		}
		if *root != trueBlockRoot {
			t.Fatalf("root mismatch for tx %d: got %s, want %s", i, root, trueBlockRoot)
		}
	}
}

func TestApplyCoinbaseToSTUMP_ClearsStaleHashes(t *testing.T) {
	placeholder := chainhash.Hash{0xff, 0xff, 0xff, 0xff}
	coinbaseTxID := generateTxHashes(1)[0]

	leaves := make([]chainhash.Hash, 4)
	leaves[0] = placeholder
	for i := 1; i < 4; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(7000+i))
		h := sha256.Sum256(buf[:])
		hash, _ := chainhash.NewHash(h[:])
		leaves[i] = *hash
	}

	tree := buildMerkleTree(leaves)

	numLevels := len(tree) - 1
	mp := &transaction.MerklePath{
		BlockHeight: 100,
		Path:        make([][]*transaction.PathElement, numLevels),
	}

	for i, h := range leaves {
		hashCopy := h
		isTxid := (i == 1)
		mp.Path[0] = append(mp.Path[0], &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
			Txid:   &isTxid,
		})
	}

	for level := 1; level < numLevels; level++ {
		staleHash := tree[level][0]
		mp.Path[level] = append(mp.Path[level], &transaction.PathElement{
			Offset: 0,
			Hash:   &staleHash,
		})
	}

	applyCoinbaseToSTUMP(mp, &coinbaseTxID, nil)

	found := false
	for _, leaf := range mp.Path[0] {
		if leaf.Offset == 0 {
			if leaf.Hash == nil || *leaf.Hash != coinbaseTxID {
				t.Fatalf("expected coinbase at level 0 offset 0, got %v", leaf.Hash)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no element at level 0 offset 0")
	}

	for level := 1; level < numLevels; level++ {
		for _, elem := range mp.Path[level] {
			if elem.Offset == 0 {
				t.Fatalf("stale hash at level %d offset 0 was not removed", level)
			}
		}
	}
}

// --- ExtractMinimalPathForTx Tests ---

func TestExtractMinimalPathForTx_AllTxsExtractable(t *testing.T) {
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(2, 4)

	stump0 := buildFullSTUMP(allLeaves[0], 0, 990000)
	stump1 := buildFullSTUMP(allLeaves[1], 0, 990000)

	stumps := []*models.Stump{
		{BlockHash: "blockhash", SubtreeIndex: 0, StumpData: stump0},
		{BlockHash: "blockhash", SubtreeIndex: 1, StumpData: stump1},
	}

	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}

	bumpData := compound.Bytes()

	for s := range 2 {
		for i := range 4 {
			txHash := allLeaves[s][i]
			txid := txHash.String()

			minimalBytes := ExtractMinimalPathForTx(bumpData, txid)
			if minimalBytes == nil {
				t.Fatalf("ExtractMinimalPathForTx returned nil for subtree %d tx %d (%s)", s, i, txid)
			}

			minimal, err := transaction.NewMerklePathFromBinary(minimalBytes)
			if err != nil {
				t.Fatalf("failed to parse minimal path for subtree %d tx %d: %v", s, i, err)
			}

			root, err := minimal.ComputeRoot(&txHash)
			if err != nil {
				t.Fatalf("ComputeRoot failed for subtree %d tx %d: %v", s, i, err)
			}
			if *root != blockRoot {
				t.Fatalf("root mismatch for subtree %d tx %d: got %s, want %s", s, i, root, blockRoot)
			}
		}
	}
}

func TestExtractMinimalPathForTx_TxNotFound(t *testing.T) {
	allLeaves, subtreeHashes, _ := multiSubtreeTestSetup(2, 4)

	stump0 := buildFullSTUMP(allLeaves[0], 0, 990000)
	stumps := []*models.Stump{
		{BlockHash: "blockhash", SubtreeIndex: 0, StumpData: stump0},
	}

	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes[:1], nil)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}

	bumpData := compound.Bytes()

	// Use a txid that is NOT in the compound BUMP
	fakeTxid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	result := ExtractMinimalPathForTx(bumpData, fakeTxid)
	if result != nil {
		t.Fatalf("expected nil for unknown txid, got %d bytes", len(result))
	}
}

func TestValidateCompoundRoot_Passes(t *testing.T) {
	// 4-subtree, 4-tx-per-subtree block: build the real compound via
	// BuildCompoundBUMP, then validate against the independently-computed
	// block root from test helpers. Both must agree.
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(4, 4)

	stumps := make([]*models.Stump, 4)
	for s := 0; s < 4; s++ {
		stumps[s] = &models.Stump{
			BlockHash:    "deadbeef",
			SubtreeIndex: s,
			StumpData:    buildFullSTUMP(allLeaves[s], 0, 700000),
		}
	}

	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}
	if err := ValidateCompoundRoot(compound, &blockRoot); err != nil {
		t.Fatalf("ValidateCompoundRoot returned error for valid compound: %v", err)
	}
}

func TestValidateCompoundRoot_RejectsMismatch(t *testing.T) {
	allLeaves, subtreeHashes, _ := multiSubtreeTestSetup(2, 4)

	stumps := make([]*models.Stump, 2)
	for s := 0; s < 2; s++ {
		stumps[s] = &models.Stump{
			BlockHash:    "deadbeef",
			SubtreeIndex: s,
			StumpData:    buildFullSTUMP(allLeaves[s], 0, 700000),
		}
	}

	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}

	wrong := chainhash.Hash{}
	for i := range wrong {
		wrong[i] = 0xcc
	}
	err = ValidateCompoundRoot(compound, &wrong)
	if err == nil {
		t.Fatal("expected error for mismatched root, got nil")
	}
	// Error should reference both the computed and expected roots so logs can
	// surface the diff without extra formatting.
	if !strings.Contains(err.Error(), wrong.String()) {
		t.Errorf("expected expected-root in error, got: %v", err)
	}
}

func TestValidateCompoundRoot_RejectsEmptyInputs(t *testing.T) {
	// nil expected
	realHash := chainhash.Hash{}
	empty := &transaction.MerklePath{Path: [][]*transaction.PathElement{{{Hash: &realHash}}}}
	if err := ValidateCompoundRoot(empty, nil); err == nil {
		t.Error("expected error for nil expected, got nil")
	}

	// nil compound
	if err := ValidateCompoundRoot(nil, &realHash); err == nil {
		t.Error("expected error for nil compound, got nil")
	}

	// compound with empty path
	emptyPath := &transaction.MerklePath{}
	if err := ValidateCompoundRoot(emptyPath, &realHash); err == nil {
		t.Error("expected error for empty path, got nil")
	}

	// compound whose level-0 entries have no hashes at all
	dupTrue := true
	noHash := &transaction.MerklePath{Path: [][]*transaction.PathElement{{{Offset: 0, Duplicate: &dupTrue}}}}
	if err := ValidateCompoundRoot(noHash, &realHash); err == nil {
		t.Error("expected error when level-0 has no hash, got nil")
	}
}

func TestExtractMinimalPathForTx_InvalidInput(t *testing.T) {
	// Invalid binary data
	result := ExtractMinimalPathForTx([]byte{0xff, 0xff, 0xff}, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if result != nil {
		t.Fatal("expected nil for invalid BUMP binary")
	}

	// Invalid txid (not hex)
	result = ExtractMinimalPathForTx([]byte{0x01, 0x01, 0x01, 0x00, 0x01}, "not-a-valid-hex-txid")
	if result != nil {
		t.Fatal("expected nil for invalid txid")
	}

	// Nil input
	result = ExtractMinimalPathForTx(nil, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if result != nil {
		t.Fatal("expected nil for nil BUMP data")
	}
}

// --- Non-power-of-2 subtree count regression tests ---
//
// These reproduce a production failure where a block with 39 subtrees produced
// "we do not have a hash for this index at height: 11" during ComputeRoot.
// Root cause: bump.assembleFullPath populated the subtree-root layer with the
// real N hashes but emitted no duplicate-padding entries for the odd slots
// that Bitcoin's canonical merkle-root algorithm requires above the subtree
// layer, so the climb hit a hole at the first odd level. Every prior
// multi-subtree test used power-of-2 counts (2/4/8/16) so this was untested.

// TestAssembleBUMP_NonPow2SubtreeCounts exercises AssembleBUMP for every
// non-power-of-2 subtree count we're likely to see in production. For each,
// it assembles the BUMP for a tracked tx in every subtree and climbs the
// tree back to the root — the computed root must equal the canonical
// Bitcoin merkle root of the subtree roots.
func TestAssembleBUMP_NonPow2SubtreeCounts(t *testing.T) {
	cases := []struct {
		name        string
		numSubtrees int
		subtreeSize int
	}{
		{"3subtrees_4txs", 3, 4},
		{"5subtrees_4txs", 5, 4},
		{"7subtrees_4txs", 7, 4},
		{"17subtrees_8txs", 17, 8},
		{"33subtrees_8txs", 33, 8},
		{"39subtrees_8txs_productionShape", 39, 8},
		{"63subtrees_4txs", 63, 4},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(tc.numSubtrees, tc.subtreeSize)

			// For every subtree, pick a txid and assemble its BUMP. Every one
			// must climb to blockRoot — the climb path varies across subtrees
			// so the duplicate-padding slots must be correct at every level.
			for s := 0; s < tc.numSubtrees; s++ {
				txOffset := uint64(s % tc.subtreeSize)
				stump := buildSTUMP(allLeaves[s], txOffset, 900000)
				result, _, err := AssembleBUMP(stump, s, subtreeHashes, nil)
				if err != nil {
					t.Fatalf("subtree %d: AssembleBUMP failed: %v", s, err)
				}
				root, err := result.ComputeRoot(&allLeaves[s][txOffset])
				if err != nil {
					t.Fatalf("subtree %d: ComputeRoot failed: %v", s, err)
				}
				if *root != blockRoot {
					t.Fatalf("subtree %d: root mismatch: got %s want %s", s, root, blockRoot)
				}
			}
		})
	}
}

// TestBuildCompoundBUMP_NonPow2SubtreeCounts builds the compound BUMP for a
// block with non-pow2 subtree count, then validates every txid at level 0
// climbs to the canonical block root. This reproduces the production failure
// end-to-end: BuildCompoundBUMP → ValidateCompoundRoot → per-tx ComputeRoot.
func TestBuildCompoundBUMP_NonPow2SubtreeCounts(t *testing.T) {
	cases := []struct {
		name        string
		numSubtrees int
		subtreeSize int
	}{
		{"3subtrees_4txs", 3, 4},
		{"5subtrees_4txs", 5, 4},
		{"7subtrees_8txs", 7, 8},
		{"17subtrees_8txs", 17, 8},
		{"33subtrees_4txs", 33, 4},
		{"39subtrees_8txs_productionShape", 39, 8},
		{"63subtrees_4txs", 63, 4},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(tc.numSubtrees, tc.subtreeSize)

			stumps := make([]*models.Stump, tc.numSubtrees)
			for s := 0; s < tc.numSubtrees; s++ {
				stumps[s] = &models.Stump{
					BlockHash:    "deadbeef",
					SubtreeIndex: s,
					StumpData:    buildFullSTUMP(allLeaves[s], 0, 700000),
				}
			}

			compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil)
			if err != nil {
				t.Fatalf("BuildCompoundBUMP failed: %v", err)
			}

			// Header-root validation — the failure mode in production.
			if err := ValidateCompoundRoot(compound, &blockRoot); err != nil {
				t.Fatalf("ValidateCompoundRoot failed: %v", err)
			}

			// Probe every txid in every subtree — the climb from each must
			// reach the same block root. The production bug manifested as
			// different "height: N" errors depending on which leaf was chosen
			// first, so asserting every leaf catches those variants.
			for s := 0; s < tc.numSubtrees; s++ {
				for i := 0; i < tc.subtreeSize; i++ {
					leaf := allLeaves[s][i]
					root, err := compound.ComputeRoot(&leaf)
					if err != nil {
						t.Fatalf("subtree %d tx %d: ComputeRoot: %v", s, i, err)
					}
					if *root != blockRoot {
						t.Fatalf("subtree %d tx %d: root mismatch: got %s want %s", s, i, root, blockRoot)
					}
				}
			}
		})
	}
}

// TestBuildCompoundBUMP_39Subtrees_StructuralShape pins down the exact layout
// of the compound BUMP for the production-shape block (39 subtrees). The
// failure dump from block 000...fff29 showed no duplicate=true entries
// anywhere and missing upper-level offsets; after the fix the compound must
// contain Bitcoin's canonical padding at each odd level.
func TestBuildCompoundBUMP_39Subtrees_StructuralShape(t *testing.T) {
	const (
		numSubtrees = 39
		subtreeSize = 4 // height 2 — keeps the test fast, pads at levels 2, 5, 6
	)
	allLeaves, subtreeHashes, blockRoot := multiSubtreeTestSetup(numSubtrees, subtreeSize)

	stumps := make([]*models.Stump, numSubtrees)
	for s := 0; s < numSubtrees; s++ {
		stumps[s] = &models.Stump{
			BlockHash:    "deadbeef",
			SubtreeIndex: s,
			StumpData:    buildFullSTUMP(allLeaves[s], 0, 700000),
		}
	}
	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, nil)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}
	if err := ValidateCompoundRoot(compound, &blockRoot); err != nil {
		t.Fatalf("ValidateCompoundRoot failed: %v", err)
	}

	// With subtreeSize=4 (internal height 2) and 39 subtrees (6 block levels),
	// the canonical Bitcoin merkle tree padding produces exactly these counts
	// and these duplicate-marker offsets. If this test ever starts failing,
	// padAndComputeBlockLevel has regressed — inspect which level diverges.
	want := []struct {
		level    int
		count    int
		dupAt    []uint64 // offsets carrying Duplicate:true
		minReal  uint64   // lowest real-hash offset
		maxReal  uint64   // highest real-hash offset
	}{
		{level: 0, count: 39 * 4, minReal: 0, maxReal: 39*4 - 1},
		{level: 1, count: 39 * 2, minReal: 0, maxReal: 39*2 - 1},
		{level: 2, count: 40, dupAt: []uint64{39}, minReal: 0, maxReal: 38}, // subtree-root layer
		{level: 3, count: 20, minReal: 0, maxReal: 19},
		{level: 4, count: 10, minReal: 0, maxReal: 9},
		{level: 5, count: 6, dupAt: []uint64{5}, minReal: 0, maxReal: 4},
		{level: 6, count: 4, dupAt: []uint64{3}, minReal: 0, maxReal: 2},
		{level: 7, count: 2, minReal: 0, maxReal: 1},
	}
	if got, wantLen := len(compound.Path), len(want); got != wantLen {
		t.Fatalf("compound path levels = %d, want %d", got, wantLen)
	}
	for _, exp := range want {
		elems := compound.Path[exp.level]
		if len(elems) != exp.count {
			t.Errorf("level %d: count = %d, want %d", exp.level, len(elems), exp.count)
		}
		dupSeen := map[uint64]bool{}
		realOffsets := []uint64{}
		for _, e := range elems {
			if isDuplicate(e) {
				dupSeen[e.Offset] = true
				if e.Hash != nil {
					t.Errorf("level %d offset %d: Duplicate entry must not carry a hash", exp.level, e.Offset)
				}
				continue
			}
			if e.Hash == nil {
				t.Errorf("level %d offset %d: non-duplicate entry missing hash", exp.level, e.Offset)
			}
			realOffsets = append(realOffsets, e.Offset)
		}
		for _, off := range exp.dupAt {
			if !dupSeen[off] {
				t.Errorf("level %d: missing Duplicate entry at offset %d (seen=%v)", exp.level, off, dupSeen)
			}
		}
		if len(exp.dupAt) == 0 && len(dupSeen) > 0 {
			t.Errorf("level %d: unexpected Duplicate entries at %v", exp.level, dupSeen)
		}
		if len(realOffsets) > 0 {
			minO, maxO := realOffsets[0], realOffsets[0]
			for _, o := range realOffsets {
				if o < minO {
					minO = o
				}
				if o > maxO {
					maxO = o
				}
			}
			if minO != exp.minReal || maxO != exp.maxReal {
				t.Errorf("level %d: real-offset range = [%d,%d], want [%d,%d]",
					exp.level, minO, maxO, exp.minReal, exp.maxReal)
			}
		}
	}
}

// TestBuildCompoundBUMP_NonPow2_WithCoinbase exercises the interaction between
// coinbase placeholder replacement (which rewrites subtreeHashes[0] and
// scrubs level-0 offset 0 hashes) and the non-pow2 block-level padding. Both
// code paths mutate the same fullPath structure; this guards against one
// undoing the other.
func TestBuildCompoundBUMP_NonPow2_WithCoinbase(t *testing.T) {
	const (
		numSubtrees = 5
		subtreeSize = 4
	)
	allLeaves, trueAllLeaves, subtreeHashes, coinbaseTxID, trueBlockRoot := setupCoinbaseBlock(numSubtrees, subtreeSize)
	cbBUMP := buildCoinbaseBUMP(trueAllLeaves[0], coinbaseTxID, 700000)

	stumps := make([]*models.Stump, numSubtrees)
	for s := 0; s < numSubtrees; s++ {
		stumps[s] = &models.Stump{
			BlockHash:    "deadbeef",
			SubtreeIndex: s,
			StumpData:    buildFullSTUMP(allLeaves[s], 0, 700000),
		}
	}

	compound, _, err := BuildCompoundBUMP(stumps, subtreeHashes, cbBUMP)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}
	if err := ValidateCompoundRoot(compound, &trueBlockRoot); err != nil {
		t.Fatalf("ValidateCompoundRoot failed: %v", err)
	}
	// Every tx (including the coinbase at subtree 0, offset 0) climbs cleanly.
	for s := 0; s < numSubtrees; s++ {
		for i := 0; i < subtreeSize; i++ {
			leaf := trueAllLeaves[s][i]
			root, err := compound.ComputeRoot(&leaf)
			if err != nil {
				t.Fatalf("subtree %d tx %d: ComputeRoot: %v", s, i, err)
			}
			if *root != trueBlockRoot {
				t.Fatalf("subtree %d tx %d: root mismatch", s, i)
			}
		}
	}
}

// TestComputeMissingHashes_HandlesDuplicateSibling verifies the extended
// computeMissingHashes branch: when a node's sibling carries Duplicate:true,
// the parent is MerkleTreeParent(node, node), matching BRC-74 verifier
// semantics. Before the fix, computeMissingHashes silently skipped such
// pairs, which produced holes in the compound BUMP whenever a subtree (or
// block) level needed an odd-leaf duplicate.
func TestComputeMissingHashes_HandlesDuplicateSibling(t *testing.T) {
	dupTrue := true
	leaves := generateTxHashes(3) // odd → canonical merkle pads the last
	// Manually construct a 2-level path with real hashes + a Duplicate marker
	// at the odd position.
	l0 := []*transaction.PathElement{
		{Offset: 0, Hash: &leaves[0]},
		{Offset: 1, Hash: &leaves[1]},
		{Offset: 2, Hash: &leaves[2]},
		{Offset: 3, Duplicate: &dupTrue},
	}
	mp := &transaction.MerklePath{
		BlockHeight: 100,
		Path:        [][]*transaction.PathElement{l0, nil},
	}
	computeMissingHashes(mp)

	if len(mp.Path[1]) != 2 {
		t.Fatalf("level 1: count = %d, want 2", len(mp.Path[1]))
	}
	parent0 := findLeafByOffset(mp, 1, 0)
	parent1 := findLeafByOffset(mp, 1, 1)
	if parent0 == nil || parent0.Hash == nil {
		t.Fatal("missing parent at level 1 offset 0")
	}
	if parent1 == nil || parent1.Hash == nil {
		t.Fatal("missing parent at level 1 offset 1 (should be merkle(leaf2, leaf2))")
	}
	want := transaction.MerkleTreeParent(&leaves[2], &leaves[2])
	if *parent1.Hash != *want {
		t.Errorf("parent1 = %s, want merkle(leaf2, leaf2) = %s", parent1.Hash, want)
	}
}

// --- Coinbase pre-correction order-independence regression ---
//
// Production incident (block 00000000847b80c9360e93d0141477abbd3c6c3510ca359c005738c9a07560a8,
// arcade-v2-ttn): BuildCompoundBUMP failed ValidateCompoundRoot whenever the
// stumps slice was not in subtree-index order. Root cause:
// computeCorrectedSubtreeRoot returned mp.Path[height-1][0].Hash (the LEFT
// child of the subtree root) instead of H(mp.Path[height-1][0],
// mp.Path[height-1][1]) (the root). subtreeHashes[0] was silently corrupted.
//
// The merge in BuildCompoundBUMP dedupes by (level, offset) with first-seen
// wins. When subtree 0 is processed first, its own assembleFullPath
// independently climbs from level 0 and places the CORRECT root at
// (internalHeight, 0) before any other subtree writes the corrupt value
// there — the merge preserves the correct entry and validation passes. When
// any other subtree is first, it writes the corrupt subtreeHashes[0] into the
// same slot and subtree 0's later correct value is ignored. Aerospike returns
// stumps in non-deterministic order, so prod surfaced it once the order
// happened to be [2,4,5,3,0,1].
//
// This test pins down the fix by running the exact production-shape
// permutation plus several other orderings. All must validate.

func TestBuildCompoundBUMP_CoinbaseReplacement_OrderIndependence(t *testing.T) {
	const (
		numSubtrees = 6
		subtreeSize = 8 // pow2 ≥2 is sufficient to reproduce; larger only adds runtime
	)
	allLeaves, trueAllLeaves, subtreeHashes, coinbaseTxID, trueBlockRoot := setupCoinbaseBlock(numSubtrees, subtreeSize)
	cbBUMP := buildCoinbaseBUMP(trueAllLeaves[0], coinbaseTxID, 700000)

	orderings := [][]int{
		{0, 1, 2, 3, 4, 5}, // baseline — this was the only order the old code happened to handle
		{5, 4, 3, 2, 1, 0},
		{2, 4, 5, 3, 0, 1}, // production Aerospike order that triggered the incident
		{1, 0, 2, 3, 4, 5}, // subtree 0 second — minimal shift that still breaks old code
		{3, 2, 1, 5, 4, 0}, // subtree 0 last
	}

	for _, order := range orderings {
		order := order
		t.Run(fmt.Sprintf("order_%v", order), func(t *testing.T) {
			// Each run needs a fresh subtreeHashes copy — BuildCompoundBUMP
			// mutates subtreeHashes[0] via the pre-correction step.
			localHashes := make([]chainhash.Hash, len(subtreeHashes))
			copy(localHashes, subtreeHashes)

			stumps := make([]*models.Stump, numSubtrees)
			for i, idx := range order {
				stumps[i] = &models.Stump{
					BlockHash:    "deadbeef",
					SubtreeIndex: idx,
					StumpData:    buildFullSTUMP(allLeaves[idx], 0, 700000),
				}
			}

			compound, _, err := BuildCompoundBUMP(stumps, localHashes, cbBUMP)
			if err != nil {
				t.Fatalf("BuildCompoundBUMP failed: %v", err)
			}
			if err := ValidateCompoundRoot(compound, &trueBlockRoot); err != nil {
				t.Fatalf("ValidateCompoundRoot failed (order %v): %v", order, err)
			}

			// Every true txid (coinbase + non-coinbase, all subtrees) must
			// climb to the canonical block root. This catches subtler variants
			// where the compound passes header validation but individual
			// per-tx paths diverge.
			for s := 0; s < numSubtrees; s++ {
				for i := 0; i < subtreeSize; i++ {
					leaf := trueAllLeaves[s][i]
					root, err := compound.ComputeRoot(&leaf)
					if err != nil {
						t.Fatalf("subtree %d tx %d: ComputeRoot: %v", s, i, err)
					}
					if *root != trueBlockRoot {
						t.Fatalf("subtree %d tx %d: root mismatch (order %v): got %s want %s",
							s, i, order, root, trueBlockRoot)
					}
				}
			}
		})
	}
}

// TestComputeCorrectedSubtreeRoot_ReturnsRealRoot targets the buggy function
// directly: for a known subtree-0 layout (placeholder at offset 0 + known
// coinbase), the returned hash must equal the merkle root of the same leaves
// with offset 0 replaced by the real coinbase txid — NOT the left child of
// that root. Covers multiple subtree heights so no pow2 case regresses.
func TestComputeCorrectedSubtreeRoot_ReturnsRealRoot(t *testing.T) {
	sizes := []int{2, 4, 8, 16, 32, 1024}
	for _, size := range sizes {
		size := size
		t.Run(fmt.Sprintf("subtreeSize_%d", size), func(t *testing.T) {
			allLeaves, trueAllLeaves, _, coinbaseTxID, _ := setupCoinbaseBlock(1, size)
			stumpData := buildFullSTUMP(allLeaves[0], 0, 700000)

			got, err := computeCorrectedSubtreeRoot(stumpData, &coinbaseTxID)
			if err != nil {
				t.Fatalf("computeCorrectedSubtreeRoot failed: %v", err)
			}
			want := computeMerkleRootFromLeaves(trueAllLeaves[0])
			if *got != want {
				t.Fatalf("size=%d: got %s, want %s (actual subtree root)", size, got, want)
			}
		})
	}
}
