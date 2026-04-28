// Package bump implements BUMP (Bitcoin Unified Merkle Path) construction from STUMPs.
// This is ported directly from the reference arcade implementation.
package bump

import (
	"fmt"
	"math"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/models"
)

// AssembleBUMP constructs a minimal BUMP (block-level merkle proof) from a STUMP
// (subtree-level merkle path) and the block's subtree root hashes.
// The returned path contains only the nodes needed to verify the single tracked transaction.
//
// Parameters:
//   - stumpData: BRC-74 binary-encoded STUMP (subtree merkle path with local offsets)
//   - subtreeIndex: index of the subtree containing the tracked transaction
//   - subtreeHashes: root hashes for all subtrees in the block
//   - coinbaseBUMP: BRC-74 binary-encoded merkle path of the coinbase transaction; nil if unavailable.
//
// Returns the minimal merkle path for the tracked transaction (with global offsets) and the global tx offset.
func AssembleBUMP(stumpData []byte, subtreeIndex int, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) (*transaction.MerklePath, uint64, error) {
	fullPath, txOffset, err := assembleFullPath(stumpData, subtreeIndex, subtreeHashes, coinbaseBUMP)
	if err != nil {
		return nil, 0, err
	}
	minimalPath := ExtractMinimalPath(fullPath, txOffset)
	return minimalPath, txOffset, nil
}

// assembleFullPath constructs a full BUMP from a STUMP, retaining ALL level-0 hashes
// and intermediate nodes. This is used by BuildCompoundBUMP to preserve data for all
// tracked transactions, not just one.
func assembleFullPath(stumpData []byte, subtreeIndex int, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) (*transaction.MerklePath, uint64, error) {
	stumpPath, err := transaction.NewMerklePathFromBinary(stumpData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse STUMP: %w", err)
	}

	internalHeight := len(stumpPath.Path)

	// Handle coinbase placeholder replacement for subtree 0.
	if subtreeIndex == 0 && len(coinbaseBUMP) > 0 {
		coinbaseTxID := extractCoinbaseTxID(coinbaseBUMP)
		if coinbaseTxID != nil {
			applyCoinbaseToSTUMP(stumpPath, coinbaseTxID, coinbaseBUMP)
		}
	}

	numSubtrees := len(subtreeHashes)
	if numSubtrees <= 1 {
		// Single subtree: STUMP IS the full BUMP.
		computeMissingHashes(stumpPath)
		var txOffset uint64
		if internalHeight > 0 {
			for _, leaf := range stumpPath.Path[0] {
				if leaf.Txid != nil && *leaf.Txid {
					txOffset = leaf.Offset
					break
				}
			}
		}
		return stumpPath, txOffset, nil
	}

	// Multi-subtree: shift STUMP offsets from local (within subtree) to global (within block).
	for level := 0; level < internalHeight; level++ {
		shift := uint64(subtreeIndex) << uint(internalHeight-level) //nolint:gosec // safe
		for _, elem := range stumpPath.Path[level] {
			elem.Offset += shift
		}
	}

	var txOffset uint64
	if internalHeight > 0 {
		for _, leaf := range stumpPath.Path[0] {
			if leaf.Txid != nil && *leaf.Txid {
				txOffset = leaf.Offset
				break
			}
		}
	}

	// Build the full path: STUMP levels (global offsets) + subtree root layer
	subtreeRootLayer := int(math.Ceil(math.Log2(float64(numSubtrees))))
	totalHeight := internalHeight + subtreeRootLayer
	fullPath := &transaction.MerklePath{
		BlockHeight: stumpPath.BlockHeight,
		Path:        make([][]*transaction.PathElement, totalHeight),
	}

	for level := 0; level < internalHeight; level++ {
		for _, elem := range stumpPath.Path[level] {
			addLeaf(fullPath,level, elem)
		}
	}

	// Add subtree root hashes at the subtree root layer.
	for i, subHash := range subtreeHashes {
		if i == subtreeIndex {
			continue // our subtree root will be computed from STUMP leaves
		}
		hashCopy := subHash
		addLeaf(fullPath,internalHeight, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
		})
	}

	// First pass: compute the STUMP's own subtree root at internalHeight from
	// its level-0..internalHeight-1 data, and any block-level parents that
	// happen to have both siblings already present.
	computeMissingHashes(fullPath)

	// Second pass: Bitcoin-canonical padding above the subtree-root layer.
	// Required whenever numSubtrees is not a power of two — otherwise the
	// compound BUMP is missing siblings for every climb that touches the
	// padded region and ComputeRoot errors with "we do not have a hash for
	// this index at height: N".
	padAndComputeBlockLevel(fullPath, internalHeight, numSubtrees)
	return fullPath, txOffset, nil
}

// extractCoinbaseTxID parses a BRC-74 coinbase BUMP and returns the real
// coinbase txid (the hash at level 0, offset 0).
func extractCoinbaseTxID(coinbaseBUMP []byte) *chainhash.Hash {
	if len(coinbaseBUMP) == 0 {
		return nil
	}
	cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
	if err != nil || len(cbPath.Path) == 0 {
		return nil
	}
	for _, leaf := range cbPath.Path[0] {
		if leaf.Offset == 0 && leaf.Hash != nil {
			return leaf.Hash
		}
	}
	return nil
}

// applyCoinbaseToSTUMP replaces the coinbase placeholder at level 0, offset 0
// with the real coinbase txid, and removes any stale pre-computed hashes at
// offset 0 for higher levels so ComputeMissingHashes can recompute them correctly.
//
// For full STUMPs (all level-0 leaves present): replaces level 0 offset 0 and
// clears stale higher-level offset-0 hashes, letting ComputeMissingHashes rebuild them.
//
// For minimal STUMPs (only tracked tx path): if level 0 doesn't include offset 0,
// walks the coinbase BUMP to replace stale higher-level offset-0 hashes directly.
func applyCoinbaseToSTUMP(stumpPath *transaction.MerklePath, coinbaseTxID *chainhash.Hash, coinbaseBUMP []byte) {
	if coinbaseTxID == nil || len(stumpPath.Path) == 0 {
		return
	}

	// Check if level 0 has offset 0 (full STUMP includes the coinbase slot).
	level0HasOffset0 := false
	for _, elem := range stumpPath.Path[0] {
		if elem.Offset == 0 {
			h := *coinbaseTxID
			elem.Hash = &h
			level0HasOffset0 = true
			break
		}
	}

	if level0HasOffset0 {
		// Full STUMP: clear stale higher-level offset-0 hashes.
		// ComputeMissingHashes will recompute them from the corrected level-0 data.
		for level := 1; level < len(stumpPath.Path); level++ {
			filtered := make([]*transaction.PathElement, 0, len(stumpPath.Path[level]))
			for _, elem := range stumpPath.Path[level] {
				if elem.Offset != 0 {
					filtered = append(filtered, elem)
				}
			}
			stumpPath.Path[level] = filtered
		}
		return
	}

	// Minimal STUMP: level 0 doesn't contain offset 0, so we can't recompute
	// higher-level offset-0 hashes from level-0 data. Instead, walk the coinbase
	// BUMP to compute correct intermediate hashes and replace stale ones.
	cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
	if err != nil || len(cbPath.Path) == 0 {
		return
	}
	currentHash := coinbaseTxID
	internalHeight := len(stumpPath.Path)
	for level := 0; level < internalHeight && level < len(cbPath.Path); level++ {
		for _, elem := range stumpPath.Path[level] {
			if elem.Offset == 0 && (elem.Txid == nil || !*elem.Txid) {
				h := *currentHash
				elem.Hash = &h
				break
			}
		}
		sibling := findLeafByOffset(cbPath,level, 1)
		if sibling == nil || sibling.Hash == nil {
			break
		}
		currentHash = transaction.MerkleTreeParent(currentHash, sibling.Hash)
	}
}

// computeCorrectedSubtreeRoot parses a subtree 0 STUMP, replaces the placeholder
// with the real coinbase txid, and returns the corrected subtree root by climbing
// from the coinbase leaf via the BRC-74 ComputeRoot algorithm.
//
// The prior implementation built only the STUMP's level 0 and called
// computeMissingHashes, then returned mp.Path[height-1][0].Hash — but that
// element is the LEFT child of the subtree root, not the root itself
// (the root is H(mp.Path[height-1][0], mp.Path[height-1][1])). For any
// pow2-sized subtree ≥2 leaves, the returned value was a mid-level hash, and
// subtreeHashes[0] was silently corrupted. The compound BUMP merge uses
// first-seen-wins by (level, offset), so the bug only surfaced when subtree 0
// was not the first stump processed — i.e., it depended on the Aerospike read
// order and went undetected until a production block with 6 subtrees was
// delivered in order [2,4,5,3,0,1].
func computeCorrectedSubtreeRoot(stumpData []byte, coinbaseTxID *chainhash.Hash) (*chainhash.Hash, error) {
	stumpPath, err := transaction.NewMerklePathFromBinary(stumpData)
	if err != nil {
		return nil, err
	}
	applyCoinbaseToSTUMP(stumpPath, coinbaseTxID, nil)
	return stumpPath.ComputeRoot(coinbaseTxID)
}

// ExtractMinimalPath extracts the minimal set of nodes needed to verify a single
// transaction at the given offset from a full merkle path.
func ExtractMinimalPath(fullPath *transaction.MerklePath, txOffset uint64) *transaction.MerklePath {
	mp := &transaction.MerklePath{
		BlockHeight: fullPath.BlockHeight,
		Path:        make([][]*transaction.PathElement, len(fullPath.Path)),
	}

	offset := txOffset
	for level := 0; level < len(fullPath.Path); level++ {
		if level == 0 {
			if leaf := findLeafByOffset(fullPath,level, offset); leaf != nil {
				addLeaf(mp,level, leaf)
			}
		}
		if sibling := findLeafByOffset(fullPath,level, offset^1); sibling != nil {
			addLeaf(mp,level, sibling)
		}
		offset = offset >> 1
	}

	return mp
}

// ExtractMinimalPathForTx extracts a per-tx minimal merkle path from a compound BUMP.
// It parses the compound BUMP from binary, finds the txid at level 0, and extracts
// the minimal BRC-74 path needed to verify that specific transaction.
// Returns nil if the txid is not found or the input is invalid.
func ExtractMinimalPathForTx(bumpData []byte, txid string) []byte {
	compound, err := transaction.NewMerklePathFromBinary(bumpData)
	if err != nil {
		return nil
	}

	txHash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return nil
	}

	// Find the tx at level 0
	var txOffset uint64
	found := false
	if len(compound.Path) > 0 {
		for _, leaf := range compound.Path[0] {
			if leaf.Hash != nil && *leaf.Hash == *txHash {
				txOffset = leaf.Offset
				found = true
				break
			}
		}
	}
	if !found {
		return nil
	}

	minimal := ExtractMinimalPath(compound, txOffset)
	return minimal.Bytes()
}

// ExtractLevel0Hashes parses a BRC-74 STUMP binary and returns all level-0 hashes.
func ExtractLevel0Hashes(stumpData []byte) []chainhash.Hash {
	mp, err := transaction.NewMerklePathFromBinary(stumpData)
	if err != nil || len(mp.Path) == 0 {
		return nil
	}

	hashes := make([]chainhash.Hash, 0, len(mp.Path[0]))
	for _, leaf := range mp.Path[0] {
		if leaf.Hash != nil {
			hashes = append(hashes, *leaf.Hash)
		}
	}
	return hashes
}

// ValidateCompoundRoot computes the merkle root from the compound BUMP using
// any level-0 leaf and compares it against the expected block-header merkle
// root. Returns a descriptive error on mismatch so the caller can log both
// values and refuse to persist a broken compound.
func ValidateCompoundRoot(compound *transaction.MerklePath, expected *chainhash.Hash) error {
	if expected == nil {
		return fmt.Errorf("no block-header merkle root to validate against")
	}
	if compound == nil || len(compound.Path) == 0 || len(compound.Path[0]) == 0 {
		return fmt.Errorf("empty compound path")
	}
	var leaf *chainhash.Hash
	for _, l := range compound.Path[0] {
		if l.Hash != nil {
			leaf = l.Hash
			break
		}
	}
	if leaf == nil {
		return fmt.Errorf("no level-0 hash to compute root from")
	}
	got, err := compound.ComputeRoot(leaf)
	if err != nil {
		return fmt.Errorf("compute root from compound: %w", err)
	}
	if !got.IsEqual(expected) {
		return fmt.Errorf("computed root %s != block header merkle root %s", got, expected)
	}
	return nil
}

// BuildCompoundBUMP merges multiple per-subtree BUMPs into a single compound MerklePath
// containing all tracked transactions at level 0. The tracker is used to discover
// which level-0 hashes are tracked transactions.
func BuildCompoundBUMP(stumps []*models.Stump, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) (*transaction.MerklePath, []string, error) {
	if len(stumps) == 0 {
		return nil, nil, fmt.Errorf("no stumps to build compound BUMP")
	}

	coinbaseTxID := extractCoinbaseTxID(coinbaseBUMP)

	// Correct subtreeHashes[0] if coinbase is available.
	// The original subtreeHashes[0] may be computed from the placeholder, not the real coinbase.
	if coinbaseTxID != nil && len(subtreeHashes) > 0 {
		for _, stump := range stumps {
			if stump.SubtreeIndex == 0 {
				if correctedRoot, err := computeCorrectedSubtreeRoot(stump.StumpData, coinbaseTxID); err == nil {
					subtreeHashes[0] = *correctedRoot
				}
				break
			}
		}
	}

	// Assemble each STUMP into a full path, collect all elements by level
	var blockHeight uint32
	var txids []string
	allPaths := make([]*transaction.MerklePath, 0, len(stumps))

	for _, stump := range stumps {
		fullPath, _, err := assembleFullPath(stump.StumpData, stump.SubtreeIndex, subtreeHashes, coinbaseBUMP)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to assemble BUMP for subtree %d: %w", stump.SubtreeIndex, err)
		}
		blockHeight = fullPath.BlockHeight

		// Collect all level-0 hashes as candidate txids.
		// The caller's store will filter to only those that are tracked.
		for _, h := range ExtractLevel0Hashes(stump.StumpData) {
			txids = append(txids, h.String())
		}

		allPaths = append(allPaths, fullPath)
	}

	// Determine total height from the first path
	totalHeight := len(allPaths[0].Path)

	compound := &transaction.MerklePath{
		BlockHeight: blockHeight,
		Path:        make([][]*transaction.PathElement, totalHeight),
	}

	// Merge all path elements, deduplicating by (level, offset)
	type key struct {
		level  int
		offset uint64
	}
	seen := make(map[key]bool)

	for _, mp := range allPaths {
		for level := 0; level < len(mp.Path); level++ {
			for _, elem := range mp.Path[level] {
				k := key{level, elem.Offset}
				if seen[k] {
					continue
				}
				seen[k] = true
				addLeaf(compound,level, elem)
			}
		}
	}

	return compound, txids, nil
}
