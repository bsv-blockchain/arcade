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
	if err := validateSubtreeIndex(subtreeIndex, len(subtreeHashes)); err != nil {
		return nil, 0, err
	}
	fullPath, txOffset, err := assembleFullPath(stumpData, subtreeIndex, subtreeHashes, coinbaseBUMP)
	if err != nil {
		return nil, 0, err
	}
	minimalPath := ExtractMinimalPath(fullPath, txOffset)
	return minimalPath, txOffset, nil
}

// validateSubtreeIndex rejects subtreeIndex values that are negative or that fall
// outside the range of subtree hashes the block actually has. Without this guard
// a negative index wraps when converted to uint64 and corrupts every offset in
// the assembled BUMP, while an out-of-range positive index produces a path for a
// subtree that isn't present in the block.
func validateSubtreeIndex(subtreeIndex, numSubtrees int) error {
	if subtreeIndex < 0 || subtreeIndex >= numSubtrees {
		return fmt.Errorf("invalid subtree index %d for block with %d subtrees", subtreeIndex, numSubtrees)
	}
	return nil
}

// assembleFullPath constructs a full BUMP from a STUMP, retaining ALL level-0 hashes
// and intermediate nodes. This is used by BuildCompoundBUMP to preserve data for all
// tracked transactions, not just one.
func assembleFullPath(stumpData []byte, subtreeIndex int, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) (*transaction.MerklePath, uint64, error) {
	if err := validateSubtreeIndex(subtreeIndex, len(subtreeHashes)); err != nil {
		return nil, 0, err
	}
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
		// Single subtree: the STUMP covers the subtree-internal levels.
		computeMissingHashes(stumpPath)
		// The block tree may be taller than this subtree. When the coinbase tx
		// sits outside the subtree, the block merkle root is one or more levels
		// above the subtree root, and the STUMP's internal height is short of
		// the true block height. The coinbase BUMP is the authoritative full
		// block-level path (it computes the block-header merkle root from the
		// coinbase txid), so fold the subtree root up to the block root using
		// its left-spine siblings. Without this, a single-subtree block whose
		// coinbase adds a block level computes the subtree root instead of the
		// block root and fails ValidateCompoundRoot (issue #167).
		extendSubtreeToBlockRoot(stumpPath, coinbaseBUMP, internalHeight)
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
			addLeaf(fullPath, level, elem)
		}
	}

	// Add subtree root hashes at the subtree root layer.
	for i, subHash := range subtreeHashes {
		if i == subtreeIndex {
			continue // our subtree root will be computed from STUMP leaves
		}
		hashCopy := subHash
		addLeaf(fullPath, internalHeight, &transaction.PathElement{
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

// extendSubtreeToBlockRoot lifts a single-subtree path from the subtree root up
// to the true block-header merkle root when the block tree is taller than the
// subtree (the coinbase tx sits outside the subtree, adding one or more
// block-levels above the subtree-root layer).
//
// The coinbase BUMP is the canonical full block-level path: the coinbase txid
// climbs the block's left spine (offset 0 at every level) to the block root, so
// its sibling at each level above the subtree's internal height is exactly the
// right-hand sibling the subtree root must fold with. Subtree 0 contains the
// coinbase, so it occupies that same left spine — its accumulated node stays at
// offset 0 and the sibling sits at offset 1. Appending those siblings lets
// ComputeRoot fold every tracked tx's path through to the block root.
//
// No-op when there is no coinbase BUMP or when it is not taller than the STUMP
// (the common case where the subtree root already is the block root), so this
// is safe for power-of-two single-subtree blocks that previously worked.
func extendSubtreeToBlockRoot(stumpPath *transaction.MerklePath, coinbaseBUMP []byte, internalHeight int) {
	if len(coinbaseBUMP) == 0 {
		return
	}
	cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
	if err != nil || len(cbPath.Path) <= internalHeight {
		return
	}
	for level := internalHeight; level < len(cbPath.Path); level++ {
		sib := findLeafByOffset(cbPath, level, 1)
		if sib == nil || sib.Hash == nil {
			return
		}
		if findLeafByOffset(stumpPath, level, 1) != nil {
			continue
		}
		h := *sib.Hash
		addLeaf(stumpPath, level, &transaction.PathElement{Offset: 1, Hash: &h})
	}
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
		sibling := findLeafByOffset(cbPath, level, 1)
		if sibling == nil || sibling.Hash == nil {
			break
		}
		currentHash = transaction.MerkleTreeParent(currentHash, sibling.Hash)
	}
}

// subtree0RootFromCoinbaseBUMP derives the real subtree-0 root by walking the
// coinbase BUMP (which is a full block-level path for the coinbase tx) from
// level 0 up through the subtree-internal levels.
//
// Why this is needed: DataHub's subtreeHashes[0] is computed against the
// coinbase *placeholder* (zero hash) at offset 0, not the real coinbase txid.
// computeCorrectedSubtreeRoot only fires when we happen to have a STUMP for
// subtree 0 — but if merkle-service didn't track any txid in subtree 0, no
// STUMP for subtree 0 is delivered, and subtreeHashes[0] stays silently
// wrong. Every other subtree's BUMP path that touches subtreeHashes[0]
// (which is most of them — all paths under L23 offset 0 do) then produces a
// wrong block merkle root.
//
// The coinbase BUMP carries everything we need: the coinbase txid sits at
// (level 0, offset 0) and the BUMP encodes its full path to the block root.
// The first internalHeight levels of that path constitute subtree 0 — so
// folding coinbase txid up internalHeight levels yields the corrected
// subtree-0 root.
//
// Returns nil if the BUMP is malformed or doesn't have enough levels.
func subtree0RootFromCoinbaseBUMP(coinbaseBUMP []byte, internalHeight int) *chainhash.Hash {
	if len(coinbaseBUMP) == 0 || internalHeight <= 0 {
		return nil
	}
	cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
	if err != nil || len(cbPath.Path) < internalHeight {
		return nil
	}

	var coinbase *chainhash.Hash
	for _, leaf := range cbPath.Path[0] {
		if leaf.Offset == 0 && leaf.Hash != nil {
			coinbase = leaf.Hash
			break
		}
	}
	if coinbase == nil {
		return nil
	}

	working := coinbase
	for level := 0; level < internalHeight; level++ {
		// Walking up from offset 0 at level 0 keeps the working position at
		// offset 0 at every level, so the sibling is always at offset 1.
		sibling := findLeafByOffset(cbPath, level, 1)
		if sibling == nil {
			return nil
		}
		if isDuplicate(sibling) {
			working = transaction.MerkleTreeParent(working, working)
		} else if sibling.Hash != nil {
			working = transaction.MerkleTreeParent(working, sibling.Hash)
		} else {
			return nil
		}
	}
	return working
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
			if leaf := findLeafByOffset(fullPath, level, offset); leaf != nil {
				addLeaf(mp, level, leaf)
			}
		}
		if sibling := findLeafByOffset(fullPath, level, offset^1); sibling != nil {
			addLeaf(mp, level, sibling)
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

// correctedSubtree0Root returns the corrected root for subtree 0, derived
// SOLELY from the coinbase BUMP. The datahub-supplied subtreeHashes[0] is
// computed against the coinbase placeholder (zero hash), not the real coinbase
// txid, so it must be recomputed from the real coinbase path.
//
// The coinbase BUMP is a full-block-height path: the coinbase txid climbs the
// block's left spine to the block-header merkle root. Its first internalHeight
// levels are exactly subtree 0, where internalHeight = (coinbase BUMP height) −
// (subtree-root layer). We never trust a STUMP's height here: merkle-service
// sometimes delivers subtree 0's STUMP at full block height, and climbing every
// level of such a STUMP overshoots the subtree-0 root and lands on the BLOCK
// root (the 2026-05-30 block-951360 incident). Deriving internalHeight from the
// coinbase BUMP and folding only that many levels is immune to over-tall STUMPs.
//
// Returns nil if there is no coinbase BUMP, it is malformed, or the derived
// internal height is non-positive.
func correctedSubtree0Root(coinbaseBUMP []byte, numSubtrees int) *chainhash.Hash {
	cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
	if err != nil || len(cbPath.Path) == 0 {
		return nil
	}
	subtreeRootLayer := int(math.Ceil(math.Log2(float64(numSubtrees))))
	internalHeight := len(cbPath.Path) - subtreeRootLayer
	if internalHeight <= 0 {
		return nil
	}
	return subtree0RootFromCoinbaseBUMP(coinbaseBUMP, internalHeight)
}

// BuildCompoundBUMP merges multiple per-subtree BUMPs into a single compound
// MerklePath containing every block leaf the input STUMPs cover (with each
// STUMP's tracked-txid markers preserved). The block-level tree is filled in
// from the STUMP leaves and the supplied subtreeHashes.
//
// This is the live arcade hot path. The previous implementation called
// assembleFullPath once per STUMP — each call building the entire block-level
// tree (subtree-root layer + log₂(N) climb layers) and then deduping them all
// through a (level, offset) map. For a tens-of-thousands-of-subtree block
// that path allocated N copies of the block-level tree (tens of GB of
// intermediate PathElement pointers) and ran computeMissingHashes once per
// STUMP at quadratic cost, so the bump-builder would OOM rather than complete
// (verified 2026-05-22 against block 950028: 24,896 STUMPs drove arcade to
// 13 GB / 100% CPU with no completion after 8+ min).
//
// The current implementation builds the compound directly: each STUMP's
// elements are added at their global block offsets (one pass per STUMP, no
// per-STUMP climb), the subtree-root layer is seeded once with subtreeHashes
// for slots that do not have a STUMP, and the rest of the tree is computed
// in a single top-down pass with per-level offset maps so each level is O(N)
// instead of the O(N²) findLeafByOffset behavior used by computeMissingHashes.
// Output is structurally equivalent to the old algorithm — same merkle root,
// same extracted minimal paths for tracked txs — and the nine existing
// BuildCompoundBUMP tests pass unchanged.
func BuildCompoundBUMP(stumps []*models.Stump, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) (*transaction.MerklePath, []string, error) {
	if len(stumps) == 0 {
		return nil, nil, fmt.Errorf("no stumps to build compound BUMP")
	}

	coinbaseTxID := extractCoinbaseTxID(coinbaseBUMP)

	// Correct subtreeHashes[0] if coinbase is available. The datahub-supplied
	// subtreeHashes[0] is computed against the coinbase PLACEHOLDER (zero
	// hash), not the real coinbase txid — without this, every block-level
	// merkle path that climbs through the subtree-root layer's offset-0
	// sibling (which is most of them) produces the wrong root.
	if coinbaseTxID != nil && len(subtreeHashes) > 0 {
		if root := correctedSubtree0Root(coinbaseBUMP, len(subtreeHashes)); root != nil {
			subtreeHashes[0] = *root
		}
	}

	numSubtrees := len(subtreeHashes)

	// Single-subtree edge case (and pathological zero-subtree case): the
	// STUMP IS the full BUMP. Reuse assembleFullPath which handles the
	// coinbase placeholder swap and odd-leaf padding for us.
	if numSubtrees <= 1 {
		full, _, err := assembleFullPath(stumps[0].StumpData, stumps[0].SubtreeIndex, subtreeHashes, coinbaseBUMP)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to assemble single-subtree BUMP: %w", err)
		}
		txids := make([]string, 0)
		for _, h := range ExtractLevel0Hashes(stumps[0].StumpData) {
			txids = append(txids, h.String())
		}
		return full, txids, nil
	}

	firstPath, err := transaction.NewMerklePathFromBinary(stumps[0].StumpData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse first STUMP: %w", err)
	}
	blockHeight := firstPath.BlockHeight

	subtreeRootLayer := int(math.Ceil(math.Log2(float64(numSubtrees))))

	// Determine the per-subtree internal height. The coinbase BUMP is a
	// full-block-height path, so internalHeight = (coinbase BUMP height) −
	// (subtree-root layer); this is authoritative and immune to STUMPs that
	// merkle-service delivers over-tall (the block-951360 incident). Fall back
	// to the first STUMP's height only when no coinbase BUMP is available.
	internalHeight := len(firstPath.Path)
	if cbPath, cbErr := transaction.NewMerklePathFromBinary(coinbaseBUMP); cbErr == nil {
		if h := len(cbPath.Path) - subtreeRootLayer; h > 0 {
			internalHeight = h
		}
	}

	totalHeight := internalHeight + subtreeRootLayer

	compound := &transaction.MerklePath{
		BlockHeight: blockHeight,
		Path:        make([][]*transaction.PathElement, totalHeight),
	}

	// Walk STUMPs once: parse, apply coinbase swap for subtree 0, shift each
	// of its levels' offsets from local-to-subtree to global-to-block, and
	// place the elements directly into the compound. Tracked-leaf Txid
	// markers carried on the parsed PathElements are preserved by reference.
	haveSTUMP := make(map[int]bool, len(stumps))
	var txids []string

	for _, stump := range stumps {
		if haveSTUMP[stump.SubtreeIndex] {
			continue // duplicate STUMP for the same subtree — keep the first
		}
		haveSTUMP[stump.SubtreeIndex] = true

		path, err := transaction.NewMerklePathFromBinary(stump.StumpData)
		if err != nil {
			return nil, nil, fmt.Errorf("parse STUMP for subtree %d: %w", stump.SubtreeIndex, err)
		}
		// A STUMP taller than internalHeight is tolerated: merkle-service
		// sometimes delivers a STUMP at full block height, and only its first
		// internalHeight levels are the subtree-internal path — the placement
		// loop below reads exactly those. A STUMP SHORTER than internalHeight
		// cannot cover its subtree and is rejected.
		if len(path.Path) < internalHeight {
			return nil, nil, fmt.Errorf("subtree %d STUMP has internal height %d, expected at least %d", stump.SubtreeIndex, len(path.Path), internalHeight)
		}
		if stump.SubtreeIndex == 0 && coinbaseTxID != nil {
			applyCoinbaseToSTUMP(path, coinbaseTxID, coinbaseBUMP)
		}

		for level := 0; level < internalHeight; level++ {
			shift := uint64(stump.SubtreeIndex) << uint(internalHeight-level) //nolint:gosec // subtreeIndex is bounded by numSubtrees; height is small
			for _, elem := range path.Path[level] {
				elem.Offset += shift
				addLeaf(compound, level, elem)
			}
		}

		for _, h := range ExtractLevel0Hashes(stump.StumpData) {
			txids = append(txids, h.String())
		}
	}

	// Seed the subtree-root layer with hashes for slots that have NO STUMP;
	// slots that do will have their subtree root computed up from their
	// level-0 leaves in the top-down pass below. The corrected
	// subtreeHashes[0] flows through here when subtree 0 lacks a STUMP.
	for i, subHash := range subtreeHashes {
		if haveSTUMP[i] {
			continue
		}
		hashCopy := subHash
		addLeaf(compound, internalHeight, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
		})
	}

	computeAndPadCompound(compound, internalHeight, numSubtrees)

	return compound, txids, nil
}

// computeAndPadCompound fills in every missing intermediate node of a
// compound MerklePath in a single top-down pass, using per-level offset maps
// for O(1) sibling and parent-existence lookups (instead of the O(N) linear
// scans `computeMissingHashes`/`findLeafByOffset` perform — that quadratic
// behavior was the bulk of the cost on big blocks when the old per-STUMP
// algorithm paid it N times).
//
// `subtreeRootLayer` is the level holding the block's subtree-root hashes;
// levels strictly above it receive Bitcoin-canonical "duplicate last on odd"
// padding (the same logic as `padAndComputeBlockLevel`). The per-subtree
// layers below the subtree-root layer never need padding because subtrees
// are always power-of-two sized.
func computeAndPadCompound(mp *transaction.MerklePath, subtreeRootLayer, numSubtrees int) {
	dupTrue := true
	realCount := numSubtrees

	for level := 0; level < len(mp.Path)-1; level++ {
		// Bitcoin-canonical pad-on-odd at the subtree-root layer and above.
		if level >= subtreeRootLayer && realCount%2 == 1 {
			if findLeafByOffset(mp, level, uint64(realCount)) == nil { //nolint:gosec // realCount bounded by tree size
				addLeaf(mp, level, &transaction.PathElement{
					Offset:    uint64(realCount), //nolint:gosec // realCount bounded by tree size
					Duplicate: &dupTrue,
				})
			}
			realCount++
		}

		// Build offset→elem and parent-presence maps once per level so the
		// per-element work below is O(1) instead of O(N) per lookup.
		idx := make(map[uint64]*transaction.PathElement, len(mp.Path[level]))
		for _, elem := range mp.Path[level] {
			idx[elem.Offset] = elem
		}
		parentPresent := make(map[uint64]bool, len(mp.Path[level+1]))
		for _, e := range mp.Path[level+1] {
			parentPresent[e.Offset] = true
		}

		for _, elem := range mp.Path[level] {
			parentOffset := elem.Offset / 2
			if parentPresent[parentOffset] {
				continue
			}
			sibling, ok := idx[elem.Offset^1]
			if !ok {
				continue
			}

			var parent *chainhash.Hash
			switch {
			case isDuplicate(sibling):
				if elem.Hash == nil {
					continue
				}
				parent = merkleTreeParent(elem.Hash, elem.Hash)
			case isDuplicate(elem):
				if sibling.Hash == nil {
					continue
				}
				parent = merkleTreeParent(sibling.Hash, sibling.Hash)
			case elem.Hash == nil || sibling.Hash == nil:
				continue
			case elem.Offset%2 == 0:
				parent = merkleTreeParent(elem.Hash, sibling.Hash)
			default:
				parent = merkleTreeParent(sibling.Hash, elem.Hash)
			}

			addLeaf(mp, level+1, &transaction.PathElement{
				Offset: parentOffset,
				Hash:   parent,
			})
			parentPresent[parentOffset] = true
		}

		if level >= subtreeRootLayer {
			realCount /= 2
		}
	}
}
