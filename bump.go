package arcade

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// AssembleBUMP constructs a full BUMP (block-level merkle proof) from a STUMP
// (subtree-level merkle path) and the block's subtree root hashes.
//
// Parameters:
//   - stumpData: BRC-74 binary-encoded STUMP (subtree merkle path with local offsets)
//   - subtreeIndex: index of the subtree containing the tracked transaction
//   - subtreeHashes: root hashes for all subtrees in the block
//   - coinbaseBUMP: BRC-74 binary-encoded merkle path of the coinbase transaction (for subtree 0
//     placeholder replacement); nil if unavailable. When the tracked tx is in subtree 0, the
//     coinbase BUMP provides correct intermediate hashes that replace the placeholder-derived hashes.
//
// Returns the full merkle path for the tracked transaction (with global offsets) and the global tx offset.
func AssembleBUMP(stumpData []byte, subtreeIndex int, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) (*transaction.MerklePath, uint64, error) {
	stumpPath, err := transaction.NewMerklePathFromBinary(stumpData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse STUMP: %w", err)
	}

	internalHeight := len(stumpPath.Path)

	// Handle coinbase placeholder replacement for subtree 0 using the coinbase BUMP.
	if subtreeIndex == 0 && len(coinbaseBUMP) > 0 {
		replaceCoinbasePlaceholder(stumpPath, coinbaseBUMP, internalHeight)
	}

	numSubtrees := len(subtreeHashes)
	if numSubtrees <= 1 {
		// Single subtree: STUMP IS the full BUMP.
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
			fullPath.AddLeaf(level, elem)
		}
	}

	// Add subtree root hashes at the subtree root layer.
	for i, subHash := range subtreeHashes {
		if i == subtreeIndex {
			continue // our subtree root will be computed from STUMP leaves
		}
		hashCopy := subHash
		fullPath.AddLeaf(internalHeight, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   &hashCopy,
		})
	}

	fullPath.ComputeMissingHashes()
	minimalPath := ExtractMinimalPath(fullPath, txOffset)
	return minimalPath, txOffset, nil
}

// replaceCoinbasePlaceholder uses the coinbase BUMP to replace placeholder-derived
// hashes in the STUMP. The coinbase is at offset 0 in subtree 0; its ancestor at
// each level is always at offset 0. For each STUMP level, if offset 0 has a hash
// (which was computed from the placeholder), we replace it with the correct hash
// computed from the real coinbase.
func replaceCoinbasePlaceholder(stumpPath *transaction.MerklePath, coinbaseBUMP []byte, internalHeight int) {
	cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
	if err != nil {
		return
	}

	var currentHash *chainhash.Hash
	for _, leaf := range cbPath.Path[0] {
		if leaf.Offset == 0 && leaf.Hash != nil {
			currentHash = leaf.Hash
			break
		}
	}
	if currentHash == nil {
		return
	}

	for level := 0; level < internalHeight && level < len(cbPath.Path); level++ {
		for _, elem := range stumpPath.Path[level] {
			if elem.Offset == 0 && (elem.Txid == nil || !*elem.Txid) {
				h := *currentHash
				elem.Hash = &h
				break
			}
		}

		sibling := cbPath.FindLeafByOffset(level, 1)
		if sibling == nil || sibling.Hash == nil {
			break
		}
		currentHash = transaction.MerkleTreeParent(currentHash, sibling.Hash)
	}
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
			if leaf := fullPath.FindLeafByOffset(level, offset); leaf != nil {
				mp.AddLeaf(level, leaf)
			}
		}
		if sibling := fullPath.FindLeafByOffset(level, offset^1); sibling != nil {
			mp.AddLeaf(level, sibling)
		}
		offset = offset >> 1
	}

	return mp
}

// extractLevel0Hashes parses a BRC-74 STUMP binary and returns all level-0 hashes.
func extractLevel0Hashes(stumpData []byte) []chainhash.Hash {
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

// BuildCompoundBUMP merges multiple per-subtree BUMPs into a single compound MerklePath
// containing all tracked transactions at level 0. The tracker is used to discover
// which level-0 hashes are tracked transactions.
func BuildCompoundBUMP(stumps []*models.Stump, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, tracker *store.TxTracker) (*transaction.MerklePath, []string, error) {
	if len(stumps) == 0 {
		return nil, nil, fmt.Errorf("no stumps to build compound BUMP")
	}

	// Assemble each STUMP into a full path, collect all elements by level
	var blockHeight uint32
	var txids []string
	allPaths := make([]*transaction.MerklePath, 0, len(stumps))

	for _, stump := range stumps {
		fullPath, _, err := AssembleBUMP(stump.StumpData, stump.SubtreeIndex, subtreeHashes, coinbaseBUMP)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to assemble BUMP for subtree %d: %w", stump.SubtreeIndex, err)
		}
		blockHeight = fullPath.BlockHeight

		// Discover tracked txids from level-0 hashes
		level0 := extractLevel0Hashes(stump.StumpData)
		if tracker != nil {
			tracked := tracker.FilterTrackedHashes(level0)
			for _, h := range tracked {
				txids = append(txids, h.String())
			}
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
				compound.AddLeaf(level, elem)
			}
		}
	}

	return compound, txids, nil
}

// ConstructBUMPsForBlock constructs a compound BUMP from accumulated STUMPs for a block
// and stores it in the bumps table.
func (a *Arcade) ConstructBUMPsForBlock(ctx context.Context, blockHash string) error {
	stumps, err := a.store.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("failed to get stumps for block %s: %w", blockHash, err)
	}

	if len(stumps) == 0 {
		a.logger.Debug("no STUMPs for block, skipping BUMP construction",
			slog.String("blockHash", blockHash))
		return nil
	}

	a.logger.Info("constructing compound BUMP from STUMPs",
		slog.String("blockHash", blockHash),
		slog.Int("stumpCount", len(stumps)))

	subtreeHashes, coinbaseBUMP, err := a.fetchBlockDataForBUMP(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("failed to fetch block data: %w", err)
	}

	if len(subtreeHashes) == 0 {
		a.logger.Warn("block has no subtrees, cannot construct BUMPs",
			slog.String("blockHash", blockHash))
		return nil
	}

	compound, txids, err := BuildCompoundBUMP(stumps, subtreeHashes, coinbaseBUMP, a.txTracker)
	if err != nil {
		return fmt.Errorf("failed to build compound BUMP: %w", err)
	}

	blockHeight := uint64(compound.BlockHeight)
	if err := a.store.InsertBUMP(ctx, blockHash, blockHeight, compound.Bytes()); err != nil {
		return fmt.Errorf("failed to store compound BUMP: %w", err)
	}

	statuses, err := a.store.SetMinedByTxIDs(ctx, blockHash, txids)
	if err != nil {
		a.logger.Error("failed to set mined status",
			slog.String("blockHash", blockHash),
			slog.String("error", err.Error()))
	} else {
		for _, status := range statuses {
			_ = a.eventPublisher.Publish(ctx, status)
		}
		if len(statuses) > 0 {
			a.logger.Info("set transactions to MINED via BUMP construction",
				slog.String("blockHash", blockHash),
				slog.Int("count", len(statuses)))
		}
	}

	if err := a.store.DeleteStumpsByBlockHash(ctx, blockHash); err != nil {
		a.logger.Warn("failed to clean up STUMPs",
			slog.String("blockHash", blockHash),
			slog.String("error", err.Error()))
	}

	return nil
}

// fetchBlockDataForBUMP fetches subtree hashes and coinbase BUMP for a block,
// trying all DataHub URLs. Prefers the JSON endpoint (which includes coinbase_bump),
// falling back to the binary endpoint (no coinbase BUMP).
func (a *Arcade) fetchBlockDataForBUMP(ctx context.Context, blockHash string) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, err error) {
	for _, dataHubURL := range a.dataHubURLs {
		// Try JSON endpoint first (includes coinbase_bump)
		resp, jsonErr := a.fetchBlockJSON(ctx, dataHubURL, blockHash)
		if jsonErr == nil {
			hashes, coinbase, parseErr := parseBlockJSONResponse(resp)
			if parseErr == nil {
				return hashes, coinbase, nil
			}
			a.logger.Debug("failed to parse JSON block response",
				slog.String("url", dataHubURL),
				slog.String("error", parseErr.Error()))
		} else {
			a.logger.Debug("JSON block endpoint failed, trying binary",
				slog.String("url", dataHubURL),
				slog.String("error", jsonErr.Error()))
		}

		// Fall back to binary endpoint (no coinbase BUMP)
		hashes, binErr := a.fetchBlockBinarySubtrees(ctx, dataHubURL, blockHash)
		if binErr == nil {
			return hashes, nil, nil
		}
		a.logger.Debug("binary block endpoint also failed",
			slog.String("url", dataHubURL),
			slog.String("error", binErr.Error()))
	}
	return nil, nil, fmt.Errorf("all DataHub URLs failed for block %s", blockHash)
}

// parseBlockJSONResponse converts a blockJSONResponse into typed data.
func parseBlockJSONResponse(resp *blockJSONResponse) ([]chainhash.Hash, []byte, error) {
	hashes := make([]chainhash.Hash, 0, len(resp.Subtrees))
	for _, hexStr := range resp.Subtrees {
		h, err := chainhash.NewHashFromHex(hexStr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse subtree hash %q: %w", hexStr, err)
		}
		hashes = append(hashes, *h)
	}

	var coinbaseBUMP []byte
	if resp.CoinbaseBump != "" {
		var err error
		coinbaseBUMP, err = hex.DecodeString(resp.CoinbaseBump)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode coinbase_bump: %w", err)
		}
	}

	return hashes, coinbaseBUMP, nil
}

