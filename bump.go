package arcade

import (
	"context"
	"fmt"
	"log/slog"
	"math"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/models"
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
// Returns the minimal merkle path for the tracked transaction and the global tx offset.
func AssembleBUMP(stumpData []byte, subtreeIndex int, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) (*transaction.MerklePath, uint64, error) {
	stumpPath, err := transaction.NewMerklePathFromBinary(stumpData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse STUMP: %w", err)
	}

	internalHeight := len(stumpPath.Path)

	// Handle coinbase placeholder replacement for subtree 0 using the coinbase BUMP.
	// The coinbase BUMP provides correct intermediate hashes computed with the real coinbase
	// instead of the placeholder. We walk the coinbase BUMP to compute the correct hash at
	// each level, then replace the corresponding hash in the STUMP.
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

	// Walk the coinbase BUMP from the bottom to compute the correct hash at each level.
	// The coinbase is always at offset 0, so its ancestor at level L is at offset 0.
	// At each level, we compute: correctHash = MTP(left, right) using the coinbase path.

	// First, find the coinbase hash at level 0
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

	// For each level, try to replace the hash at offset 0 in the STUMP
	for level := 0; level < internalHeight && level < len(cbPath.Path); level++ {
		// Replace offset 0 in the STUMP at this level with the correct coinbase-derived hash
		for _, elem := range stumpPath.Path[level] {
			if elem.Offset == 0 && (elem.Txid == nil || !*elem.Txid) {
				h := *currentHash
				elem.Hash = &h
				break
			}
		}

		// Compute the next level's hash using the coinbase path's sibling
		sibling := cbPath.FindLeafByOffset(level, 1) // coinbase at offset 0, sibling at 1
		if sibling == nil || sibling.Hash == nil {
			break
		}
		// Coinbase is always on the left (offset 0 at every level → even → left)
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

// ConstructBUMPsForBlock constructs full BUMPs from accumulated STUMPs for a block.
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

	a.logger.Info("constructing BUMPs from STUMPs",
		slog.String("blockHash", blockHash),
		slog.Int("stumpCount", len(stumps)))

	subtreeHashes, err := a.fetchBlockSubtreeHashesForBUMP(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("failed to fetch block subtree hashes: %w", err)
	}

	if len(subtreeHashes) == 0 {
		a.logger.Warn("block has no subtrees, cannot construct BUMPs",
			slog.String("blockHash", blockHash))
		return nil
	}

	// TODO: fetch coinbase BUMP from block model once Teranode supports it
	var coinbaseBUMP []byte

	for _, stump := range stumps {
		if err := a.constructSingleBUMP(ctx, blockHash, stump, subtreeHashes, coinbaseBUMP); err != nil {
			a.logger.Error("failed to construct BUMP",
				slog.String("txid", stump.TxID),
				slog.String("blockHash", blockHash),
				slog.String("error", err.Error()))
			continue
		}
	}

	statuses, err := a.store.SetMinedByBlockHash(ctx, blockHash)
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

// constructSingleBUMP is a thin wrapper around AssembleBUMP that handles storage.
func (a *Arcade) constructSingleBUMP(
	ctx context.Context,
	blockHash string,
	stump *models.Stump,
	subtreeHashes []chainhash.Hash,
	coinbaseBUMP []byte,
) error {
	minimalPath, _, err := AssembleBUMP(stump.StumpData, stump.SubtreeIndex, subtreeHashes, coinbaseBUMP)
	if err != nil {
		return err
	}

	blockHeight := uint64(minimalPath.BlockHeight)
	if err := a.store.InsertMerklePath(ctx, stump.TxID, blockHash, blockHeight, minimalPath.Bytes()); err != nil {
		return fmt.Errorf("failed to store BUMP: %w", err)
	}

	a.logger.Debug("constructed BUMP",
		slog.String("txid", stump.TxID),
		slog.String("blockHash", blockHash),
		slog.Int("subtreeIndex", stump.SubtreeIndex))

	return nil
}

// fetchBlockSubtreeHashesForBUMP fetches subtree hashes for a block, trying all DataHub URLs.
func (a *Arcade) fetchBlockSubtreeHashesForBUMP(ctx context.Context, blockHash string) ([]chainhash.Hash, error) {
	for _, dataHubURL := range a.dataHubURLs {
		hashes, err := a.fetchBlockSubtreeHashes(ctx, dataHubURL, blockHash)
		if err == nil {
			return hashes, nil
		}
		a.logger.Debug("failed to fetch block from DataHub",
			slog.String("url", dataHubURL),
			slog.String("blockHash", blockHash),
			slog.String("error", err.Error()))
	}
	return nil, fmt.Errorf("all DataHub URLs failed for block %s", blockHash)
}

// getCoinbaseTxIDForBlock attempts to get the coinbase transaction ID for a block.
func (a *Arcade) getCoinbaseTxIDForBlock(_ context.Context, _ string) *chainhash.Hash {
	// TODO: fetch coinbase from block model once Teranode supports it
	return nil
}
