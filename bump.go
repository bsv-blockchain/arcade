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

// ConstructBUMPsForBlock constructs full BUMPs from accumulated STUMPs for a block.
// This is called when Merkle Service sends a BLOCK_PROCESSED callback.
func (a *Arcade) ConstructBUMPsForBlock(ctx context.Context, blockHash string) error {
	// Get all STUMPs accumulated for this block
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

	// Fetch block model to get subtree hashes and coinbase
	subtreeHashes, err := a.fetchBlockSubtreeHashesForBUMP(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("failed to fetch block subtree hashes: %w", err)
	}

	if len(subtreeHashes) == 0 {
		a.logger.Warn("block has no subtrees, cannot construct BUMPs",
			slog.String("blockHash", blockHash))
		return nil
	}

	// Get coinbase txid if available (needed for subtree 0 placeholder replacement)
	coinbaseTxID := a.getCoinbaseTxIDForBlock(ctx, blockHash)

	subtreeRootLayer := int(math.Ceil(math.Log2(float64(len(subtreeHashes)))))

	// Construct BUMP for each STUMP
	for _, stump := range stumps {
		if err := a.constructSingleBUMP(ctx, blockHash, stump, subtreeHashes, subtreeRootLayer, coinbaseTxID); err != nil {
			a.logger.Error("failed to construct BUMP",
				slog.String("txid", stump.TxID),
				slog.String("blockHash", blockHash),
				slog.String("error", err.Error()))
			continue
		}
	}

	// Set MINED status for transactions with merkle proofs in this block
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

	// Clean up STUMPs after successful BUMP construction
	if err := a.store.DeleteStumpsByBlockHash(ctx, blockHash); err != nil {
		a.logger.Warn("failed to clean up STUMPs",
			slog.String("blockHash", blockHash),
			slog.String("error", err.Error()))
	}

	return nil
}

// constructSingleBUMP builds a full BUMP from a STUMP + subtree root hashes.
func (a *Arcade) constructSingleBUMP(
	ctx context.Context,
	blockHash string,
	stump *models.Stump,
	subtreeHashes []chainhash.Hash,
	subtreeRootLayer int,
	coinbaseTxID *chainhash.Hash,
) error {
	// Parse the STUMP (subtree-level merkle path)
	stumpPath, err := transaction.NewMerklePathFromBinary(stump.StumpData)
	if err != nil {
		return fmt.Errorf("failed to parse STUMP: %w", err)
	}

	// Find the transaction's offset in the STUMP
	var txOffset uint64
	if len(stumpPath.Path) > 0 {
		for _, leaf := range stumpPath.Path[0] {
			if leaf.Txid != nil && *leaf.Txid {
				txOffset = leaf.Offset
				break
			}
		}
	}

	// Build the full merkle path: STUMP levels + subtree root level
	internalHeight := len(stumpPath.Path)
	totalHeight := internalHeight + subtreeRootLayer

	fullPath := &transaction.MerklePath{
		BlockHeight: stumpPath.BlockHeight,
		Path:        make([][]*transaction.PathElement, totalHeight),
	}

	// Copy STUMP levels into the full path
	for level := 0; level < internalHeight; level++ {
		for _, elem := range stumpPath.Path[level] {
			fullPath.AddLeaf(level, elem)
		}
	}

	// Handle coinbase placeholder replacement for subtree 0
	if stump.SubtreeIndex == 0 && coinbaseTxID != nil {
		// Replace the placeholder at offset 0, level 0 with the real coinbase txid
		for _, leaf := range fullPath.Path[0] {
			if leaf.Offset == 0 && (leaf.Txid == nil || !*leaf.Txid) {
				coinbaseHash := *coinbaseTxID
				leaf.Hash = &coinbaseHash
				break
			}
		}
	}

	// Add subtree root hashes at the subtree root layer
	subtreeBaseOffset := uint64(stump.SubtreeIndex) << uint(internalHeight-1) //nolint:gosec // safe: subtreeIndex from trusted source
	for i, subHash := range subtreeHashes {
		if i == stump.SubtreeIndex {
			continue // This subtree's root is computed from the STUMP leaves
		}
		hashCopy := subHash
		fullPath.AddLeaf(internalHeight, &transaction.PathElement{
			Offset: subtreeBaseOffset + uint64(i),
			Hash:   &hashCopy,
		})
	}

	// Compute missing hashes in the tree
	fullPath.ComputeMissingHashes()

	// Extract the minimal path for this transaction
	minimalPath := a.extractMinimalPath(fullPath, txOffset)

	// Get block height from the STUMP
	blockHeight := uint64(stumpPath.BlockHeight)

	// Store the BUMP
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
// Returns nil if not available.
func (a *Arcade) getCoinbaseTxIDForBlock(_ context.Context, _ string) *chainhash.Hash {
	// The coinbase txid would come from the block message or from fetching the block model.
	// For now, during block processing via P2P, the coinbase is available in BlockMessage.Coinbase.
	// When triggered by callback, we need to fetch it from the block model.
	// This will be enhanced when Teranode returns the coinbase in the block model.
	//
	// TODO: fetch coinbase from block model once Teranode supports it
	return nil
}
