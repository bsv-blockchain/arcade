// Package arcade provides unified blockchain tracking and transaction status management.
package arcade

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/p2p"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// Chaintracks defines the interface for chain tracking with optional transaction status management.
// This interface can be implemented by:
// - Arcade: P2P-based embedded implementation (this package)
// - Client: HTTP/SSE-based remote client
type Chaintracks interface {
	chaintracker.ChainTracker

	// Start begins the service and returns a channel for block tip notifications
	Start(ctx context.Context) (<-chan *BlockHeader, error)

	// Stop gracefully shuts down the service
	Stop() error

	// GetHeight returns the current blockchain height
	GetHeight(ctx context.Context) uint32

	// GetTip returns the current chain tip
	GetTip(ctx context.Context) *BlockHeader

	// GetHeaderByHeight retrieves a block header by its height
	GetHeaderByHeight(ctx context.Context, height uint32) (*BlockHeader, error)

	// GetHeaderByHash retrieves a block header by its hash
	GetHeaderByHash(ctx context.Context, hash *chainhash.Hash) (*BlockHeader, error)

	// GetNetwork returns the network name (mainnet, testnet, etc.)
	GetNetwork(ctx context.Context) (string, error)
}

// BlockHeader extends the base block.Header with chain metadata
type BlockHeader struct {
	*block.Header
	Height    uint32         `json:"height"`
	Hash      chainhash.Hash `json:"hash"`
	ChainWork *big.Int       `json:"-"`
}

// SubscriptionOptions configures which P2P topics to subscribe to
type SubscriptionOptions struct {
	// Blocks subscribes to block announcements (required for chain tracking)
	Blocks bool

	// Subtrees subscribes to subtree announcements (for "seen on network" status)
	Subtrees bool

	// RejectedTxs subscribes to rejected transaction messages
	RejectedTxs bool

	// NodeStatus subscribes to node status messages
	NodeStatus bool
}

// DefaultChainTrackingSubscriptions returns options for chain tracking only (no tx status)
func DefaultChainTrackingSubscriptions() SubscriptionOptions {
	return SubscriptionOptions{
		Blocks: true,
	}
}

// DefaultFullSubscriptions returns options for full transaction status tracking
func DefaultFullSubscriptions() SubscriptionOptions {
	return SubscriptionOptions{
		Blocks:      true,
		Subtrees:    true,
		RejectedTxs: true,
	}
}

// Config holds configuration for the ChainTracker
type Config struct {
	// Network name (mainnet, testnet, etc.)
	Network string

	// LocalStoragePath for persisting chain state
	LocalStoragePath string

	// Logger for structured logging
	Logger *slog.Logger

	// P2PClient for network communication (required)
	P2PClient *p2p.Client

	// Subscriptions configures which P2P topics to listen to
	Subscriptions SubscriptionOptions

	// BootstrapURL for initial sync (optional)
	BootstrapURL string

	// Transaction tracking (optional - only needed for tx status management)
	TxTracker      *store.TxTracker
	StatusStore    store.StatusStore
	EventPublisher events.Publisher
}

// Arcade is the P2P-based implementation that tracks blockchain headers
// and optionally manages transaction statuses.
type Arcade struct {
	mu sync.RWMutex

	// Chain state
	byHeight []chainhash.Hash                // Main chain hashes indexed by height
	byHash   map[chainhash.Hash]*BlockHeader // Hash → Header (all headers: main + orphans)
	tip      *BlockHeader                    // Current chain tip

	// Configuration
	localStoragePath string
	network          string
	subscriptions    SubscriptionOptions
	logger           *slog.Logger
	httpClient       *http.Client

	// P2P
	p2pClient *p2p.Client

	// Transaction tracking (optional)
	txTracker      *store.TxTracker
	statusStore    store.StatusStore
	eventPublisher events.Publisher

	// Notification channel
	tipChan chan *BlockHeader
}

// NewArcade creates a new P2P-based Arcade instance.
// For chain tracking only, omit TxTracker/StatusStore/EventPublisher.
// For full transaction status management, provide all dependencies.
func NewArcade(ctx context.Context, cfg Config) (*Arcade, error) {
	if cfg.P2PClient == nil {
		return nil, fmt.Errorf("P2P client is required")
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Validate tx tracking dependencies - all or none
	hasTxTracking := cfg.TxTracker != nil || cfg.StatusStore != nil || cfg.EventPublisher != nil
	if hasTxTracking {
		if cfg.TxTracker == nil || cfg.StatusStore == nil || cfg.EventPublisher == nil {
			return nil, fmt.Errorf("for transaction tracking, TxTracker, StatusStore, and EventPublisher are all required")
		}
		// TX tracking requires subtrees subscription
		if !cfg.Subscriptions.Subtrees {
			cfg.Logger.Warn("enabling Subtrees subscription (required for transaction tracking)")
			cfg.Subscriptions.Subtrees = true
		}
	}

	a := &Arcade{
		byHeight:         make([]chainhash.Hash, 0, 1000000),
		byHash:           make(map[chainhash.Hash]*BlockHeader),
		network:          cfg.Network,
		localStoragePath: cfg.LocalStoragePath,
		subscriptions:    cfg.Subscriptions,
		logger:           cfg.Logger,
		p2pClient:        cfg.P2PClient,
		txTracker:        cfg.TxTracker,
		statusStore:      cfg.StatusStore,
		eventPublisher:   cfg.EventPublisher,
		httpClient:       &http.Client{Timeout: 30 * time.Second},
	}

	// Load chain state from local files
	if err := a.loadFromLocalFiles(ctx); err != nil {
		return nil, fmt.Errorf("failed to load chain state: %w", err)
	}

	// Optional bootstrap sync
	if cfg.BootstrapURL != "" {
		a.runBootstrapSync(ctx, cfg.BootstrapURL)
	}

	return a, nil
}

// Start begins listening for P2P messages and returns a channel for tip updates
func (a *Arcade) Start(ctx context.Context) (<-chan *BlockHeader, error) {
	a.mu.Lock()
	a.tipChan = make(chan *BlockHeader, 1)
	a.mu.Unlock()

	a.logger.Info("Starting ChainTracker P2P subscriptions",
		slog.Bool("blocks", a.subscriptions.Blocks),
		slog.Bool("subtrees", a.subscriptions.Subtrees),
		slog.Bool("rejectedTxs", a.subscriptions.RejectedTxs))

	// Subscribe to configured P2P topics
	if a.subscriptions.Blocks {
		blockChan := a.p2pClient.SubscribeBlocks(ctx)
		go a.handleBlockMessages(ctx, blockChan)
	}

	if a.subscriptions.Subtrees {
		subtreeChan := a.p2pClient.SubscribeSubtrees(ctx)
		go a.handleSubtreeMessages(ctx, subtreeChan)
	}

	if a.subscriptions.RejectedTxs {
		rejectedTxChan := a.p2pClient.SubscribeRejectedTxs(ctx)
		go a.handleRejectedTxMessages(ctx, rejectedTxChan)
	}

	if a.subscriptions.NodeStatus {
		nodeStatusChan := a.p2pClient.SubscribeNodeStatus(ctx)
		go a.handleNodeStatusMessages(ctx, nodeStatusChan)
	}

	// Periodic orphan check (only if tracking transactions)
	if a.txTracker != nil {
		go a.periodicOrphanCheck(ctx)
	}

	a.logger.Info("ChainTracker started", slog.String("peerID", a.p2pClient.GetID()))

	return a.tipChan, nil
}

// Stop gracefully shuts down the ChainTracker
func (a *Arcade) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.tipChan != nil {
		close(a.tipChan)
		a.tipChan = nil
	}

	return nil
}

// handleBlockMessages processes incoming block announcements
func (a *Arcade) handleBlockMessages(ctx context.Context, blockChan <-chan p2p.BlockMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case blockMsg, ok := <-blockChan:
			if !ok {
				return
			}
			if err := a.processBlockMessage(ctx, blockMsg); err != nil {
				a.logger.Error("failed to process block message",
					slog.String("hash", blockMsg.Hash),
					slog.String("error", err.Error()))
			}
		}
	}
}

// processBlockMessage handles a new block announcement
func (a *Arcade) processBlockMessage(ctx context.Context, blockMsg p2p.BlockMessage) error {
	a.logger.Info("received block message",
		slog.String("hash", blockMsg.Hash),
		slog.Uint64("height", uint64(blockMsg.Height)),
		slog.String("dataHubURL", blockMsg.DataHubURL))

	// Parse the header
	headerBytes, err := hex.DecodeString(blockMsg.Header)
	if err != nil {
		return fmt.Errorf("failed to decode header: %w", err)
	}

	header, err := block.NewHeaderFromBytes(headerBytes)
	if err != nil {
		return fmt.Errorf("failed to parse header: %w", err)
	}

	blockHash := header.Hash()

	// Check if we already have this block
	if _, err := a.GetHeaderByHash(ctx, &blockHash); err == nil {
		return nil // Already processed
	}

	// Process transactions FIRST, before analyzing chain tip.
	// This ensures merkle proofs are ready regardless of which chain becomes canonical.
	if a.txTracker != nil && a.txTracker.Count() > 0 {
		if err := a.processBlockTransactions(ctx, blockMsg); err != nil {
			a.logger.Error("failed to process block transactions",
				slog.String("hash", blockMsg.Hash),
				slog.String("error", err.Error()))
		}
	}

	// Now process header and determine if this becomes the new tip
	isNewTip, reorgedBlocks, err := a.processHeader(ctx, header, blockMsg.Height, blockMsg.DataHubURL)
	if err != nil {
		return err
	}

	// Handle reorgs - revert any txs still pointing to orphaned blocks
	if len(reorgedBlocks) > 0 && a.txTracker != nil {
		a.handleReorg(ctx, reorgedBlocks)
	}

	// Prune deeply confirmed transactions only when we have a new tip
	if isNewTip && a.txTracker != nil {
		a.pruneConfirmedTransactions(ctx, blockMsg.Height)
	}

	return nil
}

// processHeader adds a header to chain state and returns whether it's the new tip
func (a *Arcade) processHeader(ctx context.Context, header *block.Header, height uint32, dataHubURL string) (bool, []*BlockHeader, error) {
	parentHash := header.PrevHash
	parentHeader, err := a.GetHeaderByHash(ctx, &parentHash)

	if err != nil {
		// Parent not found, need to crawl back
		a.logger.Info("parent not found, crawling back", slog.String("parent", parentHash.String()))
		if err := a.crawlBackAndMerge(ctx, header, height, dataHubURL); err != nil {
			return false, nil, err
		}
		parentHeader, _ = a.GetHeaderByHash(ctx, &parentHash)
	}

	if parentHeader == nil {
		return false, nil, fmt.Errorf("parent header still not found after crawl")
	}

	// Calculate chainwork
	work := calculateWork(header.Bits)
	chainWork := new(big.Int).Add(parentHeader.ChainWork, work)

	blockHeader := &BlockHeader{
		Header:    header,
		Height:    height,
		Hash:      header.Hash(),
		ChainWork: chainWork,
	}

	// Add to byHash
	a.mu.Lock()
	a.byHash[blockHeader.Hash] = blockHeader
	a.mu.Unlock()

	// Check if this is the new tip
	currentTip := a.GetTip(ctx)
	if currentTip == nil || blockHeader.ChainWork.Cmp(currentTip.ChainWork) > 0 {
		reorgedBlocks := a.setChainTip(ctx, blockHeader)

		// Notify tip change
		a.mu.RLock()
		tipChan := a.tipChan
		a.mu.RUnlock()
		if tipChan != nil {
			select {
			case tipChan <- blockHeader:
			default:
				// Channel full, replace with latest
				select {
				case <-tipChan:
				default:
				}
				select {
				case tipChan <- blockHeader:
				default:
				}
			}
		}

		return true, reorgedBlocks, nil
	}

	return false, nil, nil
}

// processBlockTransactions processes transactions in a block on the canonical chain
func (a *Arcade) processBlockTransactions(ctx context.Context, blockMsg p2p.BlockMessage) error {
	// Fetch subtree hashes for this block
	subtreeHashes, err := a.fetchBlockSubtreeHashes(ctx, blockMsg.DataHubURL, blockMsg.Hash)
	if err != nil {
		return fmt.Errorf("failed to fetch subtree hashes: %w", err)
	}

	numSubtrees := len(subtreeHashes)
	if numSubtrees == 0 {
		return nil
	}

	a.logger.Info("processing block transactions",
		slog.String("hash", blockMsg.Hash),
		slog.Uint64("height", uint64(blockMsg.Height)),
		slog.Int("subtrees", numSubtrees))

	// Calculate the layer where subtree roots sit
	subtreeRootLayer := int(math.Ceil(math.Log2(float64(numSubtrees))))

	// Process each subtree
	for subtreeIdx, subtreeHash := range subtreeHashes {
		txHashes, err := a.fetchSubtreeHashes(ctx, blockMsg.DataHubURL, subtreeHash.String())
		if err != nil {
			a.logger.Error("failed to fetch subtree txids",
				slog.String("subtreeHash", subtreeHash.String()),
				slog.String("error", err.Error()))
			continue
		}

		// Find tracked transactions in this subtree
		tracked := a.txTracker.FilterTrackedHashes(txHashes)
		if len(tracked) == 0 {
			continue
		}

		// Build merkle paths for tracked transactions
		a.buildMerklePathsForSubtree(ctx, blockMsg, subtreeIdx, subtreeRootLayer, subtreeHashes, txHashes, tracked)
	}

	return nil
}

// pruneConfirmedTransactions marks deeply confirmed transactions as IMMUTABLE
func (a *Arcade) pruneConfirmedTransactions(ctx context.Context, currentHeight uint32) {
	immutableTxs := a.txTracker.PruneConfirmed(uint64(currentHeight))
	for _, hash := range immutableTxs {
		txID := hash.String()

		status := &models.TransactionStatus{
			TxID:      txID,
			Status:    models.StatusImmutable,
			Timestamp: time.Now(),
		}

		if err := a.statusStore.UpdateStatus(ctx, status); err != nil {
			a.logger.Error("failed to update immutable status",
				slog.String("txID", txID),
				slog.String("error", err.Error()))
			continue
		}

		a.eventPublisher.Publish(ctx, models.StatusUpdate{
			TxID:      txID,
			Status:    models.StatusImmutable,
			Timestamp: time.Now(),
		})
	}

	if len(immutableTxs) > 0 {
		a.logger.Info("marked transactions as immutable", slog.Int("count", len(immutableTxs)))
	}
}

// buildMerklePathsForSubtree constructs merkle paths for tracked transactions
func (a *Arcade) buildMerklePathsForSubtree(
	ctx context.Context,
	blockMsg p2p.BlockMessage,
	subtreeIdx int,
	subtreeRootLayer int,
	subtreeHashes []chainhash.Hash,
	txHashes []chainhash.Hash,
	tracked []chainhash.Hash,
) {
	subtreeSize := len(txHashes)
	if subtreeSize == 0 {
		return
	}

	internalHeight := int(math.Ceil(math.Log2(float64(subtreeSize))))
	if internalHeight == 0 && subtreeSize > 0 {
		internalHeight = 1
	}

	totalHeight := internalHeight + subtreeRootLayer

	for _, trackedHash := range tracked {
		// Find position of this tx in the subtree
		var txOffset uint64
		for i, h := range txHashes {
			if h == trackedHash {
				txOffset = uint64(i)
				break
			}
		}

		// Build MerklePath
		mp := &transaction.MerklePath{
			BlockHeight: blockMsg.Height,
			Path:        make([][]*transaction.PathElement, totalHeight),
		}

		// Add all txids at level 0
		for i, h := range txHashes {
			hashCopy := h
			isTxid := true
			mp.AddLeaf(0, &transaction.PathElement{
				Offset: uint64(i),
				Hash:   &hashCopy,
				Txid:   &isTxid,
			})
		}

		// Handle duplicate for odd-sized subtrees
		if subtreeSize%2 == 1 {
			dup := true
			mp.AddLeaf(0, &transaction.PathElement{
				Offset:    uint64(subtreeSize),
				Duplicate: &dup,
			})
		}

		// Add subtree roots at the appropriate layer
		subtreeBaseOffset := uint64(subtreeIdx) << uint(internalHeight-1)
		for i, subHash := range subtreeHashes {
			hashCopy := subHash
			mp.AddLeaf(internalHeight, &transaction.PathElement{
				Offset: subtreeBaseOffset + uint64(i),
				Hash:   &hashCopy,
			})
		}

		// Compute intermediate hashes
		mp.ComputeMissingHashes()

		// Extract minimal path
		minimalPath := a.extractMinimalPath(mp, txOffset)

		// Update transaction status
		status := &models.TransactionStatus{
			TxID:        trackedHash.String(),
			Status:      models.StatusMined,
			Timestamp:   time.Now(),
			BlockHash:   blockMsg.Hash,
			BlockHeight: uint64(blockMsg.Height),
			MerklePath:  minimalPath.Bytes(),
		}

		if err := a.statusStore.UpdateStatus(ctx, status); err != nil {
			a.logger.Error("failed to update mined status",
				slog.String("txID", trackedHash.String()),
				slog.String("error", err.Error()))
			continue
		}

		a.txTracker.SetMinedHash(trackedHash, uint64(blockMsg.Height))

		a.eventPublisher.Publish(ctx, models.StatusUpdate{
			TxID:      trackedHash.String(),
			Status:    models.StatusMined,
			Timestamp: time.Now(),
		})
	}
}

// extractMinimalPath extracts the minimal merkle path for a specific txid
func (a *Arcade) extractMinimalPath(fullPath *transaction.MerklePath, txOffset uint64) *transaction.MerklePath {
	mp := &transaction.MerklePath{
		BlockHeight: fullPath.BlockHeight,
		Path:        make([][]*transaction.PathElement, len(fullPath.Path)),
	}

	offset := txOffset
	for level := 0; level < len(fullPath.Path); level++ {
		if leaf := fullPath.FindLeafByOffset(level, offset); leaf != nil {
			mp.AddLeaf(level, leaf)
		}
		if sibling := fullPath.FindLeafByOffset(level, offset^1); sibling != nil {
			mp.AddLeaf(level, sibling)
		}
		offset = offset >> 1
	}

	return mp
}

// handleSubtreeMessages processes subtree announcements
func (a *Arcade) handleSubtreeMessages(ctx context.Context, subtreeChan <-chan p2p.SubtreeMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case subtreeMsg, ok := <-subtreeChan:
			if !ok {
				return
			}
			a.processSubtreeMessage(ctx, subtreeMsg)
		}
	}
}

func (a *Arcade) processSubtreeMessage(ctx context.Context, subtreeMsg p2p.SubtreeMessage) {
	if a.txTracker == nil {
		return
	}

	hashes, err := a.fetchSubtreeHashes(ctx, subtreeMsg.DataHubURL, subtreeMsg.Hash)
	if err != nil {
		a.logger.Error("failed to fetch subtree hashes",
			slog.String("hash", subtreeMsg.Hash),
			slog.String("error", err.Error()))
		return
	}

	tracked := a.txTracker.FilterTrackedHashes(hashes)
	if len(tracked) == 0 {
		return
	}

	a.logger.Info("found tracked transactions in subtree",
		slog.String("hash", subtreeMsg.Hash),
		slog.Int("tracked", len(tracked)))

	for _, hash := range tracked {
		txID := hash.String()
		status := &models.TransactionStatus{
			TxID:      txID,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now(),
		}

		if err := a.statusStore.UpdateStatus(ctx, status); err != nil {
			a.logger.Error("failed to update seen status",
				slog.String("txID", txID),
				slog.String("error", err.Error()))
			continue
		}

		a.txTracker.UpdateStatusHash(hash, models.StatusSeenOnNetwork)

		a.eventPublisher.Publish(ctx, models.StatusUpdate{
			TxID:      txID,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now(),
		})
	}
}

// handleRejectedTxMessages processes rejected transaction messages
func (a *Arcade) handleRejectedTxMessages(ctx context.Context, rejectedTxChan <-chan p2p.RejectedTxMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case rejectedMsg, ok := <-rejectedTxChan:
			if !ok {
				return
			}
			a.processRejectedTxMessage(ctx, rejectedMsg)
		}
	}
}

func (a *Arcade) processRejectedTxMessage(ctx context.Context, rejectedMsg p2p.RejectedTxMessage) {
	if a.txTracker == nil {
		return
	}

	a.logger.Debug("received rejected-tx message",
		slog.String("txID", rejectedMsg.TxID),
		slog.String("reason", rejectedMsg.Reason))

	txStatus := models.StatusRejected
	if strings.Contains(strings.ToLower(rejectedMsg.Reason), "double spend") {
		txStatus = models.StatusDoubleSpendAttempted
	}

	status := &models.TransactionStatus{
		TxID:      rejectedMsg.TxID,
		Status:    txStatus,
		Timestamp: time.Now(),
		ExtraInfo: rejectedMsg.Reason,
	}

	if err := a.statusStore.UpdateStatus(ctx, status); err != nil {
		a.logger.Error("failed to update rejected status",
			slog.String("txID", rejectedMsg.TxID),
			slog.String("error", err.Error()))
		return
	}

	a.eventPublisher.Publish(ctx, models.StatusUpdate{
		TxID:      rejectedMsg.TxID,
		Status:    txStatus,
		Timestamp: time.Now(),
	})
}

// handleNodeStatusMessages processes node status messages
func (a *Arcade) handleNodeStatusMessages(ctx context.Context, nodeStatusChan <-chan p2p.NodeStatusMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-nodeStatusChan:
			if !ok {
				return
			}
			a.logger.Debug("received node_status message",
				slog.String("peerID", msg.PeerID),
				slog.String("clientName", msg.ClientName))
		}
	}
}

// handleReorg reverts transaction statuses for orphaned blocks.
// This is called AFTER new canonical blocks have been processed, so most txs
// will already have new merkle proofs. Only txs still pointing to orphaned
// block hashes need to be reverted to SEEN_ON_NETWORK.
func (a *Arcade) handleReorg(ctx context.Context, orphanedBlocks []*BlockHeader) {
	a.logger.Warn("handling reorg", slog.Int("orphanedBlocks", len(orphanedBlocks)))

	for _, orphan := range orphanedBlocks {
		blockHash := orphan.Hash.String()

		// Revert any remaining transactions still pointing to this orphaned block
		revertedTxIDs, err := a.statusStore.SetStatusByBlockHash(ctx, blockHash, models.StatusSeenOnNetwork)
		if err != nil {
			a.logger.Error("failed to revert transactions for orphaned block",
				slog.String("blockHash", blockHash),
				slog.String("error", err.Error()))
			continue
		}

		// Update in-memory tracker and emit events for reverted transactions
		for _, txID := range revertedTxIDs {
			a.txTracker.UpdateStatus(txID, models.StatusSeenOnNetwork)

			a.eventPublisher.Publish(ctx, models.StatusUpdate{
				TxID:      txID,
				Status:    models.StatusSeenOnNetwork,
				Timestamp: time.Now(),
			})

			a.logger.Info("reverted transaction due to reorg",
				slog.String("txID", txID),
				slog.String("orphanedBlock", blockHash))
		}

		if len(revertedTxIDs) > 0 {
			a.logger.Info("reverted transactions for orphaned block",
				slog.String("blockHash", blockHash),
				slog.Int("count", len(revertedTxIDs)))
		}
	}
}

// periodicOrphanCheck periodically checks for orphaned transactions
func (a *Arcade) periodicOrphanCheck(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.logger.Debug("performing periodic orphan check")
		}
	}
}

// setChainTip updates the chain tip and returns any reorged blocks
func (a *Arcade) setChainTip(_ context.Context, newTip *BlockHeader) []*BlockHeader {
	a.mu.Lock()
	defer a.mu.Unlock()

	var reorgedBlocks []*BlockHeader
	oldTip := a.tip

	if oldTip != nil && newTip.Header.PrevHash != oldTip.Hash {
		reorgedBlocks = a.findReorgedBlocks(oldTip, newTip)
	}

	// Update byHeight
	headers := []*BlockHeader{newTip}
	current := newTip
	for {
		if int(current.Height) < len(a.byHeight) && a.byHeight[current.Height] == current.Hash {
			break
		}
		parent, ok := a.byHash[current.Header.PrevHash]
		if !ok {
			break
		}
		headers = append([]*BlockHeader{parent}, headers...)
		current = parent
	}

	for _, h := range headers {
		for len(a.byHeight) <= int(h.Height) {
			a.byHeight = append(a.byHeight, chainhash.Hash{})
		}
		a.byHeight[h.Height] = h.Hash
	}

	a.tip = newTip
	a.pruneOrphans()

	return reorgedBlocks
}

func (a *Arcade) findReorgedBlocks(oldTip, newTip *BlockHeader) []*BlockHeader {
	var orphaned []*BlockHeader

	current := oldTip
	for current != nil {
		if a.isAncestor(current.Hash, newTip) {
			break
		}
		orphaned = append(orphaned, current)
		parent, ok := a.byHash[current.Header.PrevHash]
		if !ok {
			break
		}
		current = parent
	}

	return orphaned
}

func (a *Arcade) isAncestor(ancestorHash chainhash.Hash, descendant *BlockHeader) bool {
	current := descendant
	for current != nil {
		if current.Hash == ancestorHash {
			return true
		}
		parent, ok := a.byHash[current.Header.PrevHash]
		if !ok {
			break
		}
		current = parent
	}
	return false
}

func (a *Arcade) pruneOrphans() {
	if a.tip == nil {
		return
	}

	pruneHeight := uint32(0)
	if a.tip.Height > 100 {
		pruneHeight = a.tip.Height - 100
	}

	for hash, header := range a.byHash {
		if int(header.Height) < len(a.byHeight) && a.byHeight[header.Height] == hash {
			continue
		}
		if header.Height < pruneHeight {
			delete(a.byHash, hash)
		}
	}
}

// GetTip returns the current chain tip
func (a *Arcade) GetTip(_ context.Context) *BlockHeader {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.tip
}

// GetHeight returns the current chain height
func (a *Arcade) GetHeight(_ context.Context) uint32 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.tip == nil {
		return 0
	}
	return a.tip.Height
}

// GetHeaderByHeight retrieves a header by height
func (a *Arcade) GetHeaderByHeight(_ context.Context, height uint32) (*BlockHeader, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if height >= uint32(len(a.byHeight)) {
		return nil, fmt.Errorf("header not found at height %d", height)
	}

	hash := a.byHeight[height]
	header, ok := a.byHash[hash]
	if !ok {
		return nil, fmt.Errorf("header not found for hash at height %d", height)
	}

	return header, nil
}

// GetHeaderByHash retrieves a header by hash
func (a *Arcade) GetHeaderByHash(_ context.Context, hash *chainhash.Hash) (*BlockHeader, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	header, ok := a.byHash[*hash]
	if !ok {
		return nil, fmt.Errorf("header not found: %s", hash.String())
	}

	return header, nil
}

// GetNetwork returns the network name
func (a *Arcade) GetNetwork(_ context.Context) (string, error) {
	return a.network, nil
}

// IsValidRootForHeight implements chaintracker.ChainTracker interface
func (a *Arcade) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	header, err := a.GetHeaderByHeight(ctx, height)
	if err != nil {
		return false, err
	}
	return header.MerkleRoot.IsEqual(root), nil
}

// CurrentHeight implements chaintracker.ChainTracker interface
func (a *Arcade) CurrentHeight(ctx context.Context) (uint32, error) {
	return a.GetHeight(ctx), nil
}

// HTTP fetching methods

func (a *Arcade) fetchBlockSubtreeHashes(ctx context.Context, dataHubURL, blockHash string) ([]chainhash.Hash, error) {
	url := fmt.Sprintf("%s/block/%s", strings.TrimSuffix(dataHubURL, "/"), blockHash)
	return a.fetchHashes(ctx, url)
}

func (a *Arcade) fetchSubtreeHashes(ctx context.Context, dataHubURL, subtreeHash string) ([]chainhash.Hash, error) {
	url := fmt.Sprintf("%s/subtree/%s", strings.TrimSuffix(dataHubURL, "/"), subtreeHash)
	return a.fetchHashes(ctx, url)
}

func (a *Arcade) fetchHashes(ctx context.Context, url string) ([]chainhash.Hash, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	hashes := make([]chainhash.Hash, 0)
	hashBuf := make([]byte, 32)

	for {
		n, err := io.ReadFull(resp.Body, hashBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read hash: %w", err)
		}
		if n != 32 {
			return nil, fmt.Errorf("invalid hash size: %d", n)
		}

		hash, err := chainhash.NewHashFromHex(hex.EncodeToString(hashBuf))
		if err != nil {
			return nil, fmt.Errorf("failed to create hash: %w", err)
		}
		hashes = append(hashes, *hash)
	}

	return hashes, nil
}

// Placeholder methods

func (a *Arcade) loadFromLocalFiles(_ context.Context) error {
	// TODO: Load chain state from local files
	return nil
}

func (a *Arcade) runBootstrapSync(_ context.Context, _ string) {
	// TODO: Bootstrap sync from remote
}

func (a *Arcade) crawlBackAndMerge(_ context.Context, _ *block.Header, _ uint32, _ string) error {
	// TODO: Crawl back to find common ancestor
	return nil
}

func calculateWork(_ uint32) *big.Int {
	// TODO: Implement work calculation from bits
	return big.NewInt(1)
}
