// Package arcade provides unified blockchain tracking and transaction status management.
package arcade

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/p2p"
	"github.com/bsv-blockchain/arcade/store"
	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
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

	// SubscribeTip returns a channel for chain tip updates.
	// Multiple subscribers receive the same updates (fan-out).
	// Channel is closed when the context is cancelled.
	SubscribeTip(ctx context.Context) <-chan *BlockHeader

	// SubscribeStatus returns a channel for transaction status updates.
	// If token is empty, all status updates are returned.
	// If token is provided, only updates for transactions with that callback token are returned.
	// Channel is closed when the context is cancelled.
	// Returns nil if transaction tracking is not configured.
	SubscribeStatus(ctx context.Context, token string) <-chan *models.TransactionStatus

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

// CDNMetadata represents the JSON metadata file structure for header checkpoints
type CDNMetadata struct {
	RootFolder     string         `json:"rootFolder"`
	JSONFilename   string         `json:"jsonFilename"`
	HeadersPerFile int            `json:"headersPerFile"`
	Files          []CDNFileEntry `json:"files"`
}

// CDNFileEntry represents a single file entry in the metadata
type CDNFileEntry struct {
	Chain         string         `json:"chain"`
	Count         int            `json:"count"`
	FileHash      string         `json:"fileHash"`
	FileName      string         `json:"fileName"`
	FirstHeight   uint32         `json:"firstHeight"`
	LastChainWork string         `json:"lastChainWork"`
	LastHash      chainhash.Hash `json:"lastHash"`
	PrevChainWork string         `json:"prevChainWork"`
	PrevHash      chainhash.Hash `json:"prevHash"`
	SourceURL     string         `json:"sourceUrl"`
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

	// BootstrapURL for initial sync (optional)
	BootstrapURL string

	// Transaction tracking (optional - only needed for tx status management)
	TxTracker       *store.TxTracker
	StatusStore     store.StatusStore
	SubmissionStore store.SubmissionStore
	EventPublisher  events.Publisher
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
	localStoragePath  string
	network           string
	txTrackingEnabled bool
	logger            *slog.Logger
	httpClient        *http.Client

	// P2P
	p2pClient *p2p.Client

	// Transaction tracking (optional)
	txTracker       *store.TxTracker
	statusStore     store.StatusStore
	submissionStore store.SubmissionStore
	eventPublisher  events.Publisher

	// Subscribers (fan-out channels)
	subMu         sync.RWMutex
	tipSubs       []*tipSubscriber
	statusSubs    []*statusSubscriber
	statusSubDone chan struct{}
}

// tipSubscriber holds a tip channel and its context for cleanup
type tipSubscriber struct {
	ch  chan *BlockHeader
	ctx context.Context
}

// statusSubscriber holds a status channel, context, and optional token filter
type statusSubscriber struct {
	ch    chan *models.TransactionStatus
	ctx   context.Context
	token string // empty means all updates
}

// NewArcade creates a new P2P-based Arcade instance.
// For chain tracking only, omit TxTracker/StatusStore/EventPublisher.
// For full transaction status management, provide all dependencies.
// If P2PClient is nil, a default client will be created using Network and LocalStoragePath.
func NewArcade(ctx context.Context, cfg Config) (*Arcade, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Auto-create P2P client if not provided
	if cfg.P2PClient == nil {
		storagePath := cfg.LocalStoragePath
		if storagePath == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				storagePath = ".arcade"
			} else {
				storagePath = filepath.Join(home, ".arcade")
			}
			_ = os.MkdirAll(storagePath, 0o750)
			cfg.LocalStoragePath = storagePath
		}

		network := cfg.Network
		if network == "" {
			network = "main"
			cfg.Network = network
		}

		privKey, err := p2p.LoadOrGeneratePrivateKey(storagePath)
		if err != nil {
			return nil, fmt.Errorf("failed to load P2P private key: %w", err)
		}

		cfg.P2PClient, err = p2p.NewClient(msgbus.Config{
			Name:           "arcade",
			PrivateKey:     privKey,
			PeerCacheFile:  filepath.Join(storagePath, "peer_cache.json"),
			BootstrapPeers: p2p.BootstrapPeers(network),
			Port:           9999,
			DHTMode:        "off",
		}, network, cfg.Logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create P2P client: %w", err)
		}
	}

	// Validate tx tracking dependencies - all or none
	txTrackingEnabled := cfg.TxTracker != nil || cfg.StatusStore != nil || cfg.EventPublisher != nil
	if txTrackingEnabled {
		if cfg.TxTracker == nil || cfg.StatusStore == nil || cfg.EventPublisher == nil {
			return nil, fmt.Errorf("for transaction tracking, TxTracker, StatusStore, and EventPublisher are all required")
		}
	}

	a := &Arcade{
		byHeight:          make([]chainhash.Hash, 0, 1000000),
		byHash:            make(map[chainhash.Hash]*BlockHeader),
		network:           cfg.Network,
		localStoragePath:  cfg.LocalStoragePath,
		txTrackingEnabled: txTrackingEnabled,
		logger:            cfg.Logger,
		p2pClient:         cfg.P2PClient,
		txTracker:         cfg.TxTracker,
		statusStore:       cfg.StatusStore,
		submissionStore:   cfg.SubmissionStore,
		eventPublisher:    cfg.EventPublisher,
		httpClient:        &http.Client{Timeout: 30 * time.Second},
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

// Start begins listening for P2P messages
func (a *Arcade) Start(ctx context.Context) error {
	a.logger.Info("Starting ChainTracker P2P subscriptions",
		slog.Bool("txTracking", a.txTrackingEnabled))

	// Always subscribe to blocks for chain tracking
	blockChan := a.p2pClient.SubscribeBlocks(ctx)
	go a.handleBlockMessages(ctx, blockChan)

	// Subscribe to tx status topics only if tx tracking is enabled
	if a.txTrackingEnabled {
		subtreeChan := a.p2pClient.SubscribeSubtrees(ctx)
		go a.handleSubtreeMessages(ctx, subtreeChan)

		rejectedTxChan := a.p2pClient.SubscribeRejectedTxs(ctx)
		go a.handleRejectedTxMessages(ctx, rejectedTxChan)
	}

	// Periodic orphan check (only if tracking transactions)
	if a.txTracker != nil {
		go a.periodicOrphanCheck(ctx)
	}

	// Forward status updates from EventPublisher to status subscribers
	if a.eventPublisher != nil {
		a.statusSubDone = make(chan struct{})
		go a.forwardStatusUpdates(ctx)
	}

	a.logger.Info("ChainTracker started", slog.String("peerID", a.p2pClient.GetID()))

	return nil
}

// SubscribeTip returns a channel for chain tip updates.
// Multiple subscribers can call this method; all receive the same updates.
// Channel is closed when the context is cancelled.
func (a *Arcade) SubscribeTip(ctx context.Context) <-chan *BlockHeader {
	ch := make(chan *BlockHeader, 1)
	sub := &tipSubscriber{ch: ch, ctx: ctx}

	a.subMu.Lock()
	a.tipSubs = append(a.tipSubs, sub)
	a.subMu.Unlock()

	// Cleanup when context is cancelled
	go func() {
		<-ctx.Done()
		a.subMu.Lock()
		for i, s := range a.tipSubs {
			if s == sub {
				a.tipSubs = append(a.tipSubs[:i], a.tipSubs[i+1:]...)
				close(ch)
				break
			}
		}
		a.subMu.Unlock()
	}()

	return ch
}

// SubscribeStatus returns a channel for transaction status updates.
// If token is empty, all status updates are returned.
// If token is provided, only updates for transactions with that callback token are returned.
// Channel is closed when the context is cancelled.
// Returns nil if transaction tracking is not configured.
func (a *Arcade) SubscribeStatus(ctx context.Context, token string) <-chan *models.TransactionStatus {
	if a.eventPublisher == nil {
		return nil
	}
	ch := make(chan *models.TransactionStatus, 100)
	sub := &statusSubscriber{ch: ch, ctx: ctx, token: token}

	a.subMu.Lock()
	a.statusSubs = append(a.statusSubs, sub)
	a.subMu.Unlock()

	// Cleanup when context is cancelled
	go func() {
		<-ctx.Done()
		a.subMu.Lock()
		for i, s := range a.statusSubs {
			if s == sub {
				a.statusSubs = append(a.statusSubs[:i], a.statusSubs[i+1:]...)
				close(ch)
				break
			}
		}
		a.subMu.Unlock()
	}()

	return ch
}

// forwardStatusUpdates reads from EventPublisher and fans out to status subscribers
func (a *Arcade) forwardStatusUpdates(ctx context.Context) {
	eventCh, err := a.eventPublisher.Subscribe(ctx)
	if err != nil {
		a.logger.Error("failed to subscribe to event publisher", slog.String("error", err.Error()))
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.statusSubDone:
			return
		case status, ok := <-eventCh:
			if !ok {
				return
			}
			a.notifyStatusSubscribers(status)
		}
	}
}

// notifyStatusSubscribers sends a status update to matching status subscribers
func (a *Arcade) notifyStatusSubscribers(status *models.TransactionStatus) {
	a.subMu.RLock()
	subs := a.statusSubs
	a.subMu.RUnlock()

	for _, sub := range subs {
		// If subscriber has a token filter, check if this txid belongs to that token
		if sub.token != "" {
			if !a.txBelongsToToken(status.TxID, sub.token) {
				continue
			}
		}
		select {
		case sub.ch <- status:
		default:
			// Subscriber slow, skip
		}
	}
}

// txBelongsToToken checks if a transaction belongs to a callback token
func (a *Arcade) txBelongsToToken(txid, token string) bool {
	if a.submissionStore == nil {
		return false
	}
	subs, err := a.submissionStore.GetSubmissionsByToken(context.Background(), token)
	if err != nil {
		return false
	}
	for _, sub := range subs {
		if sub.TxID == txid {
			return true
		}
	}
	return false
}

// notifyTipSubscribers sends a tip update to all tip subscribers
func (a *Arcade) notifyTipSubscribers(tip *BlockHeader) {
	a.subMu.RLock()
	subs := a.tipSubs
	a.subMu.RUnlock()

	for _, sub := range subs {
		// Skip if subscriber's context is cancelled
		select {
		case <-sub.ctx.Done():
			continue
		default:
		}

		select {
		case sub.ch <- tip:
		default:
			// Channel full, replace with latest
			select {
			case <-sub.ch:
			default:
			}
			select {
			case sub.ch <- tip:
			default:
			}
		}
	}
}

// Stop gracefully shuts down the ChainTracker and closes all subscription channels
func (a *Arcade) Stop() error {
	// Stop status forwarder
	if a.statusSubDone != nil {
		close(a.statusSubDone)
	}

	// Close all subscriber channels
	a.subMu.Lock()
	for _, sub := range a.tipSubs {
		close(sub.ch)
	}
	a.tipSubs = nil
	for _, sub := range a.statusSubs {
		close(sub.ch)
	}
	a.statusSubs = nil
	a.subMu.Unlock()

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
	blockHash := header.Hash()
	parentHash := header.PrevHash

	// Check if parent exists
	parentHeader, err := a.GetHeaderByHash(ctx, &parentHash)
	if err != nil {
		// Parent not found - crawl back from datahub to find common ancestor and sync
		a.logger.Info("parent not found, crawling back", slog.String("parent", parentHash.String()))
		if err := a.crawlBackAndMerge(ctx, header, height, dataHubURL); err != nil {
			return false, nil, err
		}

		// syncFromRemoteTip added all headers including this block
		existing, _ := a.GetHeaderByHash(ctx, &blockHash)
		a.notifyTipSubscribers(existing)
		return true, nil, nil
	}

	// Calculate chainwork
	work := calculateWork(header.Bits)
	chainWork := new(big.Int).Add(parentHeader.ChainWork, work)

	blockHeader := &BlockHeader{
		Header:    header,
		Height:    height,
		Hash:      blockHash,
		ChainWork: chainWork,
	}

	// Add to byHash
	a.mu.Lock()
	a.byHash[blockHeader.Hash] = blockHeader
	a.mu.Unlock()

	// Check if this is the new tip
	currentTip := a.GetTip(ctx)
	if currentTip == nil || blockHeader.ChainWork.Cmp(currentTip.ChainWork) > 0 {
		reorgedBlocks, newCanonicalBlocks := a.setChainTip(ctx, blockHeader)

		// Set MINED status for transactions in all newly canonical blocks
		if a.txTracker != nil {
			for _, canonicalBlock := range newCanonicalBlocks {
				a.setMinedForBlock(ctx, canonicalBlock)
			}
		}

		// Notify tip subscribers
		a.notifyTipSubscribers(blockHeader)

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

// setMinedForBlock sets MINED status for all tracked transactions in a block
func (a *Arcade) setMinedForBlock(ctx context.Context, block *BlockHeader) {
	if a.statusStore == nil {
		return
	}

	statuses, err := a.statusStore.SetMinedByBlockHash(ctx, block.Hash.String())
	if err != nil {
		a.logger.Error("failed to set mined status for block",
			slog.String("blockHash", block.Hash.String()),
			slog.String("error", err.Error()))
		return
	}

	for _, status := range statuses {
		a.eventPublisher.Publish(ctx, status)
	}

	if len(statuses) > 0 {
		a.logger.Info("set transactions to MINED",
			slog.String("blockHash", block.Hash.String()),
			slog.Int("count", len(statuses)))
	}
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
		}
	}

	if len(immutableTxs) > 0 {
		a.logger.Info("marked transactions as immutable", slog.Int("count", len(immutableTxs)))
	}
}

// buildMerklePathsForSubtree constructs and stores merkle paths for tracked transactions.
// This only stores the merkle paths - status is set separately when the block becomes canonical.
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

		// Extract minimal path and store it
		minimalPath := a.extractMinimalPath(mp, txOffset)

		if err := a.statusStore.InsertMerklePath(ctx, trackedHash.String(), blockMsg.Hash, uint64(blockMsg.Height), minimalPath.Bytes()); err != nil {
			a.logger.Error("failed to store merkle path",
				slog.String("txID", trackedHash.String()),
				slog.String("blockHash", blockMsg.Hash),
				slog.String("error", err.Error()))
		}
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
	if a.txTracker == nil || a.txTracker.Count() == 0 {
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
		a.eventPublisher.Publish(ctx, status)
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

	a.eventPublisher.Publish(ctx, status)
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

			a.eventPublisher.Publish(ctx, &models.TransactionStatus{
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

// setChainTip updates the chain tip and returns orphaned blocks and newly canonical blocks
func (a *Arcade) setChainTip(ctx context.Context, newTip *BlockHeader) (orphaned []*BlockHeader, newCanonical []*BlockHeader) {
	a.mu.Lock()

	oldTip := a.tip

	if oldTip != nil && newTip.Header.PrevHash != oldTip.Hash {
		orphaned = a.findReorgedBlocks(oldTip, newTip)
	}

	// Find all blocks being added to the canonical chain
	newCanonical = []*BlockHeader{newTip}
	current := newTip
	for {
		if int(current.Height) < len(a.byHeight) && a.byHeight[current.Height] == current.Hash {
			break
		}
		parent, ok := a.byHash[current.Header.PrevHash]
		if !ok {
			break
		}
		newCanonical = append([]*BlockHeader{parent}, newCanonical...)
		current = parent
	}

	// Update byHeight
	for _, h := range newCanonical {
		for len(a.byHeight) <= int(h.Height) {
			a.byHeight = append(a.byHeight, chainhash.Hash{})
		}
		a.byHeight[h.Height] = h.Hash
	}

	a.tip = newTip
	a.pruneOrphans()

	a.mu.Unlock()

	a.logger.Info("new chain tip",
		slog.Uint64("height", uint64(newTip.Height)),
		slog.String("hash", newTip.Hash.String()))

	// Persist new headers to disk (outside lock)
	if len(newCanonical) > 0 {
		if err := a.writeHeadersToFiles(newCanonical); err != nil {
			a.logger.Error("failed to write headers to files", slog.String("error", err.Error()))
		} else if err := a.updateMetadataForTip(ctx); err != nil {
			a.logger.Error("failed to update metadata", slog.String("error", err.Error()))
		}
	}

	return orphaned, newCanonical
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

// GetPeers returns information about connected P2P peers
func (a *Arcade) GetPeers() []p2p.PeerInfo {
	return a.p2pClient.GetPeers()
}

// GetPeerID returns this node's P2P peer ID
func (a *Arcade) GetPeerID() string {
	return a.p2pClient.GetID()
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

// loadFromLocalFiles restores the chain from local header files.
// First checks the storage path for user's saved state, then falls back to embedded checkpoint files.
func (a *Arcade) loadFromLocalFiles(ctx context.Context) error {
	// Determine metadata path - check storage path first, then embedded checkpoints
	metadataPath := filepath.Join(a.localStoragePath, a.network+"NetBlockHeaders.json")
	basePath := a.localStoragePath

	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// Fall back to embedded checkpoint files
		checkpointPath := filepath.Join("data", "headers")
		checkpointMetadata := filepath.Join(checkpointPath, a.network+"NetBlockHeaders.json")

		if _, err := os.Stat(checkpointMetadata); os.IsNotExist(err) {
			a.logger.Info("No checkpoint files found, initializing from genesis block",
				slog.String("network", a.network))
			return a.initializeFromGenesis()
		}

		a.logger.Info("Loading from embedded checkpoint", slog.String("path", checkpointMetadata))
		metadataPath = checkpointMetadata
		basePath = checkpointPath
	} else {
		a.logger.Info("Loading from storage path", slog.String("path", metadataPath))
	}

	metadata, err := parseMetadata(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	a.logger.Info("Found checkpoint files to load", slog.Int("files", len(metadata.Files)))

	for _, fileEntry := range metadata.Files {
		filePath := filepath.Join(basePath, fileEntry.FileName)
		headers, err := loadHeadersFromFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to load file %s: %w", fileEntry.FileName, err)
		}

		// Calculate chainwork incrementally
		var prevChainWork *big.Int
		if fileEntry.FirstHeight == 0 {
			prevChainWork = big.NewInt(0)
		} else {
			prevHeader, err := a.GetHeaderByHeight(ctx, fileEntry.FirstHeight-1)
			if err != nil {
				return fmt.Errorf("failed to get previous header at height %d: %w", fileEntry.FirstHeight-1, err)
			}
			prevChainWork = prevHeader.ChainWork
		}

		a.mu.Lock()
		for i, header := range headers {
			height := fileEntry.FirstHeight + uint32(i)

			var chainWork *big.Int
			if height == 0 {
				chainWork = big.NewInt(0)
			} else {
				work := calculateWork(header.Bits)
				chainWork = new(big.Int).Add(prevChainWork, work)
				prevChainWork = chainWork
			}

			blockHeader := &BlockHeader{
				Header:    header,
				Height:    height,
				Hash:      header.Hash(),
				ChainWork: chainWork,
			}

			// Ensure slice is large enough
			for uint32(len(a.byHeight)) <= height {
				a.byHeight = append(a.byHeight, chainhash.Hash{})
			}

			a.byHeight[height] = blockHeader.Hash
			a.byHash[blockHeader.Hash] = blockHeader
			a.tip = blockHeader
		}
		a.mu.Unlock()

		a.logger.Info("Loaded headers from file",
			slog.String("file", fileEntry.FileName),
			slog.Int("count", len(headers)))
	}

	if a.tip != nil {
		a.logger.Info("Chain loaded",
			slog.Uint64("height", uint64(a.tip.Height)),
			slog.String("tip", a.tip.Hash.String()))
	}

	return nil
}

// initializeFromGenesis sets up the chain with just the genesis block for the configured network.
func (a *Arcade) initializeFromGenesis() error {
	header, err := getGenesisHeader(a.network)
	if err != nil {
		return fmt.Errorf("failed to get genesis header: %w", err)
	}

	genesis := &BlockHeader{
		Header:    header,
		Height:    0,
		Hash:      header.Hash(),
		ChainWork: big.NewInt(0),
	}

	a.mu.Lock()
	a.byHeight = append(a.byHeight, genesis.Hash)
	a.byHash[genesis.Hash] = genesis
	a.tip = genesis
	a.mu.Unlock()

	// Persist to storage so subsequent runs load from local files
	if err := a.writeHeadersToFiles([]*BlockHeader{genesis}); err != nil {
		return fmt.Errorf("failed to persist genesis block: %w", err)
	}
	if err := a.updateMetadataForTip(context.Background()); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	a.logger.Info("Initialized from genesis block",
		slog.String("hash", genesis.Hash.String()))

	return nil
}

// parseMetadata reads and parses the metadata JSON file
func parseMetadata(path string) (*CDNMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata CDNMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}

	return &metadata, nil
}

// loadHeadersFromFile reads a binary .headers file and returns a slice of headers
func loadHeadersFromFile(path string) ([]*block.Header, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	if len(data)%80 != 0 {
		return nil, fmt.Errorf("invalid file size: %d bytes (not multiple of 80)", len(data))
	}

	headerCount := len(data) / 80
	headers := make([]*block.Header, 0, headerCount)

	for i := 0; i < headerCount; i++ {
		headerBytes := data[i*80 : (i+1)*80]
		header, err := block.NewHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse header at index %d: %w", i, err)
		}
		headers = append(headers, header)
	}

	return headers, nil
}

// writeHeadersToFiles writes headers to the appropriate .headers files for persistence
func (a *Arcade) writeHeadersToFiles(headers []*BlockHeader) error {
	if a.localStoragePath == "" {
		return nil
	}

	if err := os.MkdirAll(a.localStoragePath, 0o750); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Group headers by file (100,000 headers per file)
	fileHeaders := make(map[uint32][]*BlockHeader)
	for _, header := range headers {
		fileIndex := header.Height / 100000
		fileHeaders[fileIndex] = append(fileHeaders[fileIndex], header)
	}

	// Write to each file
	for fileIndex, hdrs := range fileHeaders {
		fileName := fmt.Sprintf("%s_%d.headers", a.network, fileIndex)
		filePath := filepath.Join(a.localStoragePath, fileName)

		f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", fileName, err)
		}

		for _, header := range hdrs {
			positionInFile := (header.Height % 100000) * 80
			if _, err := f.Seek(int64(positionInFile), 0); err != nil {
				_ = f.Close()
				return fmt.Errorf("failed to seek in file: %w", err)
			}

			if _, err := f.Write(header.Bytes()); err != nil {
				_ = f.Close()
				return fmt.Errorf("failed to write header: %w", err)
			}
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
	}

	return nil
}

// updateMetadataForTip updates the metadata JSON with current chain tip info
func (a *Arcade) updateMetadataForTip(ctx context.Context) error {
	if a.localStoragePath == "" {
		return nil
	}

	metadataPath := filepath.Join(a.localStoragePath, a.network+"NetBlockHeaders.json")

	// Read existing metadata or create new
	var metadata *CDNMetadata
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		metadata = &CDNMetadata{
			RootFolder:     "",
			JSONFilename:   a.network + "NetBlockHeaders.json",
			HeadersPerFile: 100000,
			Files:          []CDNFileEntry{},
		}
	} else {
		var err error
		metadata, err = parseMetadata(metadataPath)
		if err != nil {
			return fmt.Errorf("failed to parse existing metadata: %w", err)
		}
	}

	tip := a.GetTip(ctx)
	if tip == nil {
		return nil
	}

	fileIndex := tip.Height / 100000

	// Ensure we have entries for all files up to the current tip
	for i := uint32(len(metadata.Files)); i <= fileIndex; i++ {
		metadata.Files = append(metadata.Files, CDNFileEntry{
			Chain:         a.network,
			Count:         0,
			FileHash:      "",
			FileName:      fmt.Sprintf("%s_%d.headers", a.network, i),
			FirstHeight:   i * 100000,
			LastChainWork: "0000000000000000000000000000000000000000000000000000000000000000",
			PrevChainWork: "0000000000000000000000000000000000000000000000000000000000000000",
			SourceURL:     "",
		})
	}

	// Update the last file entry with current tip info
	lastFileEntry := &metadata.Files[fileIndex]
	lastFileEntry.Count = int((tip.Height % 100000) + 1)
	lastFileEntry.LastChainWork = chainWorkToHex(tip.ChainWork)
	lastFileEntry.LastHash = tip.Hash

	// Get previous header for prevChainWork and prevHash
	if tip.Height > 0 {
		prevHeader, err := a.GetHeaderByHeight(ctx, tip.Height-1)
		if err == nil {
			lastFileEntry.PrevChainWork = chainWorkToHex(prevHeader.ChainWork)
			lastFileEntry.PrevHash = prevHeader.Hash
		}
	}

	// Write updated metadata
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

const (
	maxHeadersPerRequest = 1000
	headerSize           = 80
)

func (a *Arcade) runBootstrapSync(ctx context.Context, baseURL string) {
	if baseURL == "" {
		return
	}

	remoteTipHash, err := a.fetchLatestBlock(ctx, baseURL)
	if err != nil {
		a.logger.Error("failed to fetch latest block for bootstrap", slog.String("error", err.Error()))
		return
	}

	a.logger.Info("Starting bootstrap sync", slog.String("remoteTip", remoteTipHash.String()))

	if err := a.syncFromRemoteTip(ctx, remoteTipHash, baseURL); err != nil {
		a.logger.Error("bootstrap sync failed", slog.String("error", err.Error()))
	}
}

func (a *Arcade) crawlBackAndMerge(ctx context.Context, header *block.Header, _ uint32, dataHubURL string) error {
	return a.syncFromRemoteTip(ctx, header.Hash(), dataHubURL)
}

// syncFromRemoteTip walks backwards from a remote tip to find common ancestor,
// then imports the entire branch. Used for both bootstrap sync and P2P block
// messages with unknown parents.
func (a *Arcade) syncFromRemoteTip(ctx context.Context, remoteTipHash chainhash.Hash, baseURL string) error {
	// Check if we already have the remote tip
	if _, err := a.GetHeaderByHash(ctx, &remoteTipHash); err == nil {
		a.logger.Debug("already have block", slog.String("hash", remoteTipHash.String()))
		return nil
	}

	a.logger.Info("Walking backwards to find common ancestor", slog.String("remoteTip", remoteTipHash.String()))

	branch := make([]*block.Header, 0, 10000)
	currentHash := remoteTipHash
	var commonAncestor *BlockHeader

	startTime := time.Now()
	for {
		// Check if we have this block in our chain
		if existingHeader, err := a.GetHeaderByHash(ctx, &currentHash); err == nil {
			commonAncestor = existingHeader
			a.logger.Info("Found common ancestor",
				slog.Uint64("height", uint64(commonAncestor.Height)),
				slog.Duration("elapsed", time.Since(startTime)))
			break
		}

		// Fetch batch of headers walking backwards
		headers, err := a.fetchHeadersBackward(ctx, baseURL, currentHash.String(), maxHeadersPerRequest)
		if err != nil {
			return fmt.Errorf("failed to fetch headers backward from %s: %w", currentHash.String(), err)
		}

		if len(headers) == 0 {
			return fmt.Errorf("no headers returned from %s/headers/%s", baseURL, currentHash.String())
		}

		a.logger.Debug("Fetched headers batch",
			slog.Int("count", len(headers)),
			slog.String("from", currentHash.String()))

		// Add headers to branch (they're in reverse order - newest first)
		branch = append(branch, headers...)

		// Check if any of the fetched headers exist in our chain
		found := false
		for i, header := range headers {
			hash := header.Hash()
			if existingHeader, err := a.GetHeaderByHash(ctx, &hash); err == nil {
				commonAncestor = existingHeader
				// Trim the branch to only include headers after the common ancestor
				branch = branch[:len(branch)-len(headers)+i]
				found = true
				a.logger.Info("Found common ancestor",
					slog.Uint64("height", uint64(commonAncestor.Height)),
					slog.Duration("elapsed", time.Since(startTime)))
				break
			}
		}

		if found {
			break
		}

		// Continue from the last header's parent
		currentHash = headers[len(headers)-1].PrevHash
	}

	if commonAncestor == nil {
		return fmt.Errorf("common ancestor not found")
	}

	if len(branch) == 0 {
		a.logger.Debug("No new headers to sync")
		return nil
	}

	a.logger.Info("Importing new headers", slog.Int("count", len(branch)))

	// Reverse branch (it's currently newest to oldest, we need oldest to newest)
	for i := 0; i < len(branch)/2; i++ {
		branch[i], branch[len(branch)-1-i] = branch[len(branch)-1-i], branch[i]
	}

	// Calculate heights and chainwork for the entire branch
	blockHeaders := make([]*BlockHeader, len(branch))
	currentHeight := commonAncestor.Height + 1
	currentChainWork := commonAncestor.ChainWork

	for i, header := range branch {
		work := calculateWork(header.Bits)
		currentChainWork = new(big.Int).Add(currentChainWork, work)

		blockHeaders[i] = &BlockHeader{
			Header:    header,
			Height:    currentHeight,
			Hash:      header.Hash(),
			ChainWork: new(big.Int).Set(currentChainWork),
		}
		currentHeight++
	}

	// Import headers to chain state
	a.mu.Lock()
	for _, bh := range blockHeaders {
		for uint32(len(a.byHeight)) <= bh.Height {
			a.byHeight = append(a.byHeight, chainhash.Hash{})
		}
		a.byHeight[bh.Height] = bh.Hash
		a.byHash[bh.Hash] = bh
	}
	a.tip = blockHeaders[len(blockHeaders)-1]
	a.mu.Unlock()

	// Persist to disk
	if err := a.writeHeadersToFiles(blockHeaders); err != nil {
		a.logger.Error("failed to write headers to files", slog.String("error", err.Error()))
	} else if err := a.updateMetadataForTip(ctx); err != nil {
		a.logger.Error("failed to update metadata", slog.String("error", err.Error()))
	}

	a.logger.Info("Sync complete",
		slog.Uint64("tipHeight", uint64(a.tip.Height)),
		slog.String("tipHash", a.tip.Hash.String()),
		slog.Int("headersAdded", len(blockHeaders)))

	return nil
}

// fetchLatestBlock gets the latest block hash from the node's bestblockheader endpoint
func (a *Arcade) fetchLatestBlock(ctx context.Context, baseURL string) (chainhash.Hash, error) {
	url := fmt.Sprintf("%s/bestblockheader", strings.TrimSuffix(baseURL, "/"))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to fetch best block header: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return chainhash.Hash{}, fmt.Errorf("best block header request failed: status %d", resp.StatusCode)
	}

	headerBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to read response: %w", err)
	}

	if len(headerBytes) != headerSize {
		return chainhash.Hash{}, fmt.Errorf("invalid header size: expected %d, got %d", headerSize, len(headerBytes))
	}

	header, err := block.NewHeaderFromBytes(headerBytes)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to parse header: %w", err)
	}

	return header.Hash(), nil
}

// fetchHeadersBackward fetches headers walking backwards from a starting hash.
// Returns headers in reverse chronological order (newest first).
func (a *Arcade) fetchHeadersBackward(ctx context.Context, baseURL, startHash string, count int) ([]*block.Header, error) {
	url := fmt.Sprintf("%s/headers/%s?n=%d", strings.TrimSuffix(baseURL, "/"), startHash, count)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch headers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch headers failed: status %d", resp.StatusCode)
	}

	headerBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if len(headerBytes)%headerSize != 0 {
		return nil, fmt.Errorf("invalid header data length: %d bytes", len(headerBytes))
	}

	numHeaders := len(headerBytes) / headerSize
	headers := make([]*block.Header, numHeaders)

	for i := 0; i < numHeaders; i++ {
		start := i * headerSize
		end := start + headerSize
		headerData := headerBytes[start:end]

		header, err := block.NewHeaderFromBytes(headerData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse header %d: %w", i, err)
		}

		headers[i] = header
	}

	return headers, nil
}

// oneLsh256 is 1 shifted left 256 bits (used for chainwork calculation)
var oneLsh256 = new(big.Int).Lsh(big.NewInt(1), 256)

// compactToBig converts a compact representation of a 256-bit number (as used in Bitcoin difficulty)
// to a big.Int. The compact format is a special floating point notation where:
// - The first byte is the exponent (number of bytes)
// - The remaining 3 bytes are the mantissa
// - The sign bit (0x00800000) indicates if the number is negative
func compactToBig(compact uint32) *big.Int {
	mantissa := compact & 0x007fffff
	isNegative := compact&0x00800000 != 0
	exponent := uint(compact >> 24)

	var bn *big.Int
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		bn = big.NewInt(int64(mantissa))
	} else {
		bn = big.NewInt(int64(mantissa))
		bn.Lsh(bn, 8*(exponent-3))
	}

	if isNegative {
		bn = bn.Neg(bn)
	}

	return bn
}

// calculateWork calculates the work represented by a given difficulty target (bits).
// Work is calculated as: work = 2^256 / (target + 1)
func calculateWork(bits uint32) *big.Int {
	target := compactToBig(bits)

	if target.Sign() <= 0 {
		return big.NewInt(0)
	}

	denominator := new(big.Int).Add(target, big.NewInt(1))
	return new(big.Int).Div(oneLsh256, denominator)
}

// chainWorkToHex converts chainwork to a 64-character hex string (padded)
func chainWorkToHex(work *big.Int) string {
	hexStr := work.Text(16)
	for len(hexStr) < 64 {
		hexStr = "0" + hexStr
	}
	return hexStr
}
