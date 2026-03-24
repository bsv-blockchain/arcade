// Package arcade provides transaction broadcast and status tracking for BSV.
package arcade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/util"
	p2p "github.com/bsv-blockchain/go-teranode-p2p-client"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Static error variables for configuration validation.
var (
	errP2PClientRequired      = errors.New("p2p client is required")
	errChaintracksRequired    = errors.New("chaintracks is required")
	errTxTrackerRequired      = errors.New("tx tracker is required")
	errStoreRequired          = errors.New("store is required")
	errEventPublisherRequired = errors.New("event publisher is required")
	errChaintracksNoTip       = errors.New("chaintracks has no tip")
	errUnexpectedStatusCode   = errors.New("unexpected status code")
)

// Config holds configuration for Arcade
type Config struct {
	// P2PClient for network communication (required)
	P2PClient *p2p.Client

	// Chaintracks for tip updates, block header lookups, and merkle root validation (required)
	Chaintracks chaintracks.Chaintracks

	// Logger for structured logging
	Logger *slog.Logger

	// Transaction tracking stores (required)
	TxTracker      *store.TxTracker
	Store          store.Store
	EventPublisher events.Publisher

	// DataHubURLs are URLs for fetching block/subtree data
	DataHubURLs []string
}

// Arcade tracks transaction statuses via P2P network messages.
type Arcade struct {
	p2pClient   *p2p.Client
	chaintracks chaintracks.Chaintracks
	logger      *slog.Logger
	httpClient  *http.Client

	// Transaction tracking
	txTracker      *store.TxTracker
	store          store.Store
	eventPublisher events.Publisher

	// DataHub URLs for fetching block/subtree data
	dataHubURLs []string

	// Status subscribers (fan-out)
	subMu         sync.RWMutex
	statusSubs    []*statusSubscriber
	statusSubDone chan struct{}
}

// statusSubscriber holds a status channel, context, and optional token filter
type statusSubscriber struct {
	ch    chan *models.TransactionStatus
	ctx   context.Context //nolint:containedctx // context needed for subscriber lifecycle
	token string          // empty means all updates
}

// NewArcade creates a new Arcade instance.
func NewArcade(cfg Config) (*Arcade, error) {
	if cfg.P2PClient == nil {
		return nil, errP2PClientRequired
	}
	if cfg.Chaintracks == nil {
		return nil, errChaintracksRequired
	}
	if cfg.TxTracker == nil {
		return nil, errTxTrackerRequired
	}
	if cfg.Store == nil {
		return nil, errStoreRequired
	}
	if cfg.EventPublisher == nil {
		return nil, errEventPublisherRequired
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Arcade{
		p2pClient:            cfg.P2PClient,
		chaintracks:          cfg.Chaintracks,
		logger:               cfg.Logger,
		httpClient:           &http.Client{Timeout: 30 * time.Second},
		txTracker:            cfg.TxTracker,
		store:                cfg.Store,
		eventPublisher:       cfg.EventPublisher,
		dataHubURLs: cfg.DataHubURLs,
	}, nil
}

// Start begins listening for P2P messages
func (a *Arcade) Start(ctx context.Context) error {
	a.logger.Info("Starting Arcade P2P subscriptions")

	// Subscribe to block messages for block tracking
	blockChan := a.p2pClient.SubscribeBlocks(ctx)
	go a.handleBlockMessages(ctx, blockChan)

	// Subscribe to rejected-tx messages
	rejectedTxChan := a.p2pClient.SubscribeRejectedTxs(ctx)
	go a.handleRejectedTxMessages(ctx, rejectedTxChan)

	// Forward status updates from EventPublisher to status subscribers
	a.statusSubDone = make(chan struct{})
	go a.forwardStatusUpdates(ctx)

	// Initialize block tracking
	if err := a.initializeBlockTracking(ctx); err != nil {
		a.logger.Warn("failed to initialize block tracking",
			slog.String("error", err.Error()))
		// Continue anyway - tip updates will handle catch-up
	}

	// Subscribe to chaintracks tip updates - this drives all catch-up
	tipChan := a.chaintracks.Subscribe(ctx)
	go a.handleTipUpdates(ctx, tipChan)

	a.logger.Info("Arcade started", slog.String("peerID", a.p2pClient.GetID()))
	return nil
}

// Stop gracefully shuts down Arcade
func (a *Arcade) Stop() error {
	if a.statusSubDone != nil {
		close(a.statusSubDone)
	}

	a.subMu.Lock()
	for _, sub := range a.statusSubs {
		close(sub.ch)
	}
	a.statusSubs = nil
	a.subMu.Unlock()

	return nil
}

// SubscribeStatus returns a channel for transaction status updates.
// If token is empty, all status updates are returned.
// If token is provided, only updates for transactions with that callback token are returned.
func (a *Arcade) SubscribeStatus(ctx context.Context, token string) <-chan *models.TransactionStatus {
	ch := make(chan *models.TransactionStatus, 100)
	sub := &statusSubscriber{ch: ch, ctx: ctx, token: token}

	a.subMu.Lock()
	a.statusSubs = append(a.statusSubs, sub)
	a.subMu.Unlock()

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

// GetPeers returns information about connected P2P peers
func (a *Arcade) GetPeers() []msgbus.PeerInfo {
	return a.p2pClient.GetPeers()
}

// GetPeerID returns this node's P2P peer ID
func (a *Arcade) GetPeerID() string {
	return a.p2pClient.GetID()
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
			a.notifyStatusSubscribers(ctx, status)
		}
	}
}

func (a *Arcade) notifyStatusSubscribers(ctx context.Context, status *models.TransactionStatus) {
	a.subMu.RLock()
	subs := a.statusSubs
	a.subMu.RUnlock()

	for _, sub := range subs {
		if sub.token != "" && !a.txBelongsToToken(ctx, status.TxID, sub.token) {
			continue
		}
		select {
		case sub.ch <- status:
		default:
		}
	}
}

func (a *Arcade) txBelongsToToken(ctx context.Context, txid, token string) bool {
	if a.store == nil {
		return false
	}
	subs, err := a.store.GetSubmissionsByToken(ctx, token)
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

// handleBlockMessages processes incoming block announcements
func (a *Arcade) handleBlockMessages(ctx context.Context, blockChan <-chan teranode.BlockMessage) {
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

func (a *Arcade) processBlockMessage(ctx context.Context, blockMsg teranode.BlockMessage) error {
	a.logger.Info("received block message",
		slog.String("hash", blockMsg.Hash),
		slog.Uint64("height", uint64(blockMsg.Height)))

	// Prune deeply confirmed transactions
	if a.txTracker.Count() > 0 {
		a.pruneConfirmedTransactions(ctx, blockMsg.Height)
	}

	// Mark block as processed but NOT on_chain
	// P2P block messages may arrive out of order or for blocks not yet connected to our chain
	// The tip catch-up handler will mark blocks as on_chain when it connects them
	if err := a.store.MarkBlockProcessed(ctx, blockMsg.Hash, uint64(blockMsg.Height), false); err != nil {
		a.logger.Warn("failed to mark block as processed",
			slog.String("blockHash", blockMsg.Hash),
			slog.String("error", err.Error()))
	}

	a.logger.Info("processed block message",
		slog.String("hash", blockMsg.Hash),
		slog.Uint64("height", uint64(blockMsg.Height)))

	return nil
}

func (a *Arcade) pruneConfirmedTransactions(ctx context.Context, currentHeight uint32) {
	immutableTxs := a.txTracker.PruneConfirmed(uint64(currentHeight))
	for _, hash := range immutableTxs {
		txID := hash.String()
		status := &models.TransactionStatus{
			TxID:      txID,
			Status:    models.StatusImmutable,
			Timestamp: time.Now(),
		}
		if err := a.store.UpdateStatus(ctx, status); err != nil {
			a.logger.Error("failed to update immutable status",
				slog.String("txID", txID),
				slog.String("error", err.Error()))
		}
	}

	if len(immutableTxs) > 0 {
		a.logger.Info("marked transactions as immutable", slog.Int("count", len(immutableTxs)))
	}
}

// Block tracking and catch-up methods

const startupSyncDepth = 1000 // How many blocks to mark as on_chain on first run

// initializeBlockTracking marks recent blocks as on_chain on first run (empty DB)
func (a *Arcade) initializeBlockTracking(ctx context.Context) error {
	// Check if we have any processed blocks already
	hasProcessedBlocks, err := a.store.HasAnyProcessedBlocks(ctx)
	if err != nil {
		return fmt.Errorf("failed to check for processed blocks: %w", err)
	}

	if hasProcessedBlocks {
		// Not first run - tip updates will handle any catch-up
		return nil
	}

	// First run - mark last 1000 blocks as on_chain
	tip := a.chaintracks.GetTip(ctx)
	if tip == nil {
		return errChaintracksNoTip
	}

	startHeight := uint32(0)
	if tip.Height > startupSyncDepth {
		startHeight = tip.Height - startupSyncDepth
	}

	headers, err := a.chaintracks.GetHeaders(ctx, startHeight, startupSyncDepth)
	if err != nil {
		return fmt.Errorf("failed to get headers: %w", err)
	}

	a.logger.Info("first run - marking recent blocks as on_chain",
		slog.Int("count", len(headers)),
		slog.Uint64("fromHeight", uint64(startHeight)))

	for _, header := range headers {
		if err := a.store.MarkBlockProcessed(ctx, header.Hash.String(), uint64(header.Height), true); err != nil {
			a.logger.Warn("failed to mark block as on_chain",
				slog.String("hash", header.Hash.String()),
				slog.String("error", err.Error()))
		}
	}

	a.logger.Info("block tracking initialized", slog.Int("blocksMarked", len(headers)))
	return nil
}

// handleTipUpdates listens for chaintracks tip updates and processes catch-up
func (a *Arcade) handleTipUpdates(ctx context.Context, tipChan <-chan *chaintracks.BlockHeader) {
	for {
		select {
		case <-ctx.Done():
			return
		case tip, ok := <-tipChan:
			if !ok {
				return
			}
			if err := a.processNewTip(ctx, tip); err != nil {
				a.logger.Error("failed to process new tip",
					slog.String("hash", tip.Hash.String()),
					slog.Uint64("height", uint64(tip.Height)),
					slog.String("error", err.Error()))
			}
		}
	}
}

// processNewTip handles a new chain tip from chaintracks
func (a *Arcade) processNewTip(ctx context.Context, tip *chaintracks.BlockHeader) error {
	// Find blocks not yet on our canonical chain and detect reorgs
	unprocessedBlocks, orphanedBlocks, err := a.findUnprocessedAndOrphanedBlocks(ctx, tip)
	if err != nil {
		return fmt.Errorf("failed to find unprocessed blocks: %w", err)
	}

	// Handle orphaned blocks (reorg) - reset transaction statuses
	for _, orphan := range orphanedBlocks {
		if err := a.handleOrphanedBlock(ctx, orphan); err != nil {
			a.logger.Error("failed to handle orphaned block",
				slog.String("hash", orphan.Hash),
				slog.Uint64("height", orphan.Height),
				slog.String("error", err.Error()))
		}
	}

	if len(unprocessedBlocks) == 0 {
		return nil
	}

	a.logger.Info("catching up blocks",
		slog.Int("count", len(unprocessedBlocks)),
		slog.Int("orphaned", len(orphanedBlocks)),
		slog.String("tipHash", tip.Hash.String()))

	// Process blocks oldest to newest to maintain chain continuity
	for i := len(unprocessedBlocks) - 1; i >= 0; i-- {
		header := unprocessedBlocks[i]
		if err := a.processBlockByHeader(ctx, header); err != nil {
			a.logger.Error("failed to process block",
				slog.String("hash", header.Hash.String()),
				slog.Uint64("height", uint64(header.Height)),
				slog.String("error", err.Error()))
			return err
		}
	}

	return nil
}

// OrphanedBlock represents a block that was reorged out
type OrphanedBlock struct {
	Hash   string
	Height uint64
}

// findUnprocessedAndOrphanedBlocks walks back from tip to find blocks not on our chain and detect reorgs
func (a *Arcade) findUnprocessedAndOrphanedBlocks(ctx context.Context, tip *chaintracks.BlockHeader) ([]*chaintracks.BlockHeader, []OrphanedBlock, error) {
	var unprocessed []*chaintracks.BlockHeader
	var orphaned []OrphanedBlock
	current := tip

	for {
		blockHash := current.Hash.String()

		// Check if this block is already on our canonical chain
		onChain, err := a.store.IsBlockOnChain(ctx, blockHash)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check block %s: %w", blockHash, err)
		}

		if onChain {
			// Found a block on our canonical chain - we're connected!
			break
		}

		// Check for reorg: is there a DIFFERENT block at this height that's on_chain?
		existingHash, found, err := a.store.GetOnChainBlockAtHeight(ctx, uint64(current.Height))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check height %d: %w", current.Height, err)
		}
		if found && existingHash != blockHash {
			// Reorg detected! The existing block at this height is now orphaned
			orphaned = append(orphaned, OrphanedBlock{
				Hash:   existingHash,
				Height: uint64(current.Height),
			})
		}

		unprocessed = append(unprocessed, current)

		// Don't go past genesis
		if current.Height == 0 {
			break
		}

		// Get parent block from chaintracks
		parent, err := a.chaintracks.GetHeaderByHash(ctx, &current.PrevHash)
		if err != nil {
			// Can't walk back further (might be at our sync start point)
			a.logger.Debug("can't get parent block, stopping walk-back",
				slog.String("hash", current.PrevHash.String()),
				slog.String("error", err.Error()))
			break
		}
		current = parent
	}

	return unprocessed, orphaned, nil
}

// handleOrphanedBlock resets transaction statuses when a block is reorged out
func (a *Arcade) handleOrphanedBlock(ctx context.Context, orphan OrphanedBlock) error {
	a.logger.Info("handling orphaned block (reorg)",
		slog.String("hash", orphan.Hash),
		slog.Uint64("height", orphan.Height))

	// Mark block as off-chain
	if err := a.store.MarkBlockOffChain(ctx, orphan.Hash); err != nil {
		return fmt.Errorf("failed to mark block off-chain: %w", err)
	}

	// Clean up STUMPs for orphaned block (Merkle Service integration)
	if err := a.store.DeleteStumpsByBlockHash(ctx, orphan.Hash); err != nil {
		a.logger.Warn("failed to delete STUMPs for orphaned block",
			slog.String("blockHash", orphan.Hash),
			slog.String("error", err.Error()))
	}

	// Reset transaction statuses to SEEN_ON_NETWORK
	txids, err := a.store.SetStatusByBlockHash(ctx, orphan.Hash, models.StatusSeenOnNetwork)
	if err != nil {
		return fmt.Errorf("failed to reset transaction statuses: %w", err)
	}

	// Publish status change events
	for _, txid := range txids {
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now(),
			ExtraInfo: fmt.Sprintf("reorg: block %s orphaned", orphan.Hash),
		}
		_ = a.eventPublisher.Publish(ctx, status)
		hash, _ := chainhash.NewHashFromHex(txid)
		if hash != nil {
			a.txTracker.UpdateStatusHash(*hash, models.StatusSeenOnNetwork)
		}
	}

	if len(txids) > 0 {
		a.logger.Info("reset transactions due to reorg",
			slog.String("blockHash", orphan.Hash),
			slog.Int("count", len(txids)))
	}

	return nil
}

// processBlockByHeader processes a block from tip catch-up
func (a *Arcade) processBlockByHeader(ctx context.Context, header *chaintracks.BlockHeader) error {
	blockHash := header.Hash.String()

	a.logger.Debug("processing block from tip catch-up",
		slog.String("hash", blockHash),
		slog.Uint64("height", uint64(header.Height)))

	// Mark block as on_chain
	if err := a.store.MarkBlockProcessed(ctx, blockHash, uint64(header.Height), true); err != nil {
		return fmt.Errorf("failed to mark block as on_chain: %w", err)
	}

	a.logger.Debug("marked block as on_chain",
		slog.String("hash", blockHash),
		slog.Uint64("height", uint64(header.Height)))

	return nil
}

// handleRejectedTxMessages processes rejected transaction messages
func (a *Arcade) handleRejectedTxMessages(ctx context.Context, rejectedTxChan <-chan teranode.RejectedTxMessage) {
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

func (a *Arcade) processRejectedTxMessage(ctx context.Context, rejectedMsg teranode.RejectedTxMessage) {
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

	if err := a.store.UpdateStatus(ctx, status); err != nil {
		a.logger.Error("failed to update rejected status",
			slog.String("txID", rejectedMsg.TxID),
			slog.String("error", err.Error()))
		return
	}

	if err := a.eventPublisher.Publish(ctx, status); err != nil {
		a.logger.Error("failed to publish status",
			slog.String("txID", rejectedMsg.TxID),
			slog.String("error", err.Error()))
	}
}

// HTTP fetching methods

// blockJSONResponse represents the JSON response from the Teranode block JSON endpoint.
type blockJSONResponse struct {
	CoinbaseBump string   `json:"coinbase_bump"` // hex-encoded BRC-74
	Subtrees     []string `json:"subtrees"`      // hex-encoded subtree root hashes
	Height       int      `json:"height"`
}

// fetchBlockJSON fetches block data from the Teranode JSON endpoint.
func (a *Arcade) fetchBlockJSON(ctx context.Context, dataHubURL, blockHash string) (*blockJSONResponse, error) {
	url := fmt.Sprintf("%s/block/%s/json", strings.TrimSuffix(dataHubURL, "/"), blockHash)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %d", errUnexpectedStatusCode, resp.StatusCode)
	}

	var result blockJSONResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode JSON response: %w", err)
	}

	return &result, nil
}

// fetchBlockBinarySubtrees fetches subtree hashes from the Teranode binary block endpoint.
func (a *Arcade) fetchBlockBinarySubtrees(ctx context.Context, dataHubURL, blockHash string) ([]chainhash.Hash, error) {
	url := fmt.Sprintf("%s/block/%s", strings.TrimSuffix(dataHubURL, "/"), blockHash)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %d", errUnexpectedStatusCode, resp.StatusCode)
	}

	// Skip block header (80 bytes)
	header := make([]byte, 80)
	if _, readErr := io.ReadFull(resp.Body, header); readErr != nil {
		return nil, fmt.Errorf("failed to read block header: %w", readErr)
	}

	// Read transaction count (varint)
	var txCount util.VarInt
	if _, countErr := txCount.ReadFrom(resp.Body); countErr != nil {
		return nil, fmt.Errorf("failed to read transaction count: %w", countErr)
	}

	// Read size in bytes (varint)
	var sizeBytes util.VarInt
	if _, sizeErr := sizeBytes.ReadFrom(resp.Body); sizeErr != nil {
		return nil, fmt.Errorf("failed to read size in bytes: %w", sizeErr)
	}

	// Read subtree count (varint)
	var subtreeCount util.VarInt
	if _, err := subtreeCount.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("failed to read subtree count: %w", err)
	}

	// Read subtree hashes
	hashes := make([]chainhash.Hash, 0, uint64(subtreeCount))
	hashBuf := make([]byte, 32)

	for i := uint64(0); i < uint64(subtreeCount); i++ {
		if _, err := io.ReadFull(resp.Body, hashBuf); err != nil {
			return nil, fmt.Errorf("failed to read subtree hash %d: %w", i, err)
		}
		hash, err := chainhash.NewHash(hashBuf)
		if err != nil {
			return nil, fmt.Errorf("failed to create hash: %w", err)
		}
		hashes = append(hashes, *hash)
	}

	return hashes, nil
}
