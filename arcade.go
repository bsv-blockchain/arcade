// Package arcade provides transaction broadcast and status tracking for BSV.
package arcade

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	p2p "github.com/bsv-blockchain/go-teranode-p2p-client"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"
)

// Config holds configuration for Arcade
type Config struct {
	// P2PClient for network communication (required)
	P2PClient *p2p.Client

	// ChainTracker for merkle root validation (required)
	ChainTracker chaintracker.ChainTracker

	// Logger for structured logging
	Logger *slog.Logger

	// Transaction tracking stores (required)
	TxTracker       *store.TxTracker
	StatusStore     store.StatusStore
	SubmissionStore store.SubmissionStore
	EventPublisher  events.Publisher
}

// Arcade tracks transaction statuses via P2P network messages.
type Arcade struct {
	p2pClient    *p2p.Client
	chainTracker chaintracker.ChainTracker
	logger       *slog.Logger
	httpClient   *http.Client

	// Transaction tracking
	txTracker       *store.TxTracker
	statusStore     store.StatusStore
	submissionStore store.SubmissionStore
	eventPublisher  events.Publisher

	// Status subscribers (fan-out)
	subMu         sync.RWMutex
	statusSubs    []*statusSubscriber
	statusSubDone chan struct{}
}

// statusSubscriber holds a status channel, context, and optional token filter
type statusSubscriber struct {
	ch    chan *models.TransactionStatus
	ctx   context.Context
	token string // empty means all updates
}

// NewArcade creates a new Arcade instance.
func NewArcade(cfg Config) (*Arcade, error) {
	if cfg.P2PClient == nil {
		return nil, fmt.Errorf("P2PClient is required")
	}
	if cfg.ChainTracker == nil {
		return nil, fmt.Errorf("ChainTracker is required")
	}
	if cfg.TxTracker == nil {
		return nil, fmt.Errorf("TxTracker is required")
	}
	if cfg.StatusStore == nil {
		return nil, fmt.Errorf("StatusStore is required")
	}
	if cfg.EventPublisher == nil {
		return nil, fmt.Errorf("EventPublisher is required")
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Arcade{
		p2pClient:       cfg.P2PClient,
		chainTracker:    cfg.ChainTracker,
		logger:          cfg.Logger,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
		txTracker:       cfg.TxTracker,
		statusStore:     cfg.StatusStore,
		submissionStore: cfg.SubmissionStore,
		eventPublisher:  cfg.EventPublisher,
	}, nil
}

// Start begins listening for P2P messages
func (a *Arcade) Start(ctx context.Context) error {
	a.logger.Info("Starting Arcade P2P subscriptions")

	// Subscribe to block messages for merkle proof extraction
	blockChan := a.p2pClient.SubscribeBlocks(ctx)
	go a.handleBlockMessages(ctx, blockChan)

	// Subscribe to subtree messages for SEEN_ON_NETWORK status
	subtreeChan := a.p2pClient.SubscribeSubtrees(ctx)
	go a.handleSubtreeMessages(ctx, subtreeChan)

	// Subscribe to rejected-tx messages
	rejectedTxChan := a.p2pClient.SubscribeRejectedTxs(ctx)
	go a.handleRejectedTxMessages(ctx, rejectedTxChan)

	// Forward status updates from EventPublisher to status subscribers
	a.statusSubDone = make(chan struct{})
	go a.forwardStatusUpdates(ctx)

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
			a.notifyStatusSubscribers(status)
		}
	}
}

func (a *Arcade) notifyStatusSubscribers(status *models.TransactionStatus) {
	a.subMu.RLock()
	subs := a.statusSubs
	a.subMu.RUnlock()

	for _, sub := range subs {
		if sub.token != "" && !a.txBelongsToToken(status.TxID, sub.token) {
			continue
		}
		select {
		case sub.ch <- status:
		default:
		}
	}
}

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
	if a.txTracker.Count() == 0 {
		return nil
	}

	a.logger.Info("received block message",
		slog.String("hash", blockMsg.Hash),
		slog.Uint64("height", uint64(blockMsg.Height)))

	// Process transactions to extract merkle proofs
	if err := a.processBlockTransactions(ctx, blockMsg); err != nil {
		return err
	}

	// Set MINED status for transactions with merkle proofs in this block
	statuses, err := a.statusStore.SetMinedByBlockHash(ctx, blockMsg.Hash)
	if err != nil {
		a.logger.Error("failed to set mined status",
			slog.String("blockHash", blockMsg.Hash),
			slog.String("error", err.Error()))
	} else {
		for _, status := range statuses {
			a.eventPublisher.Publish(ctx, status)
		}
		if len(statuses) > 0 {
			a.logger.Info("set transactions to MINED",
				slog.String("blockHash", blockMsg.Hash),
				slog.Int("count", len(statuses)))
		}
	}

	// Prune deeply confirmed transactions
	a.pruneConfirmedTransactions(ctx, blockMsg.Height)

	return nil
}

func (a *Arcade) processBlockTransactions(ctx context.Context, blockMsg teranode.BlockMessage) error {
	subtreeHashes, err := a.fetchBlockSubtreeHashes(ctx, blockMsg.DataHubURL, blockMsg.Hash)
	if err != nil {
		return fmt.Errorf("failed to fetch subtree hashes: %w", err)
	}

	numSubtrees := len(subtreeHashes)
	if numSubtrees == 0 {
		return nil
	}

	a.logger.Debug("processing block transactions",
		slog.String("hash", blockMsg.Hash),
		slog.Int("subtrees", numSubtrees))

	subtreeRootLayer := int(math.Ceil(math.Log2(float64(numSubtrees))))

	for subtreeIdx, subtreeHash := range subtreeHashes {
		txHashes, err := a.fetchSubtreeHashes(ctx, blockMsg.DataHubURL, subtreeHash.String())
		if err != nil {
			a.logger.Error("failed to fetch subtree txids",
				slog.String("subtreeHash", subtreeHash.String()),
				slog.String("error", err.Error()))
			continue
		}

		tracked := a.txTracker.FilterTrackedHashes(txHashes)
		if len(tracked) == 0 {
			continue
		}

		a.buildMerklePathsForSubtree(ctx, blockMsg, subtreeIdx, subtreeRootLayer, subtreeHashes, txHashes, tracked)
	}

	return nil
}

func (a *Arcade) buildMerklePathsForSubtree(
	ctx context.Context,
	blockMsg teranode.BlockMessage,
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
		var txOffset uint64
		for i, h := range txHashes {
			if h == trackedHash {
				txOffset = uint64(i)
				break
			}
		}

		mp := &transaction.MerklePath{
			BlockHeight: blockMsg.Height,
			Path:        make([][]*transaction.PathElement, totalHeight),
		}

		for i, h := range txHashes {
			hashCopy := h
			isTxid := true
			mp.AddLeaf(0, &transaction.PathElement{
				Offset: uint64(i),
				Hash:   &hashCopy,
				Txid:   &isTxid,
			})
		}

		if subtreeSize%2 == 1 {
			dup := true
			mp.AddLeaf(0, &transaction.PathElement{
				Offset:    uint64(subtreeSize),
				Duplicate: &dup,
			})
		}

		subtreeBaseOffset := uint64(subtreeIdx) << uint(internalHeight-1)
		for i, subHash := range subtreeHashes {
			hashCopy := subHash
			mp.AddLeaf(internalHeight, &transaction.PathElement{
				Offset: subtreeBaseOffset + uint64(i),
				Hash:   &hashCopy,
			})
		}

		mp.ComputeMissingHashes()
		minimalPath := a.extractMinimalPath(mp, txOffset)

		if err := a.statusStore.InsertMerklePath(ctx, trackedHash.String(), blockMsg.Hash, uint64(blockMsg.Height), minimalPath.Bytes()); err != nil {
			a.logger.Error("failed to store merkle path",
				slog.String("txID", trackedHash.String()),
				slog.String("blockHash", blockMsg.Hash),
				slog.String("error", err.Error()))
		}
	}
}

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

// handleSubtreeMessages processes subtree announcements
func (a *Arcade) handleSubtreeMessages(ctx context.Context, subtreeChan <-chan teranode.SubtreeMessage) {
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

func (a *Arcade) processSubtreeMessage(ctx context.Context, subtreeMsg teranode.SubtreeMessage) {
	if a.txTracker.Count() == 0 {
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

	if err := a.statusStore.UpdateStatus(ctx, status); err != nil {
		a.logger.Error("failed to update rejected status",
			slog.String("txID", rejectedMsg.TxID),
			slog.String("error", err.Error()))
		return
	}

	a.eventPublisher.Publish(ctx, status)
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
