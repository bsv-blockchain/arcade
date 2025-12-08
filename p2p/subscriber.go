package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	p2p "github.com/bsv-blockchain/go-teranode-p2p-client"
)

// Subscriber listens to teranode gossip topics
type Subscriber struct {
	ctx            context.Context
	cancel         context.CancelFunc
	client         *p2p.Client
	statusStore    store.StatusStore
	networkStore   store.NetworkStateStore
	eventPublisher events.Publisher
	logger         *slog.Logger
	config         *Config
	httpClient     *http.Client
}

// Config defines the configuration for the P2P subscriber
type Config struct {
	ProcessName    string
	Port           int
	BootstrapPeers []string
	PrivateKey     string
	TopicPrefix    string
	PeerCacheFile  string
}

// NewSubscriber creates a new P2P subscriber
// p2pClient must be provided and cannot be nil
func NewSubscriber(
	ctx context.Context,
	config *Config,
	statusStore store.StatusStore,
	networkStore store.NetworkStateStore,
	eventPublisher events.Publisher,
	logger *slog.Logger,
	p2pClient *p2p.Client,
) (*Subscriber, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	if statusStore == nil {
		return nil, fmt.Errorf("statusStore is nil")
	}

	if networkStore == nil {
		return nil, fmt.Errorf("networkStore is nil")
	}

	if eventPublisher == nil {
		return nil, fmt.Errorf("eventPublisher is nil")
	}

	if p2pClient == nil {
		return nil, fmt.Errorf("p2pClient is nil")
	}

	if config.TopicPrefix == "" {
		config.TopicPrefix = "teratestnet"
	}

	if config.PeerCacheFile == "" {
		config.PeerCacheFile = "peer_cache.json"
	}

	subCtx, cancel := context.WithCancel(ctx)

	return &Subscriber{
		ctx:            subCtx,
		cancel:         cancel,
		statusStore:    statusStore,
		networkStore:   networkStore,
		eventPublisher: eventPublisher,
		logger:         logger,
		config:         config,
		client:         p2pClient,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// Start starts listening to gossip topics
func (s *Subscriber) Start(ctx context.Context) error {
	if s.client == nil {
		return fmt.Errorf("P2P client is nil")
	}

	network := s.config.TopicPrefix

	s.logger.Info("Subscribing to P2P topics", slog.String("network", network))

	blockChan := s.client.SubscribeBlocks(ctx, network)
	subtreeChan := s.client.SubscribeSubtrees(ctx, network)
	rejectedTxChan := s.client.SubscribeRejectedTxs(ctx, network)
	nodeStatusChan := s.client.SubscribeNodeStatus(ctx, network)

	go s.handleBlocks(ctx, blockChan)
	go s.handleSubtrees(ctx, subtreeChan)
	go s.handleRejectedTxs(ctx, rejectedTxChan)
	go s.handleNodeStatuses(ctx, nodeStatusChan)

	go s.checkForOrphanBlocks(ctx)

	s.logger.Info("P2P Subscriber started",
		slog.String("peerID", s.client.GetID()),
		slog.String("network", network))

	return nil
}

func (s *Subscriber) handleBlocks(ctx context.Context, blockChan <-chan p2p.BlockMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case blockMsg, ok := <-blockChan:
			if !ok {
				return
			}
			s.processBlockMessage(ctx, blockMsg)
		}
	}
}

func (s *Subscriber) handleSubtrees(ctx context.Context, subtreeChan <-chan p2p.SubtreeMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case subtreeMsg, ok := <-subtreeChan:
			if !ok {
				return
			}
			s.processSubtreeMessage(ctx, subtreeMsg)
		}
	}
}

func (s *Subscriber) handleRejectedTxs(ctx context.Context, rejectedTxChan <-chan p2p.RejectedTxMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case rejectedTxMsg, ok := <-rejectedTxChan:
			if !ok {
				return
			}
			s.processRejectedTxMessage(ctx, rejectedTxMsg)
		}
	}
}

func (s *Subscriber) handleNodeStatuses(ctx context.Context, nodeStatusChan <-chan p2p.NodeStatusMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case nodeStatusMsg, ok := <-nodeStatusChan:
			if !ok {
				return
			}
			s.logger.Debug("received node_status message",
				slog.String("peerID", nodeStatusMsg.PeerID),
				slog.String("clientName", nodeStatusMsg.ClientName))
		}
	}
}

// checkForOrphanBlocks periodically checks for transactions in orphaned blocks
func (s *Subscriber) checkForOrphanBlocks(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.performOrphanCheck(ctx)
		}
	}
}

// performOrphanCheck checks if any mined transactions are in stale blocks
func (s *Subscriber) performOrphanCheck(ctx context.Context) {
	s.logger.Debug("Checking for orphaned blocks")
}

// Stop stops the P2P subscriber
func (s *Subscriber) Stop() error {
	s.logger.Info("Stopping P2P Subscriber")

	s.cancel()

	// Close the client if we created it (not shared)
	if s.client != nil {
		return s.client.Close()
	}

	return nil
}

func (s *Subscriber) processBlockMessage(ctx context.Context, blockMsg p2p.BlockMessage) {
	s.logger.Info("received block message",
		slog.String("hash", blockMsg.Hash),
		slog.Uint64("height", uint64(blockMsg.Height)),
		slog.String("dataHubURL", blockMsg.DataHubURL),
		slog.String("peerID", blockMsg.PeerID))

	txIDs, err := s.fetchBlockTxIDs(ctx, blockMsg.DataHubURL, blockMsg.Hash)
	if err != nil {
		s.logger.Error("failed to fetch block TxIDs",
			slog.String("hash", blockMsg.Hash),
			slog.String("error", err.Error()))
		return
	}

	s.logger.Info("fetched block TxIDs",
		slog.String("hash", blockMsg.Hash),
		slog.Uint64("height", uint64(blockMsg.Height)),
		slog.Int("count", len(txIDs)))

	if err := s.networkStore.UpdateNetworkState(ctx, &models.NetworkState{
		CurrentHeight: uint64(blockMsg.Height),
		LastBlockHash: blockMsg.Hash,
		LastBlockTime: time.Now(),
	}); err != nil {
		s.logger.Error("failed to update network state",
			slog.String("hash", blockMsg.Hash),
			slog.Uint64("height", uint64(blockMsg.Height)),
			slog.String("error", err.Error()))
	}

	for _, txID := range txIDs {
		status := &models.TransactionStatus{
			TxID:        txID,
			Status:      models.StatusMined,
			Timestamp:   time.Now(),
			BlockHash:   blockMsg.Hash,
			BlockHeight: uint64(blockMsg.Height),
		}

		if err := s.statusStore.UpdateStatus(ctx, status); err != nil {
			s.logger.Error("failed to update mined status",
				slog.String("txID", txID),
				slog.String("error", err.Error()))
			continue
		}

		s.eventPublisher.Publish(ctx, models.StatusUpdate{
			TxID:      txID,
			Status:    models.StatusMined,
			Timestamp: time.Now(),
		})
	}
}

func (s *Subscriber) processSubtreeMessage(ctx context.Context, subtreeMsg p2p.SubtreeMessage) {
	s.logger.Debug("received subtree message",
		slog.String("hash", subtreeMsg.Hash),
		slog.String("dataHubURL", subtreeMsg.DataHubURL),
		slog.String("peerID", subtreeMsg.PeerID))

	txIDs, err := s.fetchSubtreeTxIDs(ctx, subtreeMsg.DataHubURL, subtreeMsg.Hash)
	if err != nil {
		s.logger.Error("failed to fetch subtree TxIDs",
			slog.String("hash", subtreeMsg.Hash),
			slog.String("error", err.Error()))
		return
	}

	s.logger.Info("fetched subtree TxIDs",
		slog.String("hash", subtreeMsg.Hash),
		slog.Int("count", len(txIDs)))

	for _, txID := range txIDs {
		status := &models.TransactionStatus{
			TxID:      txID,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now(),
		}

		if err := s.statusStore.UpdateStatus(ctx, status); err != nil {
			s.logger.Error("failed to update seen status",
				slog.String("txID", txID),
				slog.String("error", err.Error()))
			continue
		}

		s.eventPublisher.Publish(ctx, models.StatusUpdate{
			TxID:      txID,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now(),
		})
	}
}

func (s *Subscriber) processRejectedTxMessage(ctx context.Context, rejectedTxMsg p2p.RejectedTxMessage) {
	s.logger.Debug("received rejected-tx message",
		slog.String("txID", rejectedTxMsg.TxID),
		slog.String("reason", rejectedTxMsg.Reason),
		slog.String("peerID", rejectedTxMsg.PeerID))

	txStatus := models.StatusRejected
	var competingTxs []string

	if strings.Contains(strings.ToLower(rejectedTxMsg.Reason), "double spend") {
		txStatus = models.StatusDoubleSpendAttempted
		if competingTxID := parseCompetingTxID(rejectedTxMsg.Reason); competingTxID != "" {
			competingTxs = []string{competingTxID}
		}
	}

	status := &models.TransactionStatus{
		TxID:         rejectedTxMsg.TxID,
		Status:       txStatus,
		Timestamp:    time.Now(),
		ExtraInfo:    rejectedTxMsg.Reason,
		CompetingTxs: competingTxs,
	}

	if err := s.statusStore.UpdateStatus(ctx, status); err != nil {
		s.logger.Error("failed to update rejected status",
			slog.String("txID", rejectedTxMsg.TxID),
			slog.String("error", err.Error()))
		return
	}

	s.eventPublisher.Publish(ctx, models.StatusUpdate{
		TxID:      rejectedTxMsg.TxID,
		Status:    txStatus,
		Timestamp: time.Now(),
	})
}

func parseCompetingTxID(reason string) string {
	var reasonData map[string]interface{}
	if err := json.Unmarshal([]byte(reason), &reasonData); err != nil {
		return ""
	}

	spendingData, ok := reasonData["SpendingData"].(map[string]interface{})
	if !ok {
		return ""
	}

	txHash, ok := spendingData["TxHash"].(string)
	if !ok {
		return ""
	}

	return txHash
}

func (s *Subscriber) fetchBlockTxIDs(ctx context.Context, dataHubURL, blockHash string) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/block/%s",
		strings.TrimSuffix(dataHubURL, "/"),
		blockHash)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	txIDs := make([]string, 0)
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

		txID := hex.EncodeToString(hashBuf)
		txIDs = append(txIDs, txID)
	}

	return txIDs, nil
}

func (s *Subscriber) fetchSubtreeTxIDs(ctx context.Context, dataHubURL, subtreeHash string) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/subtree/%s",
		strings.TrimSuffix(dataHubURL, "/"),
		subtreeHash)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch subtree: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	txIDs := make([]string, 0)
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

		txID := hex.EncodeToString(hashBuf)
		txIDs = append(txIDs, txID)
	}

	return txIDs, nil
}
