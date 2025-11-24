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

	"github.com/bitcoin-sv/arcade/events"
	"github.com/bitcoin-sv/arcade/models"
	"github.com/bitcoin-sv/arcade/store"
	p2p "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/libp2p/go-libp2p/core/crypto"
)

// Subscriber listens to teranode gossip topics
type Subscriber struct {
	ctx            context.Context
	cancel         context.CancelFunc
	client         p2p.Client
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
func NewSubscriber(
	ctx context.Context,
	config *Config,
	statusStore store.StatusStore,
	networkStore store.NetworkStateStore,
	eventPublisher events.Publisher,
	logger *slog.Logger,
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
		httpClient:     &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// Start starts listening to gossip topics
func (s *Subscriber) Start(ctx context.Context) error {
	var privKey crypto.PrivKey
	var err error

	if s.config.PrivateKey != "" {
		privKey, err = p2p.PrivateKeyFromHex(s.config.PrivateKey)
		if err != nil {
			return fmt.Errorf("failed to decode private key: %w", err)
		}
	} else {
		privKey, err = p2p.GeneratePrivateKey()
		if err != nil {
			return fmt.Errorf("failed to generate private key: %w", err)
		}
		keyHex, _ := p2p.PrivateKeyToHex(privKey)
		s.logger.Info("Generated new private key", slog.String("key", keyHex))
	}

	clientConfig := p2p.Config{
		Name:          s.config.ProcessName,
		Logger:        NewSlogAdapter(s.logger),
		PrivateKey:    privKey,
		Port:          s.config.Port,
		PeerCacheFile: s.config.PeerCacheFile,
	}

	if len(s.config.BootstrapPeers) > 0 {
		clientConfig.BootstrapPeers = s.config.BootstrapPeers
	}

	client, err := p2p.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create P2P client: %w", err)
	}

	s.client = client

	blockTopic := fmt.Sprintf("teranode/bitcoin/1.0.0/%s-block", s.config.TopicPrefix)
	subtreeTopic := fmt.Sprintf("teranode/bitcoin/1.0.0/%s-subtree", s.config.TopicPrefix)
	rejectedTxTopic := fmt.Sprintf("teranode/bitcoin/1.0.0/%s-rejected-tx", s.config.TopicPrefix)
	nodeStatusTopic := fmt.Sprintf("teranode/bitcoin/1.0.0/%s-node_status", s.config.TopicPrefix)

	s.logger.Info("Subscribing to topics",
		slog.String("block", blockTopic),
		slog.String("subtree", subtreeTopic),
		slog.String("rejectedTx", rejectedTxTopic),
		slog.String("nodeStatus", nodeStatusTopic))

	blockMsgChan := s.client.Subscribe(blockTopic)
	subtreeMsgChan := s.client.Subscribe(subtreeTopic)
	rejectedTxMsgChan := s.client.Subscribe(rejectedTxTopic)
	nodeStatusMsgChan := s.client.Subscribe(nodeStatusTopic)

	go s.forwardMessages(blockMsgChan, s.handleBlockMessage, "block")
	go s.forwardMessages(subtreeMsgChan, s.handleSubtreeMessage, "subtree")
	go s.forwardMessages(rejectedTxMsgChan, s.handleRejectedTxMessage, "rejected-tx")
	go s.forwardMessages(nodeStatusMsgChan, s.handleNodeStatusMessage, "node_status")

	go s.checkForOrphanBlocks(ctx)

	s.logger.Info("P2P Subscriber started",
		slog.String("peerID", s.client.GetID()),
		slog.String("blockTopic", blockTopic),
		slog.String("subtreeTopic", subtreeTopic),
		slog.String("rejectedTxTopic", rejectedTxTopic),
		slog.String("nodeStatusTopic", nodeStatusTopic))

	return nil
}

// forwardMessages forwards messages from the p2p channel to the handler function
func (s *Subscriber) forwardMessages(msgChan <-chan p2p.Message, handler func(context.Context, []byte, string), topic string) {
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Message forwarding stopped", slog.String("topic", topic))
			return
		case msg, ok := <-msgChan:
			if !ok {
				s.logger.Warn("Topic channel closed", slog.String("topic", topic))
				return
			}
			s.logger.Debug("Received message",
				slog.String("topic", topic),
				slog.String("from", msg.From),
				slog.String("fromID", msg.FromID),
				slog.Int("size", len(msg.Data)))
			handler(s.ctx, msg.Data, msg.FromID)
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

	if s.client != nil {
		return s.client.Close()
	}

	return nil
}

func (s *Subscriber) handleBlockMessage(ctx context.Context, msg []byte, from string) {
	var blockMsg BlockMessage

	if err := json.Unmarshal(msg, &blockMsg); err != nil {
		s.logger.Error("failed to unmarshal block message",
			slog.String("error", err.Error()),
			slog.String("from", from))
		return
	}

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

func (s *Subscriber) handleSubtreeMessage(ctx context.Context, msg []byte, from string) {
	var subtreeMsg SubtreeMessage

	if err := json.Unmarshal(msg, &subtreeMsg); err != nil {
		s.logger.Error("failed to unmarshal subtree message",
			slog.String("error", err.Error()),
			slog.String("from", from))
		return
	}

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

func (s *Subscriber) handleRejectedTxMessage(ctx context.Context, msg []byte, from string) {
	var rejectedTxMsg RejectedTxMessage

	if err := json.Unmarshal(msg, &rejectedTxMsg); err != nil {
		s.logger.Error("failed to unmarshal rejected-tx message",
			slog.String("error", err.Error()),
			slog.String("from", from))
		return
	}

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

func (s *Subscriber) handleNodeStatusMessage(ctx context.Context, msg []byte, from string) {
	var nodeStatusMsg map[string]interface{}

	if err := json.Unmarshal(msg, &nodeStatusMsg); err != nil {
		s.logger.Error("failed to unmarshal node_status message",
			slog.String("error", err.Error()),
			slog.String("from", from))
		return
	}

	s.logger.Debug("received node_status message",
		slog.String("from", from),
		slog.Any("status", nodeStatusMsg))
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

// BlockMessage represents a block gossip message
type BlockMessage struct {
	Hash       string `json:"Hash"`
	Height     int    `json:"Height"`
	DataHubURL string `json:"DataHubURL"`
	PeerID     string `json:"PeerID"`
}

// SubtreeMessage represents a subtree gossip message
type SubtreeMessage struct {
	Hash       string `json:"Hash"`
	DataHubURL string `json:"DataHubURL"`
	PeerID     string `json:"PeerID"`
}

// RejectedTxMessage represents a rejected transaction gossip message
type RejectedTxMessage struct {
	TxID   string `json:"TxID"`
	Reason string `json:"Reason"`
	PeerID string `json:"PeerID"`
}
