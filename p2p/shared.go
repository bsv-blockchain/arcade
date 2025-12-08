package p2p

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	p2p "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/libp2p/go-libp2p/core/crypto"
)

// SharedP2PManager manages a shared P2P connection for multiple consumers
type SharedP2PManager struct {
	mu sync.RWMutex

	// P2P client and configuration
	client    p2p.Client
	config    *Config
	logger    *slog.Logger
	isStarted bool
	startedAt time.Time

	// Consumer tracking
	consumers map[string]chan p2p.Message
}

// NewSharedP2PManager creates a new shared P2P manager
func NewSharedP2PManager(config *Config, logger *slog.Logger) (*SharedP2PManager, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}

	return &SharedP2PManager{
		config:    config,
		logger:    logger,
		consumers: make(map[string]chan p2p.Message),
	}, nil
}

// Start initializes and starts the shared P2P connection
func (spm *SharedP2PManager) Start(ctx context.Context) error {
	spm.mu.Lock()
	defer spm.mu.Unlock()

	if spm.isStarted {
		return fmt.Errorf("P2P manager already started")
	}

	// Generate or load private key
	var privKey crypto.PrivKey
	var err error

	if spm.config.PrivateKey != "" {
		privKey, err = p2p.PrivateKeyFromHex(spm.config.PrivateKey)
		if err != nil {
			return fmt.Errorf("failed to decode private key: %w", err)
		}
	} else {
		privKey, err = p2p.GeneratePrivateKey()
		if err != nil {
			return fmt.Errorf("failed to generate private key: %w", err)
		}
		keyHex, _ := p2p.PrivateKeyToHex(privKey)
		spm.logger.Info("Generated new private key", slog.String("key", keyHex))
	}

	clientConfig := p2p.Config{
		Name:          spm.config.ProcessName,
		Logger:        NewSlogAdapter(spm.logger),
		PrivateKey:    privKey,
		Port:          spm.config.Port,
		PeerCacheFile: spm.config.PeerCacheFile,
	}

	if len(spm.config.BootstrapPeers) > 0 {
		clientConfig.BootstrapPeers = spm.config.BootstrapPeers
	}

	client, err := p2p.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create P2P client: %w", err)
	}

	spm.client = client
	spm.isStarted = true
	spm.startedAt = time.Now()

	spm.logger.Info("Shared P2P manager started",
		slog.String("peerID", client.GetID()),
		slog.String("processName", spm.config.ProcessName))

	return nil
}

// Stop closes the shared P2P connection
func (spm *SharedP2PManager) Stop() error {
	spm.mu.Lock()
	defer spm.mu.Unlock()

	if !spm.isStarted {
		return nil
	}

	// Close all consumer channels
	for _, ch := range spm.consumers {
		close(ch)
	}
	spm.consumers = make(map[string]chan p2p.Message)

	// Close the P2P client
	if spm.client != nil {
		err := spm.client.Close()
		spm.client = nil
		spm.isStarted = false
		return err
	}

	spm.isStarted = false
	return nil
}

// GetClient returns the shared P2P client
func (spm *SharedP2PManager) GetClient() p2p.Client {
	spm.mu.RLock()
	defer spm.mu.RUnlock()
	return spm.client
}

// Subscribe creates a subscription for a consumer with a unique ID
func (spm *SharedP2PManager) Subscribe(consumerID, topic string) (<-chan p2p.Message, error) {
	spm.mu.Lock()
	defer spm.mu.Unlock()

	if !spm.isStarted {
		return nil, fmt.Errorf("P2P manager not started")
	}

	if spm.client == nil {
		return nil, fmt.Errorf("P2P client not initialized")
	}

	// Create a channel for this consumer
	msgChan := make(chan p2p.Message, 100) // Buffered channel

	// Subscribe to the topic
	subChan := spm.client.Subscribe(topic)

	// Start forwarding messages to consumer channel
	go func() {
		for {
			select {
			case msg, ok := <-subChan:
				if !ok {
					close(msgChan)
					return
				}
				select {
				case msgChan <- msg:
				default:
					// Drop message if consumer channel is full
					spm.logger.Warn("Dropping message - consumer channel full",
						slog.String("consumerID", consumerID),
						slog.String("topic", topic))
				}
			}
		}
	}()

	// Register consumer
	spm.consumers[consumerID] = msgChan

	return msgChan, nil
}

// Unsubscribe removes a consumer from the manager
func (spm *SharedP2PManager) Unsubscribe(consumerID string) {
	spm.mu.Lock()
	defer spm.mu.Unlock()

	if ch, exists := spm.consumers[consumerID]; exists {
		close(ch)
		delete(spm.consumers, consumerID)
	}
}

// GetPeerID returns the peer ID of the shared P2P client
func (spm *SharedP2PManager) GetPeerID() string {
	spm.mu.RLock()
	defer spm.mu.RUnlock()

	if spm.client != nil {
		return spm.client.GetID()
	}
	return ""
}
