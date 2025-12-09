package p2p

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/libp2p/go-libp2p/core/crypto"
)

// mapNetwork converts user-friendly network names to P2P topic network names.
func mapNetwork(network string) string {
	switch strings.ToLower(network) {
	case "main":
		return NetworkMainnet
	case "test":
		return NetworkTestnet
	case "stn":
		return NetworkSTN
	case "teratestnet":
		return NetworkTeratestnet
	default:
		return network
	}
}

// Client provides a high-level interface for subscribing to Teranode P2P messages.
type Client struct {
	msgbus  msgbus.Client
	logger  *slog.Logger
	network string // P2P network name (e.g., "mainnet", "testnet")
}

// Config holds configuration for creating a new Client.
type Config struct {
	// Name identifies this client on the P2P network
	Name string
	// StoragePath is the directory for storing persistent data (p2p_key.hex, peer_cache.json)
	StoragePath string
	// Network is the Teranode network to connect to (e.g., "mainnet", "testnet", "stn")
	Network string
	// BootstrapPeers overrides the default bootstrap peers for the network
	BootstrapPeers []string
	// Logger for debug/info output (optional)
	Logger *slog.Logger
	// Port to listen on (0 for random)
	Port int
}

// NewClient creates a new Teranode P2P client.
func NewClient(cfg Config) (*Client, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Load or generate persistent private key
	privKey, err := LoadOrGeneratePrivateKey(cfg.StoragePath)
	if err != nil {
		return nil, err
	}

	// Determine bootstrap peers
	bootstrapPeers := cfg.BootstrapPeers
	if len(bootstrapPeers) == 0 {
		bootstrapPeers = BootstrapPeers(cfg.Network)
	}

	// Create underlying P2P client
	p2pClient, err := msgbus.NewClient(msgbus.Config{
		Name:           cfg.Name,
		PrivateKey:     privKey,
		Port:           cfg.Port,
		PeerCacheFile:  cfg.StoragePath + "/peer_cache.json",
		BootstrapPeers: bootstrapPeers,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		msgbus:  p2pClient,
		logger:  logger,
		network: mapNetwork(cfg.Network),
	}, nil
}

// NewClientWithKey creates a new client with an existing private key.
func NewClientWithKey(cfg Config, privKey crypto.PrivKey) (*Client, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	bootstrapPeers := cfg.BootstrapPeers
	if len(bootstrapPeers) == 0 {
		bootstrapPeers = BootstrapPeers(cfg.Network)
	}

	p2pClient, err := msgbus.NewClient(msgbus.Config{
		Name:           cfg.Name,
		PrivateKey:     privKey,
		Port:           cfg.Port,
		PeerCacheFile:  cfg.StoragePath + "/peer_cache.json",
		BootstrapPeers: bootstrapPeers,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		msgbus:  p2pClient,
		logger:  logger,
		network: mapNetwork(cfg.Network),
	}, nil
}

// GetID returns this client's peer ID.
func (c *Client) GetID() string {
	return c.msgbus.GetID()
}

// Close shuts down the P2P client.
func (c *Client) Close() error {
	return c.msgbus.Close()
}

// PeerInfo contains information about a connected peer.
type PeerInfo struct {
	ID    string
	Name  string
	Addrs []string
}

// GetPeers returns information about all known peers.
func (c *Client) GetPeers() []PeerInfo {
	p2pPeers := c.msgbus.GetPeers()
	peers := make([]PeerInfo, len(p2pPeers))
	for i, p := range p2pPeers {
		peers[i] = PeerInfo{
			ID:    p.ID,
			Name:  p.Name,
			Addrs: p.Addrs,
		}
	}
	return peers
}

// SubscribeBlocks subscribes to block announcements.
func (c *Client) SubscribeBlocks(ctx context.Context) <-chan BlockMessage {
	topic := TopicName(c.network, TopicBlock)
	out := make(chan BlockMessage, 100)

	rawChan := c.msgbus.Subscribe(topic)
	go c.forwardMessages(ctx, rawChan, out, topic)

	return out
}

// SubscribeSubtrees subscribes to subtree (transaction batch) announcements.
func (c *Client) SubscribeSubtrees(ctx context.Context) <-chan SubtreeMessage {
	topic := TopicName(c.network, TopicSubtree)
	out := make(chan SubtreeMessage, 100)

	rawChan := c.msgbus.Subscribe(topic)
	go c.forwardMessages(ctx, rawChan, out, topic)

	return out
}

// SubscribeRejectedTxs subscribes to rejected transaction notifications.
func (c *Client) SubscribeRejectedTxs(ctx context.Context) <-chan RejectedTxMessage {
	topic := TopicName(c.network, TopicRejectedTx)
	out := make(chan RejectedTxMessage, 100)

	rawChan := c.msgbus.Subscribe(topic)
	go c.forwardMessages(ctx, rawChan, out, topic)

	return out
}

// SubscribeNodeStatus subscribes to node status updates.
func (c *Client) SubscribeNodeStatus(ctx context.Context) <-chan NodeStatusMessage {
	topic := TopicName(c.network, TopicNodeStatus)
	out := make(chan NodeStatusMessage, 100)

	rawChan := c.msgbus.Subscribe(topic)
	go c.forwardMessages(ctx, rawChan, out, topic)

	return out
}

// forwardMessages reads from raw P2P channel, unmarshals, and forwards to typed channel.
func forwardMessages[T any](ctx context.Context, rawChan <-chan msgbus.Message, out chan<- T, topic string, logger *slog.Logger) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-rawChan:
			if !ok {
				return
			}

			var typed T
			if err := json.Unmarshal(msg.Data, &typed); err != nil {
				logger.Error("failed to unmarshal message",
					slog.String("topic", topic),
					slog.String("error", err.Error()))
				continue
			}

			select {
			case out <- typed:
			case <-ctx.Done():
				return
			}
		}
	}
}

// forwardMessages is a method wrapper for the generic function.
func (c *Client) forwardMessages(ctx context.Context, rawChan <-chan msgbus.Message, out any, topic string) {
	switch ch := out.(type) {
	case chan BlockMessage:
		forwardMessages(ctx, rawChan, ch, topic, c.logger)
	case chan SubtreeMessage:
		forwardMessages(ctx, rawChan, ch, topic, c.logger)
	case chan RejectedTxMessage:
		forwardMessages(ctx, rawChan, ch, topic, c.logger)
	case chan NodeStatusMessage:
		forwardMessages(ctx, rawChan, ch, topic, c.logger)
	}
}
