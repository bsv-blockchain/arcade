// Package p2p_client subscribes to teranode's node_status topic via
// go-teranode-p2p-client and persists each peer's advertised datahub URL to
// the shared store. It runs only when `p2p.datahub_discovery: true`; when
// disabled, Start blocks on ctx with no libp2p port opened.
//
// This service is the sole libp2p host in a microservice deployment. Other
// pods (propagation, bump-builder, api-server, sse, watchdog) learn about
// discovered peer endpoints by polling the shared DatahubEndpoint registry
// via teranode.Client's refresh loop — they do not run their own libp2p
// stack.
//
// Using the wrapper library means we inherit:
//   - embedded bootstrap peers for main/test/stn (no operator config needed)
//   - canonical topic names (`teranode/bitcoin/1.0.0/{network}-node_status`)
//   - persistent peer identity stored under StoragePath as p2p_key.hex
package p2p_client

import (
	"context"
	"fmt"
	"sync"
	"time"

	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	p2pclient "github.com/bsv-blockchain/go-teranode-p2p-client"
	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/store"
)

// BlockNotification is retained from the earlier scaffold. It is not used by
// the datahub discovery path but is kept so a future block-subscription
// feature can reuse the same service.
type BlockNotification struct {
	BlockHash    string `json:"block_hash"`
	Height       uint64 `json:"height"`
	PreviousHash string `json:"previous_hash"`
	Timestamp    int64  `json:"timestamp"`
}

// teraClient is the subset of *p2pclient.Client the service actually uses.
// Extracted as an interface so tests can feed hand-crafted NodeStatusMessage
// values through a fake without opening a libp2p host.
type teraClient interface {
	SubscribeNodeStatus(ctx context.Context) <-chan teranodep2p.NodeStatusMessage
	GetID() string
	Close() error
}

// EndpointWriter is the narrow store contract p2p_client needs: persist a
// discovered datahub URL so other pods (propagation, bump-builder) can pick
// it up via their teranode.Client refresh loop. Defined as an interface here
// rather than taking *store.Store directly so tests can pass a fake.
type EndpointWriter interface {
	UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error
}

// clientFactory is overridable in tests to avoid starting a real libp2p host.
type clientFactory func(ctx context.Context, cfg p2pclient.Config) (teraClient, error)

func defaultClientFactory(ctx context.Context, cfg p2pclient.Config) (teraClient, error) {
	return cfg.Initialize(ctx, "arcade")
}

type Client struct {
	cfg           *config.Config
	logger        *zap.Logger
	producer      *kafka.Producer
	store         EndpointWriter
	clientFactory clientFactory
	bus           teraClient
	done          chan struct{}
	wg            sync.WaitGroup
	stopOnce      sync.Once
}

func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, st EndpointWriter) *Client {
	return &Client{
		cfg:           cfg,
		logger:        logger.Named("p2p-client"),
		producer:      producer,
		store:         st,
		clientFactory: defaultClientFactory,
		done:          make(chan struct{}),
	}
}

func (c *Client) Name() string { return "p2p-client" }

func (c *Client) Start(ctx context.Context) error {
	if !c.cfg.P2P.DatahubDiscovery {
		c.logger.Info(
			"p2p discovery disabled",
			zap.Bool("datahub_discovery", false),
		)
		// Block until parent shutdown so the service stays in the service
		// pool and its Stop is called on the same path as every other
		// service. Cheap — no goroutines, no sockets.
		<-ctx.Done()
		return nil
	}

	// Derive storage path: explicit p2p.storage_path wins, else nest under
	// the top-level arcade storage_path, else let the library default to
	// ~/.teranode-p2p. The library also persists the libp2p private key
	// here so the peer ID is stable across restarts.
	storagePath := c.cfg.P2P.StoragePath
	if storagePath == "" && c.cfg.StoragePath != "" {
		storagePath = c.cfg.StoragePath + "/p2p"
	}

	// Resolve the canonical network to the upstream topic identifier and its
	// bootstrap peer list. Operator-supplied BootstrapPeers wins so private
	// networks and bootstrap migrations remain possible.
	topicNetwork, defaultBootstrap := config.ResolveP2PNetwork(c.cfg.Network)
	bootstrapPeers := c.cfg.P2P.BootstrapPeers
	if len(bootstrapPeers) == 0 {
		bootstrapPeers = defaultBootstrap
	}

	p2pCfg := p2pclient.Config{
		Network:     topicNetwork,
		StoragePath: storagePath,
		MsgBus: msgbus.Config{
			Logger:          &zapBusLogger{l: c.logger},
			Port:            c.cfg.P2P.ListenPort,
			BootstrapPeers:  bootstrapPeers,
			DHTMode:         c.cfg.P2P.DHTMode,
			EnableMDNS:      c.cfg.P2P.EnableMDNS,
			AllowPrivateIPs: c.cfg.P2P.AllowPrivateURLs,
		},
	}

	b, err := c.clientFactory(ctx, p2pCfg)
	if err != nil {
		return fmt.Errorf("initializing teranode p2p client: %w", err)
	}
	c.bus = b

	msgs := b.SubscribeNodeStatus(ctx)

	c.logger.Info(
		"p2p discovery enabled",
		zap.String("network", c.cfg.Network),
		zap.String("topic_network", p2pCfg.Network),
		zap.String("peer_id", b.GetID()),
		zap.Int("bootstrap_peers", len(bootstrapPeers)),
		zap.String("dht_mode", c.cfg.P2P.DHTMode),
		zap.Int("listen_port", c.cfg.P2P.ListenPort),
	)

	c.wg.Add(1)
	go c.consume(ctx, msgs)

	<-ctx.Done()
	return nil
}

func (c *Client) Stop() error {
	var err error
	c.stopOnce.Do(func() {
		close(c.done)
		// Bound the bus close so a hung libp2p shutdown can't block the
		// whole process for arbitrary time.
		if c.bus != nil {
			closed := make(chan error, 1)
			go func() { closed <- c.bus.Close() }()
			select {
			case err = <-closed:
			case <-time.After(10 * time.Second):
				c.logger.Warn("p2p bus close timed out")
			}
		}
		c.wg.Wait()
	})
	return err
}

func (c *Client) consume(ctx context.Context, msgs <-chan teranodep2p.NodeStatusMessage) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			c.handleNodeStatus(ctx, msg)
		}
	}
}

// handleNodeStatus extracts a datahub URL from a node_status announcement,
// validates it, and persists it to the shared DatahubEndpoint registry. The
// wrapper library has already JSON-decoded the payload; spoofing defense is
// left to the libp2p layer since the wrapper drops FromID before fan-out.
func (c *Client) handleNodeStatus(ctx context.Context, msg teranodep2p.NodeStatusMessage) {
	metrics.P2PNodeStatusMessagesTotal.Inc()

	if ce := c.logger.Check(zap.DebugLevel, "received node_status"); ce != nil {
		fields := []zap.Field{
			zap.String("peer_id", msg.PeerID),
			zap.String("client_name", msg.ClientName),
			zap.String("type", msg.Type),
			zap.String("base_url", msg.BaseURL),
			zap.String("propagation_url", msg.PropagationURL),
			zap.String("version", msg.Version),
			zap.String("commit_hash", msg.CommitHash),
			zap.String("best_block_hash", msg.BestBlockHash),
			zap.Uint32("best_height", msg.BestHeight),
			zap.Uint64("tx_count", msg.TxCount),
			zap.Uint32("subtree_count", msg.SubtreeCount),
			zap.String("fsm_state", msg.FSMState),
			zap.Int64("start_time", msg.StartTime),
			zap.Float64("uptime_seconds", msg.Uptime),
			zap.String("miner_name", msg.MinerName),
			zap.String("listen_mode", msg.ListenMode),
			zap.String("chain_work", msg.ChainWork),
			zap.String("sync_peer_id", msg.SyncPeerID),
			zap.Uint32("sync_peer_height", msg.SyncPeerHeight),
			zap.String("sync_peer_block_hash", msg.SyncPeerBlockHash),
			zap.Int64("sync_connected_at", msg.SyncConnectedAt),
			zap.Int("connected_peers_count", msg.ConnectedPeersCount),
			zap.String("storage", msg.Storage),
		}
		if msg.MinMiningTxFee != nil {
			fields = append(fields, zap.Float64("min_mining_tx_fee", *msg.MinMiningTxFee))
		}
		ce.Write(fields...)
	}

	raw := pickDatahubURL(msg)
	if raw == "" {
		metrics.P2PEndpointDiscoveryTotal.WithLabelValues("no_url").Inc()
		c.logger.Debug(
			"node_status has no datahub url",
			zap.String("peer_id", msg.PeerID),
		)
		return
	}

	normalized, err := validateURL(raw, c.cfg.P2P.AllowPrivateURLs)
	if err != nil {
		metrics.P2PEndpointDiscoveryTotal.WithLabelValues("invalid").Inc()
		c.logger.Warn(
			"rejected discovered datahub url",
			zap.String("peer_id", msg.PeerID),
			zap.String("url", raw),
			zap.Error(err),
		)
		return
	}

	// Persist the discovery to the shared DatahubEndpoint registry. Every
	// consumer pod's teranode.Client polls this registry on its refresh
	// tick (see teranode/client.go:refreshLoop) and merges new URLs into
	// its in-memory broadcast set — that is the single mechanism by which
	// peer URLs reach broadcast paths. Upserts are idempotent: a known URL
	// just advances LastSeen.
	if c.store == nil {
		// Constructor always supplies a writer in production; a nil store
		// only happens in narrow tests of unrelated branches above.
		metrics.P2PEndpointDiscoveryTotal.WithLabelValues("no_store").Inc()
		return
	}
	upsertCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = c.store.UpsertDatahubEndpoint(upsertCtx, store.DatahubEndpoint{
		URL:      normalized,
		Network:  c.cfg.Network,
		Source:   store.DatahubEndpointSourceDiscovered,
		LastSeen: time.Now(),
	})
	cancel()
	if err != nil {
		metrics.P2PEndpointDiscoveryTotal.WithLabelValues("error").Inc()
		c.logger.Warn(
			"failed to persist discovered datahub url",
			zap.String("url", normalized),
			zap.Error(err),
		)
		return
	}
	metrics.P2PEndpointDiscoveryTotal.WithLabelValues("registered").Inc()
	c.logger.Info(
		"registered peer datahub url",
		zap.String("peer_id", msg.PeerID),
		zap.String("url", normalized),
	)
}

// zapBusLogger adapts *zap.Logger to the message-bus logger interface, which
// wants printf-style formatters. Mapping Debugf→Sugar().Debugf keeps the
// structured logger's output coherent with the rest of arcade.
type zapBusLogger struct {
	l *zap.Logger
}

func (z *zapBusLogger) Debugf(format string, v ...any) { z.l.Sugar().Debugf(format, v...) }
func (z *zapBusLogger) Infof(format string, v ...any)  { z.l.Sugar().Infof(format, v...) }
func (z *zapBusLogger) Warnf(format string, v ...any)  { z.l.Sugar().Warnf(format, v...) }
func (z *zapBusLogger) Errorf(format string, v ...any) { z.l.Sugar().Errorf(format, v...) }
