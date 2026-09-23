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
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	p2pclient "github.com/bsv-blockchain/go-teranode-p2p-client"
	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/ssrfguard"
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
// it up via their teranode.Client refresh loop, and persist a peer's observed
// mining fee so api-server pods can serve the network-minimum fee in GET
// /policy (issue #212). Defined as an interface here rather than taking
// *store.Store directly so tests can pass a fake.
type EndpointWriter interface {
	UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error
	UpsertPeerPolicy(ctx context.Context, pp store.PeerPolicy) error
}

// clientFactory is overridable in tests to avoid starting a real libp2p host.
type clientFactory func(ctx context.Context, cfg p2pclient.Config) (teraClient, error)

func defaultClientFactory(ctx context.Context, cfg p2pclient.Config) (teraClient, error) {
	return cfg.Initialize(ctx, "arcade")
}

// dnsLookupTimeout bounds each DNS resolution performed while validating a
// discovered URL. handleNodeStatus runs on the single consume goroutine, so
// an unbounded lookup against a dead resolver would stall all announcement
// processing.
const dnsLookupTimeout = 2 * time.Second

// discoveryValidationTTL is how long a successfully validated URL skips
// re-validation (including the DNS round-trip). Success-only: rejections are
// never cached, so a peer that fixes its DNS or config is re-accepted on its
// very next announcement. Mirrors merkle-service's announcement validation.
const discoveryValidationTTL = 5 * time.Minute

// discoveryValidationCacheMax bounds the validation cache. Announced URLs
// are attacker-influenced input; the cap keeps a peer spraying unique valid
// URLs from growing process memory without bound. Far above the size of any
// real datahub fleet.
const discoveryValidationCacheMax = 512

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

	// lookupIP resolves a hostname during URL validation. Overridable in
	// tests so validation never hits real DNS; nil is never stored — New
	// installs the bounded default.
	lookupIP func(ctx context.Context, host string) ([]net.IP, error)
	// validatedURLs caches successful validations (raw URL → normalized)
	// so repeat announcements of the same URL skip the DNS round-trip.
	// Bounded — see discoveryValidationCacheMax.
	validatedURLs *ssrfguard.SuccessCache
}

func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, st EndpointWriter) *Client {
	return &Client{
		cfg:           cfg,
		logger:        logger.Named("p2p-client"),
		producer:      producer,
		store:         st,
		clientFactory: defaultClientFactory,
		done:          make(chan struct{}),
		lookupIP: func(ctx context.Context, host string) ([]net.IP, error) {
			lctx, cancel := context.WithTimeout(ctx, dnsLookupTimeout)
			defer cancel()
			return net.DefaultResolver.LookupIP(lctx, "ip", host)
		},
		validatedURLs: ssrfguard.NewSuccessCache(discoveryValidationTTL, discoveryValidationCacheMax),
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

	// Surface the peer's advertised tip height as a gauge instead of
	// discarding it with the debug log below: alongside
	// arcade_chain_tip_height it is the datahub height-lag signal operators
	// alert on (issue #254 — peers chain-stalled 50+ blocks while every
	// reachability probe stayed green). base_url-labelled only, so
	// restart-churned peer ids can't grow the series set.
	if msg.BaseURL != "" {
		metrics.P2PPeerBestHeight.WithLabelValues(msg.BaseURL).Set(float64(msg.BestHeight))
	}

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

	normalized, ok := c.validateDatahubURL(ctx, msg.PeerID, raw)
	if !ok {
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
	err := c.store.UpsertDatahubEndpoint(upsertCtx, store.DatahubEndpoint{
		URL:     normalized,
		Network: c.cfg.Network,
		Source:  store.DatahubEndpointSourceDiscovered,
		// The node's advertised policy travels with its URL so GET /health can
		// report, per endpoint, what that node will accept. nil when the peer
		// advertised none, which leaves any previously recorded policy alone.
		Policy:   endpointPolicyFrom(msg),
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

	// Record the peer's advertised policy only once its URL is in the registry,
	// so the network values GET /policy derives come from exactly the peers
	// arcade can actually broadcast to — the same set GET /health displays.
	//
	// This deliberately reverses the original ordering (issue #212), which ran
	// ahead of the URL gate so a peer advertising a policy but no usable URL
	// still counted. That made the fee floor unexplainable in production: a
	// peer with no URL, or one rejected by SSRF validation (this network really
	// does see nodes announcing cluster-internal names), set the floor for
	// everyone while appearing nowhere in /health. An operator looking at a
	// 0 sat/kB floor could see only peers advertising 100.
	c.recordPeerPolicy(ctx, msg, normalized)
}

// recordPeerPolicy extracts the peer's advertised transaction policy from a
// node_status announcement and persists it to the shared store so api-server
// pods can compute the network-wide values for GET /policy. It prefers the
// structured FeePolicy (satoshis per Bytes, plus the size limits) and falls
// back to the legacy MinMiningTxFee (*float64 in BSV/kB), which carries no size
// limits. Peers advertising neither (older nodes) are skipped.
//
// Called only after the peer's datahub URL has been registered, so every row it
// writes belongs to a peer arcade can broadcast to — see the call site for why
// that matters. datahubURL is that registered URL, and labels the per-peer
// gauges so every input to the network policy is visible under the same URL
// GET /health lists it under.
func (c *Client) recordPeerPolicy(ctx context.Context, msg teranodep2p.NodeStatusMessage, datahubURL string) {
	var sats, byts, maxTxSize, maxScriptSize uint64
	switch {
	case msg.FeePolicy != nil && msg.FeePolicy.MiningFee.Bytes > 0:
		sats = msg.FeePolicy.MiningFee.Satoshis
		byts = msg.FeePolicy.MiningFee.Bytes
		maxTxSize = msg.FeePolicy.MaxTxSizePolicy
		maxScriptSize = msg.FeePolicy.MaxScriptSizePolicy
	case msg.MinMiningTxFee != nil:
		// BSV/kB -> satoshis per 1000 bytes. The conversion rejects an
		// untrusted NaN/Inf/negative/absurd value; see legacyMiningFeeSatPerKB.
		v, ok := legacyMiningFeeSatPerKB(msg.MinMiningTxFee)
		if !ok {
			c.logger.Debug("ignoring implausible peer MinMiningTxFee",
				zap.String("peer_id", msg.PeerID),
				zap.Float64("min_mining_tx_fee", *msg.MinMiningTxFee))
			return
		}
		sats = v
		byts = legacyMiningFeeBytesBasis
	default:
		return
	}

	pp := store.PeerPolicy{
		PeerID:              msg.PeerID,
		Network:             c.cfg.Network,
		MiningFeeSatoshis:   sats,
		MiningFeeBytes:      byts,
		MaxTxSizePolicy:     maxTxSize,
		MaxScriptSizePolicy: maxScriptSize,
		LastSeen:            time.Now(),
	}

	// Zero a size limit too large to store rather than failing the whole write:
	// the fee observation is independently useful and a peer advertising a
	// garbage size must not silently remove itself from the fee minimum. A
	// zeroed limit reads downstream as "peer did not advertise" and is skipped.
	pp, dropped := pp.SanitizePolicySizes()
	if dropped > 0 {
		c.logger.Debug("ignoring implausible peer size policy",
			zap.String("peer_id", msg.PeerID),
			zap.Uint64("max_tx_size_policy", maxTxSize),
			zap.Uint64("max_script_size_policy", maxScriptSize))
	}

	// Label by the registered datahub URL, not msg.BaseURL. The store row this
	// function writes — and therefore the network minimum GET /policy derives
	// from it — is keyed off pickDatahubURL, which prefers PropagationURL. A
	// peer announcing only a propagation URL used to set the fee floor while
	// appearing in no gauge at all, which is exactly why a production floor of
	// 1 sat/kB could not be explained from the outside: every visible series
	// read 100. Using the registered URL also makes these series join to
	// /health's datahub_urls[].url, which is normalized the same way.
	if datahubURL != "" {
		// Normalize to satoshis per 1000 bytes for a comparable gauge.
		metrics.P2PPeerMinMiningFee.WithLabelValues(datahubURL).Set(float64(sats) * 1000 / float64(byts))
		// Only report a size limit the peer actually advertised: a gauge stuck
		// at 0 for a legacy peer would read as "accepts nothing".
		if pp.MaxTxSizePolicy > 0 {
			metrics.P2PPeerMaxTxSizePolicy.WithLabelValues(datahubURL).Set(float64(pp.MaxTxSizePolicy))
		}
		if pp.MaxScriptSizePolicy > 0 {
			metrics.P2PPeerMaxScriptSizePolicy.WithLabelValues(datahubURL).Set(float64(pp.MaxScriptSizePolicy))
		}
	}

	if c.store == nil || msg.PeerID == "" {
		return
	}
	upsertCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.store.UpsertPeerPolicy(upsertCtx, pp); err != nil {
		// An implausible advertisement is the peer's fault, not ours: node_status
		// is unauthenticated gossip and a peer may announce a fee too large to
		// store. Log it as a dropped advertisement rather than a store failure,
		// so a hostile peer cannot fill the operator's warning stream.
		if errors.Is(err, store.ErrInvalidPeerPolicy) {
			c.logger.Debug(
				"ignoring implausible peer policy",
				zap.String("peer_id", msg.PeerID),
				zap.Uint64("mining_fee_satoshis", sats),
				zap.Uint64("mining_fee_bytes", byts),
				zap.Error(err),
			)
			return
		}
		c.logger.Warn(
			"failed to persist peer policy",
			zap.String("peer_id", msg.PeerID),
			zap.Error(err),
		)
	}
}

// validateDatahubURL runs the full discovered-URL validation (scheme, SSRF
// ranges, DNS resolvability — see validateURL) behind a success-only TTL
// cache, so repeat announcements of a known-good URL skip the DNS
// round-trip. Rejections are metered by kind: "blocked" for SSRF-policy
// rejections (private/metadata destinations), "invalid" for everything else
// (malformed, bad scheme, unresolvable hostname).
func (c *Client) validateDatahubURL(ctx context.Context, peerID, raw string) (string, bool) {
	if normalized, ok := c.validatedURLs.Get(raw); ok {
		return normalized, true
	}

	normalized, err := validateURL(raw, c.cfg.P2P.AllowPrivateURLs, func(host string) ([]net.IP, error) {
		return c.lookupIP(ctx, host)
	})
	if err != nil {
		outcome := "invalid"
		if errors.Is(err, ssrfguard.ErrBlockedAddress) {
			outcome = "blocked"
		}
		metrics.P2PEndpointDiscoveryTotal.WithLabelValues(outcome).Inc()
		c.logger.Warn(
			"rejected discovered datahub url",
			zap.String("peer_id", peerID),
			zap.String("url", raw),
			zap.Error(err),
		)
		return "", false
	}

	c.validatedURLs.Put(raw, normalized)
	return normalized, true
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
