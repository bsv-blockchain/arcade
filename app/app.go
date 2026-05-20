// Package app builds and wires the shared dependencies arcade's services
// need (kafka broker + producer, store, teranode client, merkle-service
// client, events publisher, validator) and turns them into a slice of
// runnable services. cmd/arcade/main.go is a thin wrapper that loads
// config, calls Bootstrap, then BuildServices, and supervises the lifecycle.
//
// Splitting this out of cmd/arcade lets the e2e test harness reuse the same
// boot path without invoking the binary or duplicating wiring.
package app

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	chaintrackslib "github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/services"
	"github.com/bsv-blockchain/arcade/services/api_server"
	"github.com/bsv-blockchain/arcade/services/bump_builder"
	"github.com/bsv-blockchain/arcade/services/chaintracks_server"
	"github.com/bsv-blockchain/arcade/services/p2p_client"
	"github.com/bsv-blockchain/arcade/services/propagation"
	"github.com/bsv-blockchain/arcade/services/sse"
	"github.com/bsv-blockchain/arcade/services/watchdog"
	"github.com/bsv-blockchain/arcade/services/webhook"
	"github.com/bsv-blockchain/arcade/store"
	storefactory "github.com/bsv-blockchain/arcade/store/factory"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
)

// Deps is the set of process-wide dependencies arcade's services share.
// Bootstrap returns one of these fully wired; BuildServices consumes it.
type Deps struct {
	Cfg            *config.Config
	Logger         *zap.Logger
	Broker         kafka.Broker
	Producer       *kafka.Producer
	Publisher      events.Publisher
	Store          store.Store
	Leaser         store.Leaser
	TxTracker      *store.TxTracker
	TeranodeClient *teranode.Client
	MerkleClient   *merkleservice.Client // nil when MerkleService.URL is unset
	Validator      *validator.Validator
	// Chaintracks is the shared in-process header tracker. nil when
	// chaintracks_server is disabled (regtest, or explicit opt-out) — services
	// that consume it (chaintracks_server, bump-builder canonical-root
	// validation) must nil-guard.
	Chaintracks chaintrackslib.Chaintracks
}

// Bootstrap creates every shared dependency the services rely on, in the
// order they need to be created. The returned cleanup func closes them in
// reverse order. teranodeClient.Start is wired to ctx, so its background
// probes terminate when ctx is canceled.
func Bootstrap(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*Deps, func(), error) {
	logger.Info(
		"starting arcade",
		zap.String("mode", cfg.Mode),
		zap.String("kafka_backend", cfg.Kafka.Backend),
		zap.String("store_backend", cfg.Store.Backend),
	)

	broker, err := kafka.NewBroker(cfg.Kafka)
	if err != nil {
		return nil, nil, fmt.Errorf("creating kafka broker: %w", err)
	}
	producer := kafka.NewProducer(broker)

	// Hard requirement: TopicPropagation MUST be single-partition. The
	// dep-aware dispatcher's single-goroutine state ownership relies on
	// total order at the topic level — parent/child txids on different
	// partitions would bypass the in-memory dep index and reintroduce
	// the cross-batch "missing inputs" race we just removed. Check this
	// on every startup regardless of MinPartitions.
	if pErr := kafka.CheckExactPartitions(broker, kafka.TopicPropagation, 1, logger); pErr != nil {
		_ = producer.Close()
		return nil, nil, fmt.Errorf("kafka partition check: %w", pErr)
	}
	// MinPartitions is a soft hint for horizontally-scaled topics. There
	// are currently no other hot-path topics that need it (TopicTransaction
	// was retired with tx_validator) but the knob is retained so a future
	// fan-out topic can opt into the check without re-introducing config.
	if cfg.Kafka.MinPartitions > 1 {
		if pErr := kafka.CheckPartitions(broker, nil, cfg.Kafka.MinPartitions, logger); pErr != nil {
			_ = producer.Close()
			return nil, nil, fmt.Errorf("kafka partition check: %w", pErr)
		}
	}

	st, leaser, err := storefactory.New(ctx, cfg)
	if err != nil {
		_ = producer.Close()
		return nil, nil, fmt.Errorf("creating store: %w", err)
	}
	if idxErr := st.EnsureIndexes(); idxErr != nil {
		_ = st.Close()
		_ = producer.Close()
		return nil, nil, fmt.Errorf("ensuring store indexes: %w", idxErr)
	}

	// Align store batch-helper concurrency with config. Zero falls back to
	// runtime.NumCPU which matches validator parallelism — keeps DB write
	// fanout from being the bottleneck.
	store.SetBatchConcurrency(cfg.Store.BatchConcurrency)

	txTracker := store.NewTxTracker()

	teranodeClient := teranode.NewClient(cfg.DatahubURLs, cfg.Teranode.AuthToken, teranode.HealthConfig{
		FailureThreshold:          cfg.Propagation.EndpointHealth.FailureThreshold,
		BroadcastFailureThreshold: cfg.Propagation.EndpointHealth.BroadcastFailureThreshold,
		ProbeInterval:             time.Duration(cfg.Propagation.EndpointHealth.ProbeIntervalMs) * time.Millisecond,
		ProbeTimeout:              time.Duration(cfg.Propagation.EndpointHealth.ProbeTimeoutMs) * time.Millisecond,
		MinHealthyEndpoints:       cfg.Propagation.EndpointHealth.MinHealthyEndpoints,
		RefreshInterval:           time.Duration(cfg.Propagation.EndpointHealth.RefreshIntervalMs) * time.Millisecond,
		Source:                    endpointSource{st: st, network: cfg.Network, includeDiscovered: cfg.P2P.DatahubDiscovery},
		Logger:                    logger,
	})

	// Seed the registry with statically configured URLs so a freshly started
	// pod (especially mode=p2p-client) always sees them via the same discovery
	// path. The upsert is idempotent and refreshes LastSeen on every restart,
	// so this also works as a heartbeat for the operator-defined seed list.
	if len(cfg.DatahubURLs) > 0 {
		seedCtx, seedCancel := context.WithTimeout(ctx, 5*time.Second)
		for _, url := range cfg.DatahubURLs {
			if seedErr := st.UpsertDatahubEndpoint(seedCtx, store.DatahubEndpoint{
				URL:      url,
				Network:  cfg.Network,
				Source:   store.DatahubEndpointSourceConfigured,
				LastSeen: time.Now(),
			}); seedErr != nil {
				logger.Warn(
					"failed to seed configured datahub url",
					zap.String("url", url),
					zap.Error(seedErr),
				)
			}
		}
		seedCancel()
	}

	var merkleClient *merkleservice.Client
	if cfg.MerkleService.URL != "" {
		merkleClient = merkleservice.NewClient(cfg.MerkleService.URL, cfg.MerkleService.AuthToken, 0)
		merkleClient.SetLogger(logger.Named("merkle-client"))
	}

	txVal := validator.NewValidator(nil, nil)

	publisher := events.NewKafkaPublisher(producer, logger, cfg.Events.SubscriberBuffer)

	teranodeClient.Start(ctx)

	// Construct chaintracks once at process startup so every in-process
	// consumer (chaintracks_server, bump-builder canonical-root validation)
	// shares one P2P subscription and header cache. Skipped when:
	//   - cfg.ChaintracksServer.Enabled is false (operator-wide off switch;
	//     regtest force-disables it via config.validate), or
	//   - the configured mode never dereferences deps.Chaintracks. Without
	//     this gate every microservice pod (api-server, sse, propagation, …)
	//     would spin up an embedded ChainManager and poll the upstream node
	//     even though nothing in that pod reads from it. In microservice
	//     deployments the chaintracks pod runs embedded and bump-builder
	//     points chaintracks.mode=remote at it; everything else skips here.
	var chainTracks chaintrackslib.Chaintracks
	if cfg.ChaintracksServer.Enabled && modeNeedsChaintracks(cfg.Mode) {
		ct, ctErr := initChaintracks(ctx, cfg, logger)
		if ctErr != nil {
			_ = st.Close()
			_ = producer.Close()
			return nil, nil, fmt.Errorf("chaintracks init: %w", ctErr)
		}
		chainTracks = ct
	}

	// Hydrate the TxTracker from the store BEFORE handing it to services.
	// Without this, a process restart leaves the in-memory tracker empty
	// and bump-builder's tracked-only filtering silently drops MINED /
	// IMMUTABLE transitions for any tx that was already in-flight at the
	// previous shutdown. Chain height is used to skip deeply-confirmed
	// rows; when chaintracks is disabled we pass 0, which preserves every
	// MINED row in the tracker — safe (loads a bit more) and the operator
	// can prune later.
	var hydrateHeight uint64
	if chainTracks != nil {
		hydrateHeight = uint64(chainTracks.GetHeight(ctx))
	}
	hydrateStart := time.Now()
	loaded, hydrateErr := txTracker.LoadFromStore(ctx, st, hydrateHeight)
	if hydrateErr != nil {
		logger.Warn(
			"tx tracker hydration partial",
			zap.Int("loaded", loaded),
			zap.Uint64("current_height", hydrateHeight),
			zap.Duration("elapsed", time.Since(hydrateStart)),
			zap.Error(hydrateErr),
		)
	} else {
		logger.Info(
			"tx tracker hydrated",
			zap.Int("loaded", loaded),
			zap.Uint64("current_height", hydrateHeight),
			zap.Duration("elapsed", time.Since(hydrateStart)),
		)
	}

	deps := &Deps{
		Cfg:            cfg,
		Logger:         logger,
		Broker:         broker,
		Producer:       producer,
		Publisher:      publisher,
		Store:          st,
		Leaser:         leaser,
		TxTracker:      txTracker,
		TeranodeClient: teranodeClient,
		MerkleClient:   merkleClient,
		Validator:      txVal,
		Chaintracks:    chainTracks,
	}

	cleanup := func() {
		_ = publisher.Close()
		teranodeClient.Close()
		_ = st.Close()
		_ = producer.Close()
	}
	return deps, cleanup, nil
}

// modeNeedsChaintracks reports whether the configured service mode constructs
// at least one service that reads from deps.Chaintracks. Other modes
// (api-server, sse, propagation, p2p-client, watchdog) never dereference it,
// so initializing it would spin up an unused embedded ChainManager + upstream
// header poller in every pod — which is exactly the wrong thing for
// microservice deployments where the dedicated chaintracks pod owns the
// header store and bump-builder reads via chaintracks.mode=remote.
//
// Keep this list in sync with BuildServices: any new mode that wires
// deps.Chaintracks into a service must be added here.
func modeNeedsChaintracks(mode string) bool {
	switch mode {
	case "all", "chaintracks", "bump-builder":
		return true
	default:
		return false
	}
}

// initChaintracks brings up the embedded go-chaintracks instance shared
// across the process. Caller gates the enabled-ness check; this function
// always tries to construct and returns an error on failure.
//
// The construction logic mirrors what previously lived in
// chaintracks_server.Service.initChaintracks. Moving it here lets bump-
// builder use the same instance without depending on a service's
// initialization timing or a duplicate P2P subscription.
func initChaintracks(ctx context.Context, cfg *config.Config, logger *zap.Logger) (chaintrackslib.Chaintracks, error) {
	// Default chaintracks storage to <storage_path>/chaintracks/ so
	// operators only need to set a single storage root. Tilde expansion
	// happens in config.Load.
	if cfg.Chaintracks.StoragePath == "" {
		root := cfg.StoragePath
		if root == "" {
			root = "."
		}
		if err := os.MkdirAll(root, 0o750); err != nil {
			return nil, fmt.Errorf("creating storage directory %s: %w", root, err)
		}
		cfg.Chaintracks.StoragePath = path.Join(root, "chaintracks")
	}

	// Thread the top-level network into chaintracks' embedded p2p config.
	// Without this go-chaintracks falls back to "main" silently. Chaintracks
	// needs the upstream-strict spelling ("main"/"test"/"teratestnet").
	_, defaultBootstrap := config.ResolveP2PNetwork(cfg.Network)
	cfg.Chaintracks.P2P.Network = config.ResolveChaintracksNetwork(cfg.Network)
	if len(cfg.Chaintracks.P2P.MsgBus.BootstrapPeers) == 0 {
		cfg.Chaintracks.P2P.MsgBus.BootstrapPeers = defaultBootstrap
	}

	ct, err := cfg.Chaintracks.Initialize(ctx, "arcade", nil)
	if err != nil {
		return nil, fmt.Errorf("chaintracks initialize: %w", err)
	}

	network, _ := ct.GetNetwork(ctx)
	logger.Info(
		"chaintracks initialized",
		zap.String("storage_path", cfg.Chaintracks.StoragePath),
		zap.String("network", network),
	)
	return ct, nil
}

// BuildServices returns the services that should run for the configured mode.
// Each service's lifetime is tied to the ctx passed to its Start method by
// the caller — the supervisor in cmd/arcade or the test harness.
func BuildServices(d *Deps) []services.Service {
	cfg := d.Cfg
	var svcs []services.Service

	shouldRun := func(name string) bool {
		return cfg.Mode == "all" || cfg.Mode == name
	}

	if shouldRun("api-server") {
		svcs = append(svcs, api_server.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.TxTracker, d.TeranodeClient, d.MerkleClient, d.Validator))
	}
	if shouldRun("bump-builder") {
		// chainHeader is nil when chaintracks is disabled — bump-builder
		// nil-guards and falls back to subtree-count-only validation.
		var chainHeader bump_builder.ChainHeaderReader
		if d.Chaintracks != nil {
			chainHeader = chaintracksHeaderReader{ct: d.Chaintracks}
		}
		svcs = append(svcs, bump_builder.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.TeranodeClient, d.TxTracker, chainHeader))
	}
	if shouldRun("watchdog") && cfg.Watchdog.Enabled {
		if wd := watchdog.NewService(cfg, d.Logger, d.Store, d.Leaser, d.MerkleClient); wd != nil {
			svcs = append(svcs, wd)
		} else {
			d.Logger.Info("watchdog skipped: merkle_service.url or leaser not configured")
		}
	}
	if shouldRun("sse") {
		if ssvc := sse.New(cfg, d.Logger, d.Publisher, d.Store); ssvc != nil {
			svcs = append(svcs, ssvc)
		} else {
			d.Logger.Info("sse skipped: sse.enabled=false or publisher not configured")
		}
	}
	if shouldRun("chaintracks") {
		if ct := chaintracks_server.New(cfg, d.Logger, d.Store, d.Chaintracks); ct != nil {
			svcs = append(svcs, ct)
		} else {
			d.Logger.Info("chaintracks skipped: chaintracks_server.enabled=false (regtest force-disables this)")
		}
	}
	if shouldRun("propagation") {
		svcs = append(svcs, propagation.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.Leaser, d.TeranodeClient, d.MerkleClient))
	}
	if shouldRun("api-server") || shouldRun("webhook") {
		svcs = append(svcs, webhook.New(cfg.Webhook, cfg.Callback, d.Logger, d.Publisher, d.Store))
	}
	if shouldRun("propagation") || shouldRun("p2p-client") {
		svcs = append(svcs, p2p_client.New(cfg, d.Logger, d.Producer, d.TeranodeClient, d.Store))
	}

	return svcs
}

// chaintracksHeaderReader adapts go-chaintracks's Chaintracks interface to
// the narrower ChainHeaderReader contract bump-builder needs. The library
// already exposes GetHeaderByHash with the same signature, so this is a
// trivial passthrough; the wrapper exists only to keep arcade packages from
// importing chaintracks types into their public APIs.
type chaintracksHeaderReader struct {
	ct chaintrackslib.Chaintracks
}

func (a chaintracksHeaderReader) GetHeaderByHash(ctx context.Context, hash *chainhash.Hash) (*chaintrackslib.BlockHeader, error) {
	return a.ct.GetHeaderByHash(ctx, hash)
}

// endpointSource adapts store.Store to teranode.EndpointSource by extracting
// just the URL list. network scopes the listing to the configured Bitcoin
// network so a store shared across pods (or reused after a network change)
// never replays peers from a different network. When includeDiscovered is
// false (operator disabled p2p.datahub_discovery), rows persisted as
// source=discovered by prior runs are filtered out — the toggle now means
// "ignore discovered URLs" end-to-end, not just "stop discovering new ones."
type endpointSource struct {
	st                store.Store
	network           string
	includeDiscovered bool
}

func (a endpointSource) ListEndpointURLs(ctx context.Context) ([]string, error) {
	eps, err := a.st.ListDatahubEndpoints(ctx, a.network)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(eps))
	for _, ep := range eps {
		if !a.includeDiscovered && ep.Source == store.DatahubEndpointSourceDiscovered {
			continue
		}
		out = append(out, ep.URL)
	}
	return out, nil
}
