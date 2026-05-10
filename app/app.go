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
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/services"
	"github.com/bsv-blockchain/arcade/services/api_server"
	"github.com/bsv-blockchain/arcade/services/bump_builder"
	"github.com/bsv-blockchain/arcade/services/p2p_client"
	"github.com/bsv-blockchain/arcade/services/propagation"
	"github.com/bsv-blockchain/arcade/services/tx_validator"
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

	if cfg.Kafka.MinPartitions > 1 {
		if pErr := kafka.CheckPartitions(broker, []string{kafka.TopicTransaction, kafka.TopicPropagation}, cfg.Kafka.MinPartitions, logger); pErr != nil {
			_ = producer.Close()
			return nil, nil, fmt.Errorf("kafka partition check: %w", pErr)
		}
	}

	st, leaser, err := storefactory.New(ctx, cfg)
	if err != nil {
		_ = producer.Close()
		return nil, nil, fmt.Errorf("creating store: %w", err)
	}
	if err := st.EnsureIndexes(); err != nil {
		_ = st.Close()
		_ = producer.Close()
		return nil, nil, fmt.Errorf("ensuring store indexes: %w", err)
	}

	txTracker := store.NewTxTracker()

	teranodeClient := teranode.NewClient(cfg.DatahubURLs, cfg.Teranode.AuthToken, teranode.HealthConfig{
		FailureThreshold:    cfg.Propagation.EndpointHealth.FailureThreshold,
		ProbeInterval:       time.Duration(cfg.Propagation.EndpointHealth.ProbeIntervalMs) * time.Millisecond,
		ProbeTimeout:        time.Duration(cfg.Propagation.EndpointHealth.ProbeTimeoutMs) * time.Millisecond,
		MinHealthyEndpoints: cfg.Propagation.EndpointHealth.MinHealthyEndpoints,
		RefreshInterval:     time.Duration(cfg.Propagation.EndpointHealth.RefreshIntervalMs) * time.Millisecond,
		Source:              endpointSource{st: st, network: cfg.Network},
		Logger:              logger,
	})

	// Seed the registry with statically configured URLs so a freshly started
	// pod (especially mode=p2p-client) always sees them via the same discovery
	// path. The upsert is idempotent and refreshes LastSeen on every restart,
	// so this also works as a heartbeat for the operator-defined seed list.
	if len(cfg.DatahubURLs) > 0 {
		seedCtx, seedCancel := context.WithTimeout(ctx, 5*time.Second)
		for _, url := range cfg.DatahubURLs {
			if err := st.UpsertDatahubEndpoint(seedCtx, store.DatahubEndpoint{
				URL:      url,
				Network:  cfg.Network,
				Source:   store.DatahubEndpointSourceConfigured,
				LastSeen: time.Now(),
			}); err != nil {
				logger.Warn(
					"failed to seed configured datahub url",
					zap.String("url", url),
					zap.Error(err),
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
	}

	cleanup := func() {
		_ = publisher.Close()
		teranodeClient.Close()
		_ = st.Close()
		_ = producer.Close()
	}
	return deps, cleanup, nil
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
		svcs = append(svcs, api_server.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.TxTracker, d.TeranodeClient))
	}
	if shouldRun("bump-builder") {
		svcs = append(svcs, bump_builder.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.TeranodeClient))
	}
	if shouldRun("tx-validator") {
		svcs = append(svcs, tx_validator.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.TxTracker, d.Validator))
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

// endpointSource adapts store.Store to teranode.EndpointSource by extracting
// just the URL list. network scopes the listing to the configured Bitcoin
// network so a store shared across pods (or reused after a network change)
// never replays peers from a different network.
type endpointSource struct {
	st      store.Store
	network string
}

func (a endpointSource) ListEndpointURLs(ctx context.Context) ([]string, error) {
	eps, err := a.st.ListDatahubEndpoints(ctx, a.network)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(eps))
	for _, ep := range eps {
		out = append(out, ep.URL)
	}
	return out, nil
}
