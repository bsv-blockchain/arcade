package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/services"
	"github.com/bsv-blockchain/arcade/services/api_server"
	"github.com/bsv-blockchain/arcade/services/bump_builder"
	"github.com/bsv-blockchain/arcade/services/p2p_client"
	"github.com/bsv-blockchain/arcade/services/propagation"
	"github.com/bsv-blockchain/arcade/services/tx_validator"
	"github.com/bsv-blockchain/arcade/store"
	storefactory "github.com/bsv-blockchain/arcade/store/factory"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "arcade",
		Short: "Arcade transaction management service",
		RunE:  run,
	}

	config.BindFlags(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load(cmd)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	logger := newLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("starting arcade",
		zap.String("mode", cfg.Mode),
		zap.String("kafka_backend", cfg.Kafka.Backend),
		zap.String("store_backend", cfg.Store.Backend),
	)

	broker, err := kafka.NewBroker(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("creating kafka broker: %w", err)
	}
	producer := kafka.NewProducer(broker)
	defer producer.Close()

	// Validate that the hot-path topics have enough partitions for the
	// deployment. min_partitions is operator-supplied — leave unset (0/1) in
	// standalone or single-replica deployments; set to the expected replica
	// count in K8s. Fails fast on misconfigured horizontally-scaled topics so
	// the error surfaces before live traffic arrives.
	if cfg.Kafka.MinPartitions > 1 {
		if err := kafka.CheckPartitions(broker, []string{kafka.TopicTransaction, kafka.TopicPropagation}, cfg.Kafka.MinPartitions, logger); err != nil {
			return fmt.Errorf("kafka partition check: %w", err)
		}
	}

	st, leaser, err := storefactory.New(cfg)
	if err != nil {
		return fmt.Errorf("creating store: %w", err)
	}
	defer st.Close()
	if err := st.EnsureIndexes(); err != nil {
		return fmt.Errorf("ensuring store indexes: %w", err)
	}

	// Create shared components
	txTracker := store.NewTxTracker()

	teranodeClient := teranode.NewClient(cfg.DatahubURLs, cfg.Teranode.AuthToken, teranode.HealthConfig{
		FailureThreshold:    cfg.Propagation.EndpointHealth.FailureThreshold,
		ProbeInterval:       time.Duration(cfg.Propagation.EndpointHealth.ProbeIntervalMs) * time.Millisecond,
		ProbeTimeout:        time.Duration(cfg.Propagation.EndpointHealth.ProbeTimeoutMs) * time.Millisecond,
		MinHealthyEndpoints: cfg.Propagation.EndpointHealth.MinHealthyEndpoints,
		RefreshInterval:     time.Duration(cfg.Propagation.EndpointHealth.RefreshIntervalMs) * time.Millisecond,
		Source:              endpointSource{st: st},
		Logger:              logger,
	})
	defer teranodeClient.Close()

	// Seed the registry with statically configured URLs so a freshly started
	// pod (especially mode=p2p-client which has no static list of its own to
	// broadcast against) always sees them via the same discovery path. The
	// upsert is idempotent and refreshes LastSeen on every restart, so this
	// also works as a heartbeat for the operator-defined seed list.
	if len(cfg.DatahubURLs) > 0 {
		seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
		for _, url := range cfg.DatahubURLs {
			if err := st.UpsertDatahubEndpoint(seedCtx, store.DatahubEndpoint{
				URL:      url,
				Source:   store.DatahubEndpointSourceConfigured,
				LastSeen: time.Now(),
			}); err != nil {
				logger.Warn("failed to seed configured datahub url",
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

	txVal := validator.NewValidator(nil, nil) // Default policy, no chain tracker yet

	svcs := buildServices(cfg, logger, producer, st, leaser, txTracker, teranodeClient, merkleClient, txVal)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Tie the teranode client's background probe to the service lifecycle so
	// unhealthy endpoints are re-probed for recovery and the goroutine exits
	// cleanly on shutdown.
	teranodeClient.Start(ctx)

	// Start health server for non-API modes (api-server serves /health on its own port)
	if cfg.Mode != "api-server" {
		hs := services.NewHealthServer(cfg.Health.Port, logger)
		hs.Start(ctx)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	errCh := make(chan error, len(svcs))

	for _, svc := range svcs {
		wg.Add(1)
		go func(s services.Service) {
			defer wg.Done()
			logger.Info("starting service", zap.String("service", s.Name()))
			if err := s.Start(ctx); err != nil {
				logger.Error("service failed", zap.String("service", s.Name()), zap.Error(err))
				errCh <- fmt.Errorf("service %s: %w", s.Name(), err)
			}
		}(svc)
	}

	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		logger.Error("service error, shutting down", zap.Error(err))
	}

	cancel()

	for _, svc := range svcs {
		logger.Info("stopping service", zap.String("service", svc.Name()))
		if err := svc.Stop(); err != nil {
			logger.Error("error stopping service", zap.String("service", svc.Name()), zap.Error(err))
		}
	}

	wg.Wait()
	logger.Info("arcade stopped")
	return nil
}

func buildServices(
	cfg *config.Config,
	logger *zap.Logger,
	producer *kafka.Producer,
	st store.Store,
	leaser store.Leaser,
	txTracker *store.TxTracker,
	teranodeClient *teranode.Client,
	merkleClient *merkleservice.Client,
	txVal *validator.Validator,
) []services.Service {
	var svcs []services.Service

	shouldRun := func(name string) bool {
		return cfg.Mode == "all" || cfg.Mode == name
	}

	if shouldRun("api-server") {
		svcs = append(svcs, api_server.New(cfg, logger, producer, st, txTracker, teranodeClient))
	}
	if shouldRun("bump-builder") {
		svcs = append(svcs, bump_builder.New(cfg, logger, producer, st, teranodeClient))
	}
	if shouldRun("tx-validator") {
		svcs = append(svcs, tx_validator.New(cfg, logger, producer, st, txTracker, txVal))
	}
	if shouldRun("propagation") {
		svcs = append(svcs, propagation.New(cfg, logger, producer, st, leaser, teranodeClient, merkleClient))
	}
	// p2p_client is its own service; it's needed both by mode=propagation
	// (where it feeds the local teranode.Client directly) and by mode=p2p-client
	// (its own pod in the K8s topology, where it publishes discoveries to the
	// shared store for other pods to pick up via refresh).
	if shouldRun("propagation") || shouldRun("p2p-client") {
		svcs = append(svcs, p2p_client.New(cfg, logger, producer, teranodeClient, st))
	}

	return svcs
}

// endpointSource adapts store.Store to teranode.EndpointSource by extracting
// just the URL list. Defining the adapter here (rather than in teranode) keeps
// the teranode package free of a direct store dependency.
type endpointSource struct {
	st store.Store
}

func (a endpointSource) ListEndpointURLs(ctx context.Context) ([]string, error) {
	eps, err := a.st.ListDatahubEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(eps))
	for _, ep := range eps {
		out = append(out, ep.URL)
	}
	return out, nil
}

func newLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zap.DebugLevel
	case "warn":
		zapLevel = zap.WarnLevel
	case "error":
		zapLevel = zap.ErrorLevel
	default:
		zapLevel = zap.InfoLevel
	}

	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zapLevel),
		Encoding:         "json",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig:    zap.NewProductionEncoderConfig(),
	}

	logger, _ := cfg.Build()
	return logger
}
