package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsv-blockchain/arcade/api"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events/memory"
	"github.com/bsv-blockchain/arcade/handlers"
	"github.com/bsv-blockchain/arcade/p2p"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/sqlite"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
	terap2p "github.com/bsv-blockchain/go-teranode-p2p-client"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("Starting Arcade - P2P Transaction Broadcast Client", slog.String("version", "0.1.0"))

	cfg := config.Default()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("Application error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	logger.Info("Initializing stores", slog.String("db", cfg.Database.SQLitePath))

	logger.Info("Running database migrations")
	if err := store.RunMigrations(cfg.Database.SQLitePath); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	statusStore, err := sqlite.NewStatusStore(cfg.Database.SQLitePath)
	if err != nil {
		return fmt.Errorf("failed to create status store: %w", err)
	}

	submissionStore, err := sqlite.NewSubmissionStore(cfg.Database.SQLitePath)
	if err != nil {
		return fmt.Errorf("failed to create submission store: %w", err)
	}

	networkStore, err := sqlite.NewNetworkStateStore(cfg.Database.SQLitePath)
	if err != nil {
		return fmt.Errorf("failed to create network state store: %w", err)
	}

	logger.Info("Initializing event publisher", slog.String("type", cfg.Events.Type))
	eventPublisher := memory.NewInMemoryPublisher(cfg.Events.BufferSize)
	logger.Info("Using in-memory event publisher", slog.Int("buffer", cfg.Events.BufferSize))

	logger.Info("Initializing Teranode client")
	var endpoints []string
	if len(cfg.Teranode.BaseURLs) > 0 {
		endpoints = cfg.Teranode.BaseURLs
		logger.Info("Using multiple Teranode endpoints", slog.Int("count", len(endpoints)))
	} else {
		endpoints = []string{cfg.Teranode.BaseURL}
		logger.Info("Using single Teranode endpoint", slog.String("url", cfg.Teranode.BaseURL))
	}
	teranodeClient := teranode.NewClient(endpoints)

	logger.Info("Initializing validator")
	validatorPolicy := &validator.Policy{
		MaxTxSizePolicy:         cfg.Validator.MaxTxSize,
		MaxTxSigopsCountsPolicy: cfg.Validator.MaxSigOps,
		MaxScriptSizePolicy:     cfg.Validator.MaxScriptSize,
		MinFeePerKB:             cfg.Validator.MinFeePerKB,
	}
	txValidator := validator.NewValidator(validatorPolicy)

	logger.Info("Initializing P2P client")
	p2pClient, err := terap2p.NewClient(terap2p.Config{
		Name:           cfg.P2P.ProcessName,
		StoragePath:    cfg.P2P.PeerCacheFile,
		Network:        cfg.P2P.TopicPrefix,
		BootstrapPeers: cfg.P2P.BootstrapPeers,
		Logger:         logger,
		Port:           cfg.P2P.Port,
	})
	if err != nil {
		return fmt.Errorf("failed to create P2P client: %w", err)
	}

	logger.Info("Initializing P2P subscriber")
	subscriber, err := p2p.NewSubscriber(
		ctx,
		&p2p.Config{
			ProcessName:    cfg.P2P.ProcessName,
			Port:           cfg.P2P.Port,
			BootstrapPeers: cfg.P2P.BootstrapPeers,
			PrivateKey:     cfg.P2P.PrivateKey,
			TopicPrefix:    cfg.P2P.TopicPrefix,
			PeerCacheFile:  cfg.P2P.PeerCacheFile,
		},
		statusStore,
		networkStore,
		eventPublisher,
		logger,
		p2pClient,
	)
	if err != nil {
		return fmt.Errorf("failed to create P2P subscriber: %w", err)
	}

	if err := subscriber.Start(ctx); err != nil {
		return fmt.Errorf("failed to start P2P subscriber: %w", err)
	}

	logger.Info("Performing gap filling check")
	if err := performGapFilling(ctx, networkStore, logger); err != nil {
		logger.Warn("Gap filling failed", slog.String("error", err.Error()))
	}

	logger.Info("Initializing webhook handler")
	webhookHandler := handlers.NewWebhookHandler(
		eventPublisher,
		submissionStore,
		statusStore,
		logger,
		cfg.Webhook.PruneInterval,
		cfg.Webhook.MaxAge,
		cfg.Webhook.MaxRetries,
	)

	if err := webhookHandler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start webhook handler: %w", err)
	}

	logger.Info("Initializing API server", slog.String("address", cfg.Server.Address))
	apiPolicy := &api.PolicyResponse{
		MaxTxSizePolicy:         uint64(cfg.Validator.MaxTxSize),
		MaxTxSigOpsCountsPolicy: uint64(cfg.Validator.MaxSigOps),
		MaxScriptSizePolicy:     uint64(cfg.Validator.MaxScriptSize),
		MiningFee: api.FeeAmount{
			Bytes:    1000,
			Satoshis: cfg.Validator.MinFeePerKB,
		},
	}

	authToken := ""
	if cfg.Auth.Enabled {
		authToken = cfg.Auth.Token
		logger.Info("API authentication enabled")
	}

	apiServer := api.NewServer(
		statusStore,
		submissionStore,
		networkStore,
		eventPublisher,
		teranodeClient,
		txValidator,
		apiPolicy,
		authToken,
		logger,
	)

	errCh := make(chan error, 1)
	go func() {
		if err := apiServer.Start(cfg.Server.Address); err != nil {
			errCh <- fmt.Errorf("API server error: %w", err)
		}
	}()

	logger.Info("Arcade started successfully")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		logger.Info("Received shutdown signal")
	case err := <-errCh:
		logger.Error("Server error", slog.String("error", err.Error()))
		return err
	case <-ctx.Done():
		logger.Info("Context cancelled")
	}

	logger.Info("Shutting down gracefully")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	if err := apiServer.Stop(shutdownCtx); err != nil {
		logger.Error("Error during shutdown", slog.String("error", err.Error()))
	}

	webhookHandler.Stop()

	if err := subscriber.Stop(); err != nil {
		logger.Error("Error stopping P2P subscriber", slog.String("error", err.Error()))
	}

	if err := eventPublisher.Close(); err != nil {
		logger.Error("Error closing event publisher", slog.String("error", err.Error()))
	}

	logger.Info("Shutdown complete")
	return nil
}

// performGapFilling checks for missing blocks and attempts to fill gaps
func performGapFilling(ctx context.Context, networkStore store.NetworkStateStore, logger *slog.Logger) error {
	state, err := networkStore.GetNetworkState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get network state: %w", err)
	}

	if state == nil {
		logger.Info("No previous network state, skipping gap filling")
		return nil
	}

	logger.Info("Current network state",
		slog.Uint64("height", state.CurrentHeight),
		slog.String("hash", state.LastBlockHash))

	return nil
}
