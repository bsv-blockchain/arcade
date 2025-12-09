// Arcade API Server
//
// Transaction broadcast and blockchain header service for BSV.
//
//	@title			Arcade API
//	@version		0.1.0
//	@description	BSV transaction broadcast service with ARC-compatible endpoints and blockchain header queries.
//
//	@contact.name	BSV Blockchain
//	@contact.url	https://github.com/bsv-blockchain/arcade
//
//	@license.name	Open BSV License
//	@license.url	https://github.com/bsv-blockchain/arcade/blob/main/LICENSE
//
//	@host
//	@BasePath	/
//
//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization
//	@description				Bearer token authentication
package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/bsv-blockchain/arcade"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/docs"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/events/memory"
	"github.com/bsv-blockchain/arcade/handlers"
	"github.com/bsv-blockchain/arcade/p2p"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/sqlite"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

// Server state accessible to route handlers
var (
	statusStore     store.StatusStore
	submissionStore store.SubmissionStore
	txTracker       *store.TxTracker
	eventPublisher  events.Publisher
	teranodeClient  *teranode.Client
	txValidator     *validator.Validator
	arcadeInstance  *arcade.Arcade
	policy          *PolicyResponse
	authToken       string
	log             *slog.Logger

	// SSE clients for tip streaming
	tipClients   = make(map[int64]*bufio.Writer)
	tipClientsMu sync.RWMutex
)

const scalarHTML = `<!DOCTYPE html>
<html>
<head>
  <title>Arcade API</title>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
</head>
<body>
  <script id="api-reference" data-url="/docs/openapi.json"></script>
  <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</body>
</html>`

func main() {
	log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	log.Info("Starting Arcade", slog.String("version", "0.1.0"))

	cfg := config.Default()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		log.Error("Application error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config.Config) error {
	log.Info("Initializing stores", slog.String("db", cfg.Database.SQLitePath))

	log.Info("Running database migrations")
	if err := store.RunMigrations(cfg.Database.SQLitePath); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	var err error
	statusStore, err = sqlite.NewStatusStore(cfg.Database.SQLitePath)
	if err != nil {
		return fmt.Errorf("failed to create status store: %w", err)
	}

	submissionStore, err = sqlite.NewSubmissionStore(cfg.Database.SQLitePath)
	if err != nil {
		return fmt.Errorf("failed to create submission store: %w", err)
	}

	networkStore, err := sqlite.NewNetworkStateStore(cfg.Database.SQLitePath)
	if err != nil {
		return fmt.Errorf("failed to create network state store: %w", err)
	}

	log.Info("Initializing event publisher", slog.String("type", cfg.Events.Type))
	eventPublisher = memory.NewInMemoryPublisher(cfg.Events.BufferSize)
	log.Info("Using in-memory event publisher", slog.Int("buffer", cfg.Events.BufferSize))

	log.Info("Initializing Teranode client")
	var endpoints []string
	if len(cfg.Teranode.BaseURLs) > 0 {
		endpoints = cfg.Teranode.BaseURLs
		log.Info("Using multiple Teranode endpoints", slog.Int("count", len(endpoints)))
	} else {
		endpoints = []string{cfg.Teranode.BaseURL}
		log.Info("Using single Teranode endpoint", slog.String("url", cfg.Teranode.BaseURL))
	}
	teranodeClient = teranode.NewClient(endpoints)

	log.Info("Initializing validator")
	txValidator = validator.NewValidator(&validator.Policy{
		MaxTxSizePolicy:         cfg.Validator.MaxTxSize,
		MaxTxSigopsCountsPolicy: cfg.Validator.MaxSigOps,
		MinFeePerKB:             cfg.Validator.MinFeePerKB,
	})

	log.Info("Initializing P2P client")
	p2pClient, err := p2p.NewClient(cfg.P2P, cfg.Network, log)
	if err != nil {
		return fmt.Errorf("failed to create P2P client: %w", err)
	}

	log.Info("Initializing transaction tracker")
	txTracker = store.NewTxTracker()
	var currentHeight uint64
	if state, err := networkStore.GetNetworkState(ctx); err == nil && state != nil {
		currentHeight = state.CurrentHeight
	}
	trackedCount, err := txTracker.LoadFromStore(ctx, statusStore, currentHeight)
	if err != nil {
		log.Warn("Failed to load tracked transactions", slog.String("error", err.Error()))
	} else {
		log.Info("Loaded tracked transactions", slog.Int("count", trackedCount))
	}

	log.Info("Initializing Arcade")
	arcadeInstance, err = arcade.NewArcade(ctx, arcade.Config{
		Network:          cfg.Network,
		LocalStoragePath: cfg.StoragePath,
		Logger:           log,
		P2PClient:        p2pClient,
		BootstrapURL:     cfg.Teranode.BaseURL,
		TxTracker:        txTracker,
		StatusStore:      statusStore,
		EventPublisher:   eventPublisher,
	})
	if err != nil {
		return fmt.Errorf("failed to create arcade: %w", err)
	}

	if err := arcadeInstance.Start(ctx); err != nil {
		return fmt.Errorf("failed to start arcade: %w", err)
	}

	go broadcastTipUpdates(ctx, arcadeInstance.SubscribeTip(ctx))

	log.Info("Performing gap filling check")
	if err := performGapFilling(ctx, networkStore); err != nil {
		log.Warn("Gap filling failed", slog.String("error", err.Error()))
	}

	log.Info("Initializing webhook handler")
	webhookHandler := handlers.NewWebhookHandler(
		eventPublisher,
		submissionStore,
		statusStore,
		log,
		cfg.Webhook.PruneInterval,
		cfg.Webhook.MaxAge,
		cfg.Webhook.MaxRetries,
	)

	if err := webhookHandler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start webhook handler: %w", err)
	}

	policy = &PolicyResponse{
		MaxTxSizePolicy:         uint64(cfg.Validator.MaxTxSize),
		MaxTxSigOpsCountsPolicy: uint64(cfg.Validator.MaxSigOps),
		MaxScriptSizePolicy:     uint64(cfg.Validator.MaxScriptSize),
		MiningFee: FeeAmount{
			Bytes:    1000,
			Satoshis: cfg.Validator.MinFeePerKB,
		},
	}

	if cfg.Auth.Enabled {
		authToken = cfg.Auth.Token
		log.Info("API authentication enabled")
	}

	log.Info("Starting HTTP server", slog.String("address", cfg.Server.Address))
	app := setupServer()

	errCh := make(chan error, 1)
	go func() {
		if err := app.Listen(cfg.Server.Address); err != nil {
			errCh <- fmt.Errorf("server error: %w", err)
		}
	}()

	log.Info("Arcade started successfully")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Info("Received shutdown signal")
	case err := <-errCh:
		log.Error("Server error", slog.String("error", err.Error()))
		return err
	case <-ctx.Done():
		log.Info("Context cancelled")
	}

	log.Info("Shutting down gracefully")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Error("Error during server shutdown", slog.String("error", err.Error()))
	}

	webhookHandler.Stop()

	if err := arcadeInstance.Stop(); err != nil {
		log.Error("Error stopping arcade", slog.String("error", err.Error()))
	}

	if err := p2pClient.Close(); err != nil {
		log.Error("Error closing P2P client", slog.String("error", err.Error()))
	}

	if err := eventPublisher.Close(); err != nil {
		log.Error("Error closing event publisher", slog.String("error", err.Error()))
	}

	log.Info("Shutdown complete")
	return nil
}

func setupServer() *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Use(logger.New(logger.Config{
		Format: "${method} ${path} - ${status} (${latency})\n",
	}))
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "*",
		AllowMethods: "GET,POST,OPTIONS",
	}))

	if authToken != "" {
		app.Use(authMiddleware)
	}

	// Transaction endpoints (ARC-compatible)
	app.Post("/tx", handlePostTx)
	app.Post("/txs", handlePostTxs)
	app.Get("/tx/:txid", handleGetTx)
	app.Get("/policy", handleGetPolicy)
	app.Get("/health", handleGetHealth)
	app.Get("/events/:callbackToken", handleTxSSE)

	// Header endpoints
	app.Get("/network", handleGetNetwork)
	app.Get("/height", handleGetHeight)
	app.Get("/tip", handleGetTip)
	app.Get("/tip/stream", handleTipStream)
	app.Get("/header/height/:height", handleGetHeaderByHeight)
	app.Get("/header/hash/:hash", handleGetHeaderByHash)
	app.Get("/headers", handleGetHeaders)

	// Status dashboard
	app.Get("/", handleDashboard)
	app.Get("/status", handleDashboard)

	// API docs (Scalar UI)
	app.Get("/docs/openapi.json", func(c *fiber.Ctx) error {
		return c.Type("json").SendString(docs.SwaggerInfo.ReadDoc())
	})
	app.Get("/docs", func(c *fiber.Ctx) error {
		return c.Type("html").SendString(scalarHTML)
	})

	return app
}

func authMiddleware(c *fiber.Ctx) error {
	path := c.Path()
	if path == "/health" || path == "/policy" || path == "/status" || path == "/docs" || path == "/docs/*" {
		return c.Next()
	}

	auth := c.Get("Authorization")
	if auth == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Missing authorization header"})
	}

	const bearerPrefix = "Bearer "
	if len(auth) < len(bearerPrefix) || auth[:len(bearerPrefix)] != bearerPrefix {
		return c.Status(401).JSON(fiber.Map{"error": "Invalid authorization header format"})
	}

	if auth[len(bearerPrefix):] != authToken {
		return c.Status(401).JSON(fiber.Map{"error": "Invalid token"})
	}

	return c.Next()
}

func performGapFilling(ctx context.Context, networkStore store.NetworkStateStore) error {
	state, err := networkStore.GetNetworkState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get network state: %w", err)
	}

	if state == nil {
		log.Info("No previous network state, skipping gap filling")
		return nil
	}

	log.Info("Current network state",
		slog.Uint64("height", state.CurrentHeight),
		slog.String("hash", state.LastBlockHash))

	return nil
}
