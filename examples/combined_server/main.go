// Combined Arcade + Chaintracks Server Example
//
// This example demonstrates how to run both Arcade (transaction broadcast) and
// Chaintracks (block header tracking) in a single server, sharing the same P2P
// client. This is useful when you want a complete BSV infrastructure service.
//
// Routes:
//   - /arcade/*            - Arcade transaction endpoints (ARC-compatible)
//   - /chaintracks/v2/*    - Chaintracks header endpoints (new format)
//   - /chaintracks/v1/*    - Chaintracks legacy v1 endpoints
//   - /health              - Health check
//   - /                    - Status dashboard
//
// Usage:
//
//	go run main.go
//
// Configuration is loaded from environment variables or config file.
// See arcade/config and go-chaintracks/config for available options.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsv-blockchain/arcade/config"
	arcadeRoutes "github.com/bsv-blockchain/arcade/routes/fiber"
	chaintracksRoutes "github.com/bsv-blockchain/go-chaintracks/routes/fiber"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/spf13/viper"
)

func main() {
	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("Failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Create logger
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	log.Info("Starting Combined Arcade + Chaintracks Server")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("Application error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func loadConfig() (*config.Config, error) {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("$HOME/.arcade")
	v.SetEnvPrefix("ARCADE")
	v.AutomaticEnv()

	cfg := &config.Config{}
	cfg.SetDefaults(v, "")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
		// Config file not found is OK, use defaults + env
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}

func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	// Initialize Arcade services (this also initializes Chaintracks, P2P client,
	// and the webhook handler for callback delivery)
	// By passing nil for chaintracker and p2pClient, arcade creates its own
	services, err := cfg.Initialize(ctx, log, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to initialize services: %w", err)
	}
	defer func() {
		if err := services.Close(); err != nil {
			log.Error("Error closing services", slog.String("error", err.Error()))
		}
	}()

	// Setup Arcade routes
	arcadeRts := arcadeRoutes.NewRoutes(arcadeRoutes.Config{
		Service:        services.ArcadeService,
		Store:          services.Store,
		EventPublisher: services.EventPublisher,
		Arcade:         services.Arcade,
		Logger:         log,
	})

	// Setup Chaintracks routes
	chaintracksRts := chaintracksRoutes.NewRoutes(ctx, services.Chaintracks)

	// Create Fiber app
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

	// Register Arcade routes at /arcade
	arcadeGroup := app.Group("/arcade")
	arcadeRts.Register(arcadeGroup)

	// Register Chaintracks v2 routes at /chaintracks/v2
	chaintracksGroup := app.Group("/chaintracks")
	chaintracksV2Group := chaintracksGroup.Group("/v2")
	chaintracksRts.Register(chaintracksV2Group)

	// Register legacy Chaintracks v1 routes at /chaintracks/v1
	// These match the original chaintracks-server API format
	chaintracksV1Group := chaintracksGroup.Group("/v1")
	chaintracksRts.RegisterLegacy(chaintracksV1Group)

	// Health check
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "ok",
			"services": fiber.Map{
				"arcade":      "running",
				"chaintracks": "running",
			},
		})
	})

	// Simple status page
	app.Get("/", func(c *fiber.Ctx) error {
		tip := services.Chaintracks.GetTip(c.UserContext())
		height := uint32(0)
		if tip != nil {
			height = tip.Height
		}

		return c.Type("html").SendString(fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><title>Combined Server</title></head>
<body>
<h1>Arcade + Chaintracks Server</h1>
<h2>Status</h2>
<ul>
<li>Chain Height: %d</li>
</ul>
<h2>Endpoints</h2>
<h3>Arcade (Transaction Broadcast)</h3>
<ul>
<li>POST /arcade/tx - Submit transaction</li>
<li>POST /arcade/txs - Submit multiple transactions</li>
<li>GET /arcade/tx/{txid} - Get transaction status</li>
<li>GET /arcade/events?callbackToken=X - Stream transaction status (SSE)</li>
<li>GET /arcade/policy - Get broadcast policy</li>
</ul>
<h3>Chaintracks v2 (Block Headers)</h3>
<ul>
<li>GET /chaintracks/v2/network - Get network name</li>
<li>GET /chaintracks/v2/height - Get chain height</li>
<li>GET /chaintracks/v2/tip - Get chain tip</li>
<li>GET /chaintracks/v2/tip/stream - Stream tip updates (SSE)</li>
<li>GET /chaintracks/v2/header/height/:height - Get header by height</li>
<li>GET /chaintracks/v2/header/hash/:hash - Get header by hash</li>
<li>GET /chaintracks/v2/headers?height=N&count=M - Get multiple headers</li>
</ul>
<h3>Chaintracks Legacy v1</h3>
<ul>
<li>GET /chaintracks/v1/getChain - Get network name</li>
<li>GET /chaintracks/v1/getPresentHeight - Get chain height</li>
<li>GET /chaintracks/v1/findChainTipHashHex - Get chain tip hash</li>
<li>GET /chaintracks/v1/findChainTipHeaderHex - Get chain tip header</li>
<li>GET /chaintracks/v1/findHeaderHexForHeight?height=N - Get header by height</li>
<li>GET /chaintracks/v1/findHeaderHexForBlockHash?hash=X - Get header by hash</li>
<li>GET /chaintracks/v1/getHeaders?height=N&count=M - Get multiple headers</li>
</ul>
</body>
</html>`, height))
	})

	// Start server
	address := cfg.Server.Address
	if address == "" {
		address = ":3011"
	}
	log.Info("Starting HTTP server", slog.String("address", address))

	errCh := make(chan error, 1)
	go func() {
		if err := app.Listen(address); err != nil {
			errCh <- fmt.Errorf("server error: %w", err)
		}
	}()

	log.Info("Combined server started successfully",
		slog.String("arcade", "/arcade/*"),
		slog.String("chaintracks", "/chaintracks/*"),
	)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Info("Received shutdown signal")
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("Context cancelled")
	}

	// Graceful shutdown
	log.Info("Shutting down gracefully")
	if err := app.ShutdownWithContext(ctx); err != nil {
		log.Error("Error during server shutdown", slog.String("error", err.Error()))
	}

	log.Info("Shutdown complete")
	return nil
}
