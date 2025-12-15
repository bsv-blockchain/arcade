// Arcade API Server
//
// Transaction broadcast and status tracking service for BSV.
//
//	@title			Arcade API
//	@version		0.1.0
//	@description	BSV transaction broadcast service with ARC-compatible endpoints.
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
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/docs"
	"github.com/bsv-blockchain/arcade/handlers"
	fiberRoutes "github.com/bsv-blockchain/arcade/routes/fiber"
	chaintracksRoutes "github.com/bsv-blockchain/go-chaintracks/routes/fiber"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

var authToken string

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
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	log.Info("Starting Arcade", slog.String("version", "0.1.0"))

	cfg, err := Load()
	if err != nil {
		log.Error("Failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("Application error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	// Initialize all services (nil for chaintracker and p2pClient = arcade creates its own)
	services, err := cfg.Initialize(ctx, log, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to initialize services: %w", err)
	}
	defer func() {
		if err := services.Close(); err != nil {
			log.Error("Error closing services", slog.String("error", err.Error()))
		}
	}()

	// Subscribe to chaintracks tip updates
	tipChan := services.Chaintracks.Subscribe(ctx)

	ctRoutes := chaintracksRoutes.NewRoutes(services.Chaintracks)
	ctRoutes.StartBroadcasting(ctx, tipChan)

	// Initialize webhook handler
	log.Info("Initializing webhook handler")
	webhookHandler := handlers.NewWebhookHandler(
		services.EventPublisher,
		services.Store,
		log,
		cfg.Webhook.PruneInterval,
		cfg.Webhook.MaxAge,
		cfg.Webhook.MaxRetries,
	)

	if err := webhookHandler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start webhook handler: %w", err)
	}
	defer webhookHandler.Stop()

	// Setup routes
	arcadeRoutes := fiberRoutes.NewRoutes(fiberRoutes.Config{
		Service:        services.ArcadeService,
		Store:          services.Store,
		EventPublisher: services.EventPublisher,
		Arcade:         services.Arcade,
		Logger:         log,
	})

	if cfg.Auth.Enabled {
		authToken = cfg.Auth.Token
		log.Info("API authentication enabled")
	}

	// Setup dashboard
	dashboard := NewDashboard(services.Arcade)

	// Setup and start HTTP server
	log.Info("Starting HTTP server", slog.String("address", cfg.Server.Address))
	app := setupServer(arcadeRoutes, ctRoutes, dashboard)

	errCh := make(chan error, 1)
	go func() {
		if err := app.Listen(cfg.Server.Address); err != nil {
			errCh <- fmt.Errorf("server error: %w", err)
		}
	}()

	log.Info("Arcade started successfully")

	// Wait for shutdown signal
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

	// Graceful shutdown
	log.Info("Shutting down gracefully")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Error("Error during server shutdown", slog.String("error", err.Error()))
	}

	log.Info("Shutdown complete")
	return nil
}

func setupServer(arcadeRoutes *fiberRoutes.Routes, chaintracksRoutes *chaintracksRoutes.Routes, dashboard *Dashboard) *fiber.App {
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
	arcadeRoutes.Register(app)

	// Health check (standalone arcade server only)
	app.Get("/health", arcadeRoutes.HandleGetHealth)

	// Chaintracks endpoints (under /v2 prefix)
	chaintracksGroup := app.Group("/v2")
	chaintracksRoutes.Register(chaintracksGroup)

	// Status dashboard
	app.Get("/", dashboard.HandleDashboard)
	app.Get("/status", dashboard.HandleDashboard)

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
