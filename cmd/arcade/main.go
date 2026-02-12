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

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	fiberlogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"

	chaintracksRoutes "github.com/bsv-blockchain/go-chaintracks/routes/fiber"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/docs"
	"github.com/bsv-blockchain/arcade/logging"
	fiberRoutes "github.com/bsv-blockchain/arcade/routes/fiber"
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
	// Load config first to get log level
	cfg, err := Load()
	if err != nil {
		slog.Error("Failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Create logger with configured level
	log := logging.NewLogger(cfg.GetLogLevel())

	log.Info("Starting Arcade", slog.String("version", "0.1.0"), slog.String("log_level", cfg.GetLogLevel()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("Application error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	// Initialize all services (nil for chaintracker and p2pClient = arcade creates its own)
	// This also starts the webhook handler for callback delivery
	services, err := cfg.Initialize(ctx, log, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to initialize services: %w", err)
	}
	defer func() {
		if err := services.Close(); err != nil {
			log.Error("Error closing services", slog.String("error", err.Error()))
		}
	}()

	// Setup routes
	arcadeRoutes := fiberRoutes.NewRoutes(fiberRoutes.Config{
		Service:        services.ArcadeService,
		Store:          services.Store,
		EventPublisher: services.EventPublisher,
		Arcade:         services.Arcade,
		Logger:         log,
	})

	// Setup chaintracks routes (if enabled and running in embedded mode)
	var chaintracksRts *chaintracksRoutes.Routes
	if cfg.ChaintracksServer.Enabled && services.Chaintracks != nil {
		chaintracksRts = chaintracksRoutes.NewRoutes(ctx, services.Chaintracks)
		log.Info("Chaintracks HTTP API enabled", slog.String("routes", "/chaintracks/v1/*, /chaintracks/v2/*"))
	}

	// Setup dashboard
	dashboard := NewDashboard(services.Arcade)

	// Setup and start HTTP server
	log.Info("Starting HTTP server", slog.String("address", cfg.Server.Address))
	authToken := ""
	if cfg.Auth.Enabled {
		authToken = cfg.Auth.Token
		log.Info("API authentication enabled")
	}
	app := setupServer(arcadeRoutes, chaintracksRts, dashboard, authToken)

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
		log.Info("Context canceled")
	}

	// Graceful shutdown
	log.Info("Shutting down gracefully")

	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Error("Error during server shutdown", slog.String("error", err.Error()))
	}

	log.Info("Shutdown complete")
	return nil
}

func setupServer(arcadeRoutes *fiberRoutes.Routes, chaintracksRts *chaintracksRoutes.Routes, dashboard *Dashboard, authToken string) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Use(fiberlogger.New(fiberlogger.Config{
		Format: "${method} ${path} - ${status} (${latency})\n",
	}))
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "*",
		AllowMethods: "GET,POST,OPTIONS",
	}))

	if authToken != "" {
		app.Use(authMiddleware(authToken))
	}

	// Transaction endpoints (ARC-compatible)
	arcadeRoutes.Register(app)

	// Chaintracks endpoints (block header tracking)
	if chaintracksRts != nil {
		chaintracksGroup := app.Group("/chaintracks")
		chaintracksRts.Register(chaintracksGroup.Group("/v2"))
		chaintracksRts.RegisterLegacy(chaintracksGroup.Group("/v1"))
	}

	// Health check (standalone arcade server only)
	app.Get("/health", arcadeRoutes.HandleGetHealth)

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

func authMiddleware(token string) fiber.Handler {
	return func(c *fiber.Ctx) error {
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

		if auth[len(bearerPrefix):] != token {
			return c.Status(401).JSON(fiber.Map{"error": "Invalid token"})
		}

		return c.Next()
	}
}
