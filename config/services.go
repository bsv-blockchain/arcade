package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	p2p "github.com/bsv-blockchain/go-teranode-p2p-client"

	"github.com/bsv-blockchain/arcade"
	"github.com/bsv-blockchain/arcade/client"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/events/memory"
	"github.com/bsv-blockchain/arcade/handlers"
	"github.com/bsv-blockchain/arcade/logging"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/service"
	"github.com/bsv-blockchain/arcade/service/embedded"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/sqlite"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
)

// Services holds initialized application services.
type Services struct {
	// ArcadeService is the main interface (always set for both modes)
	ArcadeService service.ArcadeService

	// Internal components (only set for embedded mode, nil for remote mode)
	P2PClient      *p2p.Client
	Chaintracks    chaintracks.Chaintracks
	Arcade         *arcade.Arcade
	Store          store.Store
	TxTracker      *store.TxTracker
	EventPublisher events.Publisher
	TeranodeClient *teranode.Client
	Validator      *validator.Validator
	WebhookHandler *handlers.WebhookHandler
	Logger         *slog.Logger
	Config         *Config
}

// Initialize creates and returns all application services.
// If chaintracker is provided, it will be used instead of creating a new one.
// If p2pClient is provided, it will be shared instead of creating a new one.
// This allows the caller to share instances across services.
func (c *Config) Initialize(ctx context.Context, logger *slog.Logger, chaintracker chaintracks.Chaintracks, p2pClient *p2p.Client) (*Services, error) {
	// Create arcade-specific logger with configured log level
	logger = logging.NewLogger(c.GetLogLevel())

	switch c.Mode {
	case ModeRemote:
		return c.initializeRemote(logger)
	case ModeEmbedded, "":
		return c.initializeEmbedded(ctx, logger, chaintracker, p2pClient)
	default:
		return nil, fmt.Errorf("unknown arcade mode: %s", c.Mode)
	}
}

// initializeRemote creates a remote client service.
func (c *Config) initializeRemote(logger *slog.Logger) (*Services, error) {
	if c.URL == "" {
		return nil, fmt.Errorf("arcade URL required for remote mode")
	}

	logger.Info("Initializing Arcade in remote mode", slog.String("url", c.URL))

	return &Services{
		ArcadeService: client.New(c.URL),
		Logger:        logger,
		Config:        c,
	}, nil
}

// initializeEmbedded creates all embedded services.
func (c *Config) initializeEmbedded(ctx context.Context, logger *slog.Logger, chaintracker chaintracks.Chaintracks, p2pClient *p2p.Client) (*Services, error) {
	logger.Info("Initializing Arcade in embedded mode")

	// Expand ~ in storage path
	if len(c.StoragePath) >= 2 && c.StoragePath[:2] == "~/" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to resolve home directory: %w", err)
		}
		c.StoragePath = path.Join(homeDir, c.StoragePath[2:])
	}

	// Ensure storage directory exists
	if err := os.MkdirAll(c.StoragePath, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Expand ~ in database path
	dbPath := c.Database.SQLitePath
	if len(dbPath) >= 2 && dbPath[:2] == "~/" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to resolve home directory for database: %w", err)
		}
		dbPath = path.Join(homeDir, dbPath[2:])
	}

	// Run database migrations
	logger.Info("Running database migrations", slog.String("path", dbPath))
	if err := store.RunMigrations(dbPath); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	// Initialize store
	logger.Info("Initializing store")
	sqliteStore, err := sqlite.NewStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	// Initialize event publisher
	logger.Info("Initializing event publisher", slog.String("type", c.Events.Type))
	var eventPublisher events.Publisher
	switch c.Events.Type {
	case "memory", "":
		eventPublisher = memory.NewInMemoryPublisher(c.Events.BufferSize)
	default:
		return nil, fmt.Errorf("unsupported event publisher type: %s", c.Events.Type)
	}

	// Initialize Teranode client for broadcasting
	logger.Info("Initializing Teranode client")
	if len(c.Teranode.BroadcastURLs) == 0 {
		return nil, fmt.Errorf("no teranode broadcast endpoints configured: set teranode.broadcast_urls")
	}
	teranodeClient := teranode.NewClient(c.Teranode.BroadcastURLs, c.Teranode.AuthToken)

	// Initialize validator
	logger.Info("Initializing validator")
	txValidator := validator.NewValidator(&validator.Policy{
		MaxTxSizePolicy:         c.Validator.MaxTxSize,
		MaxTxSigopsCountsPolicy: c.Validator.MaxSigOps,
		MinFeePerKB:             c.Validator.MinFeePerKB,
	})

	// Initialize transaction tracker
	logger.Info("Initializing transaction tracker")
	txTracker := store.NewTxTracker()
	trackedCount, err := txTracker.LoadFromStore(ctx, sqliteStore, 0)
	if err != nil {
		logger.Warn("Failed to load tracked transactions", slog.String("error", err.Error()))
	} else {
		logger.Info("Loaded tracked transactions", slog.Int("count", trackedCount))
	}

	// Use provided P2P client or create one
	ownsP2PClient := false
	if p2pClient == nil {
		logger.Info("Initializing P2P client")
		c.P2P.Network = c.Network
		if c.P2P.StoragePath == "" {
			c.P2P.StoragePath = c.StoragePath
		}
		p2pClient, err = c.P2P.Initialize(ctx, "arcade")
		if err != nil {
			return nil, fmt.Errorf("failed to create P2P client: %w", err)
		}
		ownsP2PClient = true
	} else {
		logger.Info("Using provided P2P client")
	}

	// Use provided Chaintracks or create one
	if chaintracker == nil {
		logger.Info("Initializing Chaintracks")
		if c.Chaintracks.StoragePath == "" {
			c.Chaintracks.StoragePath = path.Join(c.StoragePath, "chaintracks")
		}
		chaintracker, err = c.Chaintracks.Initialize(ctx, "arcade", p2pClient)
		if err != nil {
			if ownsP2PClient {
				_ = p2pClient.Close()
			}
			return nil, fmt.Errorf("failed to initialize chaintracks: %w", err)
		}
	} else {
		logger.Info("Using provided Chaintracks instance")
	}

	// Initialize Arcade P2P listener
	logger.Info("Initializing Arcade P2P listener")
	arcadeInstance, err := arcade.NewArcade(arcade.Config{
		P2PClient:      p2pClient,
		Chaintracks:    chaintracker,
		Logger:         logger,
		TxTracker:      txTracker,
		Store:          sqliteStore,
		EventPublisher: eventPublisher,
		DataHubURLs:    c.Teranode.DataHubURLs,
	})
	if err != nil {
		if ownsP2PClient {
			_ = p2pClient.Close()
		}
		return nil, fmt.Errorf("failed to create arcade: %w", err)
	}

	if err := arcadeInstance.Start(ctx); err != nil {
		if ownsP2PClient {
			_ = p2pClient.Close()
		}
		return nil, fmt.Errorf("failed to start arcade: %w", err)
	}

	// Create policy
	policy := &models.Policy{
		MaxTxSizePolicy:         uint64(c.Validator.MaxTxSize),
		MaxTxSigOpsCountsPolicy: uint64(c.Validator.MaxSigOps),
		MaxScriptSizePolicy:     uint64(c.Validator.MaxScriptSize),
		MiningFeeBytes:          1000,
		MiningFeeSatoshis:       c.Validator.MinFeePerKB,
	}

	// Create embedded service
	embeddedService, err := embedded.New(embedded.Config{
		Store:          sqliteStore,
		TxTracker:      txTracker,
		EventPublisher: eventPublisher,
		TeranodeClient: teranodeClient,
		TxValidator:    txValidator,
		Arcade:         arcadeInstance,
		Policy:         policy,
		Logger:         logger,
	})
	if err != nil {
		_ = arcadeInstance.Stop()
		if ownsP2PClient {
			_ = p2pClient.Close()
		}
		return nil, fmt.Errorf("failed to create embedded service: %w", err)
	}

	// Initialize webhook handler for outbound callback delivery
	logger.Info("Initializing webhook handler")
	webhookHandler := handlers.NewWebhookHandler(
		eventPublisher,
		sqliteStore,
		logger,
		c.Webhook.PruneInterval,
		c.Webhook.MaxAge,
		c.Webhook.MaxRetries,
	)
	if err := webhookHandler.Start(ctx); err != nil {
		_ = arcadeInstance.Stop()
		if ownsP2PClient {
			_ = p2pClient.Close()
		}
		return nil, fmt.Errorf("failed to start webhook handler: %w", err)
	}

	return &Services{
		ArcadeService:  embeddedService,
		P2PClient:      p2pClient,
		Chaintracks:    chaintracker,
		Arcade:         arcadeInstance,
		Store:          sqliteStore,
		TxTracker:      txTracker,
		EventPublisher: eventPublisher,
		TeranodeClient: teranodeClient,
		Validator:      txValidator,
		WebhookHandler: webhookHandler,
		Logger:         logger,
		Config:         c,
	}, nil
}

// Close gracefully shuts down all services.
func (s *Services) Close() error {
	if s == nil {
		return nil
	}

	var errs []error

	// Stop webhook handler first (depends on event publisher)
	if s.WebhookHandler != nil {
		s.WebhookHandler.Stop()
	}

	// Stop Arcade P2P listener
	if s.Arcade != nil {
		if err := s.Arcade.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("arcade stop: %w", err))
		}
	}

	// Close event publisher
	if s.EventPublisher != nil {
		if err := s.EventPublisher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("event publisher close: %w", err))
		}
	}

	// Close P2P client (also stops Chaintracks via context)
	if s.P2PClient != nil {
		if err := s.P2PClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("p2p client close: %w", err))
		}
	}

	return errors.Join(errs...)
}
