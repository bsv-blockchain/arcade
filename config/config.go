// Package config provides configuration types for arcade.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/events/memory"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/sqlite"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
	chaintracksconfig "github.com/bsv-blockchain/go-chaintracks/config"
	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	p2p "github.com/bsv-blockchain/go-teranode-p2p-client"
	"github.com/spf13/viper"
)

// Config holds all application configuration
type Config struct {
	Network     string `mapstructure:"network"`      // "main", "test", "stn" - Bitcoin network
	StoragePath string `mapstructure:"storage_path"` // Data directory for persistent files

	Server        ServerConfig             `mapstructure:"server"`
	Database      DatabaseConfig           `mapstructure:"database"`
	Events        EventsConfig             `mapstructure:"events"`
	Teranode      TeranodeConfig           `mapstructure:"teranode"`
	P2P           p2p.Config               `mapstructure:"p2p"`
	Validator ValidatorConfig `mapstructure:"validator"`
	Auth      AuthConfig      `mapstructure:"auth"`
	Webhook       WebhookConfig            `mapstructure:"webhook"`
	Chaintracks   chaintracksconfig.Config `mapstructure:"chaintracks"`
}

// SetDefaults sets viper defaults for arcade configuration when used as an embedded library.
func (c *Config) SetDefaults(v *viper.Viper, prefix string) {
	p := ""
	if prefix != "" {
		p = prefix + "."
	}

	// Top-level defaults
	v.SetDefault(p+"network", "main")
	v.SetDefault(p+"storage_path", "~/.arcade")

	// Server defaults
	v.SetDefault(p+"server.address", ":3011")
	v.SetDefault(p+"server.read_timeout", "30s")
	v.SetDefault(p+"server.write_timeout", "30s")
	v.SetDefault(p+"server.shutdown_timeout", "10s")

	// Database defaults
	v.SetDefault(p+"database.type", "sqlite")
	v.SetDefault(p+"database.sqlite_path", "~/.arcade/arcade.db")

	// Events defaults
	v.SetDefault(p+"events.type", "memory")
	v.SetDefault(p+"events.buffer_size", 1000)

	// Teranode defaults
	v.SetDefault(p+"teranode.timeout", "30s")

	// Validator defaults
	v.SetDefault(p+"validator.max_tx_size", 4294967296)
	v.SetDefault(p+"validator.max_script_size", 500000)
	v.SetDefault(p+"validator.max_sig_ops", 4294967295)
	v.SetDefault(p+"validator.min_fee_per_kb", 50)

	// Auth defaults
	v.SetDefault(p+"auth.enabled", false)

	// Webhook defaults
	v.SetDefault(p+"webhook.prune_interval", "1h")
	v.SetDefault(p+"webhook.max_age", "24h")
	v.SetDefault(p+"webhook.max_retries", 10)

	// Delegate to external libraries
	c.P2P.SetDefaults(v, p+"p2p")
	c.Chaintracks.SetDefaults(v, p+"chaintracks")
}

// ServerConfig holds HTTP API server configuration
type ServerConfig struct {
	Address         string        `mapstructure:"address"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Type            string `mapstructure:"type"` // "sqlite" or "postgres"
	SQLitePath      string `mapstructure:"sqlite_path"`
	PostgresConnStr string `mapstructure:"postgres_conn_str"`
}

// EventsConfig holds event publisher configuration
type EventsConfig struct {
	Type       string `mapstructure:"type"` // "memory" or "redis"
	BufferSize int    `mapstructure:"buffer_size"`
	RedisURL   string `mapstructure:"redis_url"`
}

// TeranodeConfig holds teranode client configuration
type TeranodeConfig struct {
	BaseURL  string        `mapstructure:"base_url"`
	BaseURLs []string      `mapstructure:"base_urls"`
	Timeout  time.Duration `mapstructure:"timeout"`
}

// ValidatorConfig holds transaction validator configuration
type ValidatorConfig struct {
	MaxTxSize     int    `mapstructure:"max_tx_size"`
	MaxScriptSize int    `mapstructure:"max_script_size"`
	MaxSigOps     int64  `mapstructure:"max_sig_ops"`
	MinFeePerKB   uint64 `mapstructure:"min_fee_per_kb"`
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Token   string `mapstructure:"token"`
}

// WebhookConfig holds webhook handler configuration
type WebhookConfig struct {
	PruneInterval time.Duration `mapstructure:"prune_interval"`
	MaxAge        time.Duration `mapstructure:"max_age"`
	MaxRetries    int           `mapstructure:"max_retries"`
}

// Services holds initialized application services.
type Services struct {
	P2PClient       *p2p.Client
	Chaintracks     chaintracks.Chaintracks
	StatusStore     store.StatusStore
	SubmissionStore store.SubmissionStore
	TxTracker       *store.TxTracker
	EventPublisher  events.Publisher
	TeranodeClient  *teranode.Client
	Validator       *validator.Validator
	Logger          *slog.Logger
}

// Initialize creates and returns all application services.
func (c *Config) Initialize(ctx context.Context, logger *slog.Logger) (*Services, error) {
	if logger == nil {
		logger = slog.Default()
	}

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

	// Initialize stores
	logger.Info("Initializing stores")
	statusStore, err := sqlite.NewStatusStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create status store: %w", err)
	}

	submissionStore, err := sqlite.NewSubmissionStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create submission store: %w", err)
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

	// Initialize Teranode client
	logger.Info("Initializing Teranode client")
	var endpoints []string
	if len(c.Teranode.BaseURLs) > 0 {
		endpoints = c.Teranode.BaseURLs
	} else {
		endpoints = []string{c.Teranode.BaseURL}
	}
	teranodeClient := teranode.NewClient(endpoints)

	// Initialize validator
	logger.Info("Initializing validator")
	txValidator := validator.NewValidator(&validator.Policy{
		MaxTxSizePolicy:         c.Validator.MaxTxSize,
		MaxTxSigopsCountsPolicy: c.Validator.MaxSigOps,
		MinFeePerKB:             c.Validator.MinFeePerKB,
	})

	// Initialize P2P client
	logger.Info("Initializing P2P client")
	c.P2P.Network = c.Network
	if c.P2P.StoragePath == "" {
		c.P2P.StoragePath = c.StoragePath
	}
	p2pClient, err := c.P2P.Initialize(ctx, "arcade")
	if err != nil {
		return nil, fmt.Errorf("failed to create P2P client: %w", err)
	}

	// Initialize transaction tracker
	logger.Info("Initializing transaction tracker")
	txTracker := store.NewTxTracker()
	trackedCount, err := txTracker.LoadFromStore(ctx, statusStore, 0)
	if err != nil {
		logger.Warn("Failed to load tracked transactions", slog.String("error", err.Error()))
	} else {
		logger.Info("Loaded tracked transactions", slog.Int("count", trackedCount))
	}

	// Initialize Chaintracks (always embedded, sharing arcade's P2P client)
	logger.Info("Initializing Chaintracks")
	if c.Chaintracks.StoragePath == "" {
		c.Chaintracks.StoragePath = path.Join(c.StoragePath, "chaintracks")
	}
	chaintracker, err := c.Chaintracks.Initialize(ctx, "arcade", p2pClient)
	if err != nil {
		_ = p2pClient.Close()
		return nil, fmt.Errorf("failed to initialize chaintracks: %w", err)
	}

	return &Services{
		P2PClient:       p2pClient,
		Chaintracks:     chaintracker,
		StatusStore:     statusStore,
		SubmissionStore: submissionStore,
		TxTracker:       txTracker,
		EventPublisher:  eventPublisher,
		TeranodeClient:  teranodeClient,
		Validator:       txValidator,
		Logger:          logger,
	}, nil
}
