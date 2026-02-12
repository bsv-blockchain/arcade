// Package config provides configuration types for arcade.
package config

import (
	"time"

	chaintracksconfig "github.com/bsv-blockchain/go-chaintracks/config"
	p2p "github.com/bsv-blockchain/go-teranode-p2p-client"
	"github.com/spf13/viper"
)

// Config holds all application configuration
type Config struct {
	Mode Mode   `mapstructure:"mode"` // "embedded" or "remote"
	URL  string `mapstructure:"url"`  // Required for remote mode

	// Embedded mode fields (ignored when Mode is "remote")
	Network     string `mapstructure:"network"`      // "main", "test", "stn" - Bitcoin network
	StoragePath string `mapstructure:"storage_path"` // Data directory for persistent files

	LogLevel          string                   `mapstructure:"log_level"` // Log level (debug, info, warn, error)
	Server            ServerConfig             `mapstructure:"server"`
	Database          DatabaseConfig           `mapstructure:"database"`
	Events            EventsConfig             `mapstructure:"events"`
	Teranode          TeranodeConfig           `mapstructure:"teranode"`
	P2P               p2p.Config               `mapstructure:"p2p"`
	Validator         ValidatorConfig          `mapstructure:"validator"`
	Auth              AuthConfig               `mapstructure:"auth"`
	Webhook           WebhookConfig            `mapstructure:"webhook"`
	ChaintracksServer ChaintracksServerConfig  `mapstructure:"chaintracks_server"`
	Chaintracks       chaintracksconfig.Config `mapstructure:"chaintracks"`
}

// SetDefaults sets viper defaults for arcade configuration when used as an embedded library.
func (c *Config) SetDefaults(v *viper.Viper, prefix string) {
	p := ""
	if prefix != "" {
		p = prefix + "."
	}

	// Mode defaults
	v.SetDefault(p+"mode", "embedded")
	v.SetDefault(p+"url", "")

	// Embedded mode defaults
	v.SetDefault(p+"network", "main")
	v.SetDefault(p+"storage_path", "~/.arcade")
	v.SetDefault(p+"log_level", "info")

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
	v.SetDefault(p+"teranode.broadcast_urls", []string{})
	v.SetDefault(p+"teranode.datahub_urls", []string{})
	v.SetDefault(p+"teranode.auth_token", "")
	v.SetDefault(p+"teranode.timeout", "30s")

	// Validator defaults
	v.SetDefault(p+"validator.max_tx_size", 4294967296)
	v.SetDefault(p+"validator.max_script_size", 500000)
	v.SetDefault(p+"validator.max_sig_ops", 4294967295)
	v.SetDefault(p+"validator.min_fee_per_kb", 100)

	// Auth defaults
	v.SetDefault(p+"auth.enabled", false)
	v.SetDefault(p+"auth.token", "")

	// Webhook defaults
	v.SetDefault(p+"webhook.prune_interval", "1h")
	v.SetDefault(p+"webhook.max_age", "24h")
	v.SetDefault(p+"webhook.max_retries", 10)

	// Chaintracks server defaults
	v.SetDefault(p+"chaintracks_server.enabled", true)

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
	BroadcastURLs []string      `mapstructure:"broadcast_urls"` // URLs for submitting transactions
	DataHubURLs   []string      `mapstructure:"datahub_urls"`   // URLs for fetching block/subtree data (fallback)
	AuthToken     string        `mapstructure:"auth_token"`
	Timeout       time.Duration `mapstructure:"timeout"`
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

// ChaintracksServerConfig holds configuration for the chaintracks HTTP API routes.
type ChaintracksServerConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// GetLogLevel returns the log level, defaulting to "info".
func (c *Config) GetLogLevel() string {
	if c.LogLevel != "" {
		return c.LogLevel
	}
	return "info"
}
