package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

// Config holds all application configuration
type Config struct {
	Server        ServerConfig
	Database      DatabaseConfig
	Events        EventsConfig
	Teranode      TeranodeConfig
	P2P           P2PConfig
	Validator     ValidatorConfig
	Rebroadcaster RebroadcasterConfig
	Auth          AuthConfig
	Webhook       WebhookConfig
}

// ServerConfig holds HTTP API server configuration
type ServerConfig struct {
	Address         string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Type             string // "sqlite" or "postgres"
	SQLitePath       string
	PostgresConnStr  string
}

// EventsConfig holds event publisher configuration
type EventsConfig struct {
	Type        string // "memory" or "redis"
	BufferSize  int
	RedisURL    string
}

// TeranodeConfig holds teranode client configuration
type TeranodeConfig struct {
	BaseURL  string
	BaseURLs []string
	Timeout  time.Duration
}

// P2PConfig holds libp2p subscriber configuration
type P2PConfig struct {
	ProcessName    string
	Port           int
	BootstrapPeers []string
	PrivateKey     string
	TopicPrefix    string
	PeerCacheFile  string
}

// ValidatorConfig holds transaction validator configuration
type ValidatorConfig struct {
	MaxTxSize     int
	MaxScriptSize int
	MaxSigOps     int64
	MinFeePerKB   uint64
}

// RebroadcasterConfig holds rebroadcaster configuration
type RebroadcasterConfig struct {
	FrequentInterval     time.Duration
	FrequentMinAge       time.Duration
	FrequentMaxRetries   int
	BlockBasedMaxRetries int
	BatchSize            int
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	Enabled bool
	Token   string
}

// WebhookConfig holds webhook handler configuration
type WebhookConfig struct {
	PruneInterval time.Duration
	MaxAge        time.Duration
	MaxRetries    int
}

// Default returns default configuration
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Address:         ":8080",
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    30 * time.Second,
			ShutdownTimeout: 10 * time.Second,
		},
		Database: DatabaseConfig{
			Type:       "sqlite",
			SQLitePath: "./arcade.db",
		},
		Events: EventsConfig{
			Type:       "memory",
			BufferSize: 1000,
		},
		Teranode: TeranodeConfig{
			BaseURL:  "http://localhost:8080",
			BaseURLs: []string{},
			Timeout:  30 * time.Second,
		},
		P2P: P2PConfig{
			ProcessName:    "arcade",
			Port:           9999,
			BootstrapPeers: []string{},
			PrivateKey:     "",
			TopicPrefix:    "mainnet",
			PeerCacheFile:  "peer_cache.json",
		},
		Validator: ValidatorConfig{
			MaxTxSize:     4 * 1024 * 1024 * 1024, // 4 GB
			MaxScriptSize: 500000,
			MaxSigOps:     4294967295,
			MinFeePerKB:   50,
		},
		Rebroadcaster: RebroadcasterConfig{
			FrequentInterval:     1 * time.Minute,
			FrequentMinAge:       30 * time.Second,
			FrequentMaxRetries:   20,
			BlockBasedMaxRetries: 100,
			BatchSize:            100,
		},
		Auth: AuthConfig{
			Enabled: false,
		},
		Webhook: WebhookConfig{
			PruneInterval: 1 * time.Hour,
			MaxAge:        24 * time.Hour,
			MaxRetries:    10,
		},
	}
}

// Load loads configuration from file and environment variables
func Load(configFiles ...string) (*Config, error) {
	cfg := Default()

	if err := setDefaults(cfg); err != nil {
		return nil, fmt.Errorf("failed to set defaults: %w", err)
	}

	if err := overrideWithFiles(configFiles...); err != nil {
		return nil, fmt.Errorf("failed to load config files: %w", err)
	}

	viper.SetEnvPrefix("ARCADE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}

// setDefaults converts the default config into viper defaults
func setDefaults(defaultConfig *Config) error {
	defaultsMap := make(map[string]interface{})

	if err := mapstructure.Decode(defaultConfig, &defaultsMap); err != nil {
		return errors.Join(errors.New("failed to decode defaults"), err)
	}

	for key, value := range defaultsMap {
		viper.SetDefault(key, value)
	}

	return nil
}

// overrideWithFiles loads and merges config files
func overrideWithFiles(files ...string) error {
	if len(files) == 0 || files[0] == "" {
		return nil
	}

	for _, f := range files {
		viper.SetConfigFile(f)
		err := viper.MergeInConfig()
		if err != nil {
			return errors.Join(errors.New("config file error"), err)
		}
	}

	return nil
}
