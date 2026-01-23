package main

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"

	"github.com/bsv-blockchain/arcade/config"
)

// Load reads configuration from file and environment variables.
func Load() (*config.Config, error) {
	v := viper.New()

	// Set defaults (includes arcade, chaintracks, and p2p defaults)
	cfg := &config.Config{}
	cfg.SetDefaults(v, "")

	// Config file settings
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("$HOME/.arcade")
	v.AddConfigPath("/etc/arcade")

	// Environment variable settings
	v.SetEnvPrefix("ARCADE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return cfg, nil
}
