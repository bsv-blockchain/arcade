package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func loadEndpointHealthDefaults(t *testing.T) EndpointHealthConfig {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetEnvPrefix("ARCADE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	setDefaults()

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return cfg.Propagation.EndpointHealth
}

func TestEndpointHealthDiscoveredTTLDefault(t *testing.T) {
	if got := loadEndpointHealthDefaults(t).DiscoveredTTLMs; got != DefaultEndpointHealthDiscoveredTTLMs {
		t.Fatalf("DiscoveredTTLMs default = %d, want %d", got, DefaultEndpointHealthDiscoveredTTLMs)
	}
}

func TestEndpointHealthDiscoveredTTLEnvOverride(t *testing.T) {
	t.Setenv("ARCADE_PROPAGATION_ENDPOINT_HEALTH_DISCOVERED_TTL_MS", "600000")

	if got := loadEndpointHealthDefaults(t).DiscoveredTTLMs; got != 600000 {
		t.Fatalf("DiscoveredTTLMs = %d, want 600000", got)
	}
}
