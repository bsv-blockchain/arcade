package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestEnvVarBroadcastURLs(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		want     []string
	}{
		{
			name:     "single URL",
			envValue: "https://arc.taal.com",
			want:     []string{"https://arc.taal.com"},
		},
		{
			name:     "multiple URLs comma separated",
			envValue: "https://arc.taal.com,https://arc2.taal.com,https://arc3.taal.com",
			want:     []string{"https://arc.taal.com", "https://arc2.taal.com", "https://arc3.taal.com"},
		},
		{
			name:     "empty value",
			envValue: "",
			want:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			cfg := &Config{}
			cfg.SetDefaults(v, "")

			v.SetEnvPrefix("ARCADE")
			v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
			v.AutomaticEnv()

			t.Setenv("ARCADE_TERANODE_BROADCAST_URLS", tt.envValue)

			if err := v.Unmarshal(cfg); err != nil {
				t.Fatalf("failed to unmarshal config: %v", err)
			}

			if len(cfg.Teranode.BroadcastURLs) != len(tt.want) {
				t.Fatalf("got %d URLs, want %d: %v", len(cfg.Teranode.BroadcastURLs), len(tt.want), cfg.Teranode.BroadcastURLs)
			}

			for i, got := range cfg.Teranode.BroadcastURLs {
				if got != tt.want[i] {
					t.Errorf("URL[%d] = %q, want %q", i, got, tt.want[i])
				}
			}
		})
	}
}

func TestEnvVarDataHubURLs(t *testing.T) {
	v := viper.New()
	cfg := &Config{}
	cfg.SetDefaults(v, "")

	v.SetEnvPrefix("ARCADE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	t.Setenv("ARCADE_TERANODE_DATAHUB_URLS", "https://hub1.example.com,https://hub2.example.com")

	if err := v.Unmarshal(cfg); err != nil {
		t.Fatalf("failed to unmarshal config: %v", err)
	}

	if len(cfg.Teranode.DataHubURLs) != 2 {
		t.Fatalf("got %d URLs, want 2: %v", len(cfg.Teranode.DataHubURLs), cfg.Teranode.DataHubURLs)
	}

	if cfg.Teranode.DataHubURLs[0] != "https://hub1.example.com" {
		t.Errorf("DataHubURLs[0] = %q, want %q", cfg.Teranode.DataHubURLs[0], "https://hub1.example.com")
	}

	if cfg.Teranode.DataHubURLs[1] != "https://hub2.example.com" {
		t.Errorf("DataHubURLs[1] = %q, want %q", cfg.Teranode.DataHubURLs[1], "https://hub2.example.com")
	}
}
