package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestEnvVarDataHubURLs(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		want     []string
	}{
		{
			name:     "single URL",
			envValue: "https://teranode-mainnet.taal.com/api/v1",
			want:     []string{"https://teranode-mainnet.taal.com/api/v1"},
		},
		{
			name:     "multiple URLs comma separated",
			envValue: "https://hub1.example.com/api/v1,https://hub2.example.com/api/v1,https://hub3.example.com/api/v1",
			want:     []string{"https://hub1.example.com/api/v1", "https://hub2.example.com/api/v1", "https://hub3.example.com/api/v1"},
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

			t.Setenv("ARCADE_TERANODE_DATAHUB_URLS", tt.envValue)

			if err := v.Unmarshal(cfg); err != nil {
				t.Fatalf("failed to unmarshal config: %v", err)
			}

			if len(cfg.Teranode.DataHubURLs) != len(tt.want) {
				t.Fatalf("got %d URLs, want %d: %v", len(cfg.Teranode.DataHubURLs), len(tt.want), cfg.Teranode.DataHubURLs)
			}

			for i, got := range cfg.Teranode.DataHubURLs {
				if got != tt.want[i] {
					t.Errorf("URL[%d] = %q, want %q", i, got, tt.want[i])
				}
			}
		})
	}
}
