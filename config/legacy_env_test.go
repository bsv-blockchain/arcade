package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// setLegacyEnv sets name via t.Setenv and also arranges for the
// BSV_BROADCASTER_* twin that applyLegacyEnv creates with os.Setenv to be
// removed, so the mapping never leaks into other tests.
func setLegacyEnv(t *testing.T, name, value string) {
	t.Helper()
	t.Setenv(name, value)
	twin := EnvPrefix + strings.TrimPrefix(name, LegacyEnvPrefix)
	if _, set := os.LookupEnv(twin); !set {
		t.Cleanup(func() { _ = os.Unsetenv(twin) })
	}
}

func TestApplyLegacyEnv_MapsUnsetNewNames(t *testing.T) {
	setLegacyEnv(t, "ARCADE_LOG_LEVEL", "debug")
	setLegacyEnv(t, "ARCADE_KAFKA_TOPIC_PREFIX", "arcade")

	mapped := applyLegacyEnv()

	if got := os.Getenv("BSV_BROADCASTER_LOG_LEVEL"); got != "debug" {
		t.Errorf("BSV_BROADCASTER_LOG_LEVEL = %q, want %q", got, "debug")
	}
	if got := os.Getenv("BSV_BROADCASTER_KAFKA_TOPIC_PREFIX"); got != "arcade" {
		t.Errorf("BSV_BROADCASTER_KAFKA_TOPIC_PREFIX = %q, want %q", got, "arcade")
	}
	want := []string{"ARCADE_KAFKA_TOPIC_PREFIX", "ARCADE_LOG_LEVEL"}
	if strings.Join(mapped, ",") != strings.Join(want, ",") {
		t.Errorf("mapped = %v, want %v", mapped, want)
	}
}

func TestApplyLegacyEnv_NewNameWins(t *testing.T) {
	t.Setenv("BSV_BROADCASTER_LOG_LEVEL", "warn")
	setLegacyEnv(t, "ARCADE_LOG_LEVEL", "debug")

	mapped := applyLegacyEnv()

	if got := os.Getenv("BSV_BROADCASTER_LOG_LEVEL"); got != "warn" {
		t.Errorf("BSV_BROADCASTER_LOG_LEVEL = %q, want the explicitly set %q", got, "warn")
	}
	for _, name := range mapped {
		if name == "ARCADE_LOG_LEVEL" {
			t.Errorf("ARCADE_LOG_LEVEL reported as mapped although the new name was already set")
		}
	}
}

// TestLoad_HonorsLegacyEnv runs the real Load path end to end: a deployment
// still configured with ARCADE_* variables gets those values and a
// deprecation notice naming them.
func TestLoad_HonorsLegacyEnv(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	setLegacyEnv(t, "ARCADE_KAFKA_TOPIC_PREFIX", "arcade")
	setLegacyEnv(t, "ARCADE_KAFKA_CONSUMER_GROUP", "arcade")

	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgFile, []byte("kafka:\n  backend: memory\nstore:\n  backend: pebble\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{}
	BindFlags(cmd)
	if err := cmd.Flags().Set("config", cfgFile); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cmd)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Kafka.TopicPrefix != "arcade" {
		t.Errorf("Kafka.TopicPrefix = %q, want %q", cfg.Kafka.TopicPrefix, "arcade")
	}
	if cfg.Kafka.ConsumerGroup != "arcade" {
		t.Errorf("Kafka.ConsumerGroup = %q, want %q", cfg.Kafka.ConsumerGroup, "arcade")
	}
	if len(cfg.Deprecations) != 1 || !strings.Contains(cfg.Deprecations[0], "ARCADE_KAFKA_TOPIC_PREFIX") {
		t.Errorf("Deprecations = %v, want one notice naming ARCADE_KAFKA_TOPIC_PREFIX", cfg.Deprecations)
	}
}

func TestValidate_RejectsBadTopicPrefix(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Kafka.TopicPrefix = "has space"
	if err := validate(cfg); err == nil {
		t.Fatal("validate accepted kafka.topic_prefix with a space")
	}
}
