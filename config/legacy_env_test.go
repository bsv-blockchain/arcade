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

// TestMigrationPinsBindFromEnv covers every pin that
// docs/migration-arcade-to-bsv-broadcaster.md and deploy/ set as an env var.
// A misspelled key is silently ignored, so the upgrade would quietly come up
// on the new defaults and fresh storage.
func TestMigrationPinsBindFromEnv(t *testing.T) {
	pins := map[string]string{
		"BSV_BROADCASTER_KAFKA_CONSUMER_GROUP":             "arcade",
		"BSV_BROADCASTER_KAFKA_TOPIC_PREFIX":               "arcade",
		"BSV_BROADCASTER_STORE_MONGODB_DATABASE":           "arcade",
		"BSV_BROADCASTER_STORE_AEROSPIKE_NAMESPACE":        "arcade",
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_USER":     "arcade",
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_PASSWORD": "arcade",
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_DATABASE": "arcade",
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_DATA_DIR": "/srv/arcade/postgres",
		"BSV_BROADCASTER_STORE_PEBBLE_PATH":                "/srv/arcade/pebble",
		"BSV_BROADCASTER_STORAGE_PATH":                     "/srv/arcade",
		"BSV_BROADCASTER_TELEMETRY_SERVICE_NAME":           "arcade",
	}
	for k, v := range pins {
		t.Setenv(k, v)
	}

	cfg, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := map[string]string{
		"BSV_BROADCASTER_KAFKA_CONSUMER_GROUP":             cfg.Kafka.ConsumerGroup,
		"BSV_BROADCASTER_KAFKA_TOPIC_PREFIX":               cfg.Kafka.TopicPrefix,
		"BSV_BROADCASTER_STORE_MONGODB_DATABASE":           cfg.Store.Mongo.Database,
		"BSV_BROADCASTER_STORE_AEROSPIKE_NAMESPACE":        cfg.Store.Aerospike.Namespace,
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_USER":     cfg.Store.Postgres.EmbeddedUser,
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_PASSWORD": cfg.Store.Postgres.EmbeddedPassword,
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_DATABASE": cfg.Store.Postgres.EmbeddedDatabase,
		"BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_DATA_DIR": cfg.Store.Postgres.EmbeddedDataDir,
		"BSV_BROADCASTER_STORE_PEBBLE_PATH":                cfg.Store.Pebble.Path,
		"BSV_BROADCASTER_STORAGE_PATH":                     cfg.StoragePath,
		"BSV_BROADCASTER_TELEMETRY_SERVICE_NAME":           cfg.Telemetry.ServiceName,
	}
	for k, want := range pins {
		if got[k] != want {
			t.Errorf("%s=%q bound as %q", k, want, got[k])
		}
	}
}
