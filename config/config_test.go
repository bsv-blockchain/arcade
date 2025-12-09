package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefault(t *testing.T) {
	cfg := Default()

	home, _ := os.UserHomeDir()
	expectedDataDir := filepath.Join(home, ".arcade")

	assert.Equal(t, ":8080", cfg.Server.Address)
	assert.Equal(t, 30*time.Second, cfg.Server.ReadTimeout)
	assert.Equal(t, "sqlite", cfg.Database.Type)
	assert.Equal(t, filepath.Join(expectedDataDir, "arcade.db"), cfg.Database.SQLitePath)
	assert.Equal(t, "memory", cfg.Events.Type)
	assert.Equal(t, 1000, cfg.Events.BufferSize)
	assert.Equal(t, "arcade", cfg.P2P.ProcessName)
	assert.Equal(t, 9999, cfg.P2P.Port)
	assert.Equal(t, "main", cfg.P2P.Network)
	assert.Equal(t, expectedDataDir, cfg.P2P.StoragePath)
	assert.Equal(t, 4*1024*1024*1024, cfg.Validator.MaxTxSize)
	assert.Equal(t, uint64(50), cfg.Validator.MinFeePerKB)
}

func TestLoad_NoConfigFile(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, Default().Server.Address, cfg.Server.Address)
}

func TestLoad_WithYAMLFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test-config.yaml")

	yamlContent := `
server:
  address: ":9090"
  readTimeout: 45s

database:
  type: "postgres"
  postgresConnStr: "host=testhost port=5432"

teranode:
  baseURL: "http://teranode.example.com"
  timeout: 60s

p2p:
  processName: "arcade-test"
  port: 8888
  network: "test"

validator:
  minFeePerKB: 100
`

	err := os.WriteFile(configPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	cfg, err := Load(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":9090", cfg.Server.Address)
	assert.Equal(t, 45*time.Second, cfg.Server.ReadTimeout)
	assert.Equal(t, "postgres", cfg.Database.Type)
	assert.Equal(t, "host=testhost port=5432", cfg.Database.PostgresConnStr)
	assert.Equal(t, "http://teranode.example.com", cfg.Teranode.BaseURL)
	assert.Equal(t, 60*time.Second, cfg.Teranode.Timeout)
	assert.Equal(t, "arcade-test", cfg.P2P.ProcessName)
	assert.Equal(t, 8888, cfg.P2P.Port)
	assert.Equal(t, "test", cfg.P2P.Network)
	assert.Equal(t, uint64(100), cfg.Validator.MinFeePerKB)
}

func TestLoad_EnvironmentOverride(t *testing.T) {
	t.Setenv("ARCADE_SERVER_ADDRESS", ":7070")
	t.Setenv("ARCADE_DATABASE_TYPE", "postgres")
	t.Setenv("ARCADE_TERANODE_BASEURL", "http://env-teranode.example.com")
	t.Setenv("ARCADE_P2P_PORT", "7777")

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":7070", cfg.Server.Address)
	assert.Equal(t, "postgres", cfg.Database.Type)
	assert.Equal(t, "http://env-teranode.example.com", cfg.Teranode.BaseURL)
	assert.Equal(t, 7777, cfg.P2P.Port)
}

func TestLoad_FileAndEnvironment(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test-config.yaml")

	yamlContent := `
server:
  address: ":9090"

database:
  type: "sqlite"
  sqlitePath: "/tmp/arcade-test.db"
`

	err := os.WriteFile(configPath, []byte(yamlContent), 0644)
	require.NoError(t, err)

	t.Setenv("ARCADE_SERVER_ADDRESS", ":6060")
	t.Setenv("ARCADE_DATABASE_TYPE", "postgres")

	cfg, err := Load(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":6060", cfg.Server.Address, "Environment should override file")
	assert.Equal(t, "postgres", cfg.Database.Type, "Environment should override file")
}

func TestLoad_InvalidFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	assert.Error(t, err)
}

func TestLoad_MultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	config1Path := filepath.Join(tmpDir, "config1.yaml")
	config2Path := filepath.Join(tmpDir, "config2.yaml")

	yaml1 := `
server:
  address: ":9090"

database:
  type: "sqlite"
`

	yaml2 := `
server:
  readTimeout: 60s

p2p:
  port: 8888
`

	err := os.WriteFile(config1Path, []byte(yaml1), 0644)
	require.NoError(t, err)

	err = os.WriteFile(config2Path, []byte(yaml2), 0644)
	require.NoError(t, err)

	cfg, err := Load(config1Path, config2Path)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":9090", cfg.Server.Address, "From config1")
	assert.Equal(t, 60*time.Second, cfg.Server.ReadTimeout, "From config2")
	assert.Equal(t, "sqlite", cfg.Database.Type, "From config1")
	assert.Equal(t, 8888, cfg.P2P.Port, "From config2")
}

func TestLoad_JSONFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test-config.json")

	jsonContent := `{
  "server": {
    "address": ":5050"
  },
  "database": {
    "type": "postgres"
  }
}`

	err := os.WriteFile(configPath, []byte(jsonContent), 0644)
	require.NoError(t, err)

	cfg, err := Load(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":5050", cfg.Server.Address)
	assert.Equal(t, "postgres", cfg.Database.Type)
}
