package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	chaintracksconfig "github.com/bsv-blockchain/go-chaintracks/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ExpandHome rewrites a leading "~" or "~/" to the current user's home dir.
// Other paths (absolute, relative, or empty) are returned unchanged. Centralised
// so every config consumer sees real filesystem paths instead of literal tildes —
// libraries like cockroachdb/pebble don't expand "~" themselves and will happily
// create a directory named "~" in cwd.
func ExpandHome(path string) (string, error) {
	if path == "" || !strings.HasPrefix(path, "~") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, strings.TrimPrefix(path, "~")), nil
}

// Canonical network names accepted at the top-level `network` config key.
// These are the only values operators ever type; internal mapping to the
// go-teranode-p2p-client network identifiers lives in ResolveP2PNetwork.
const (
	NetworkMainnet     = "mainnet"
	NetworkTestnet     = "testnet"
	NetworkTeratestnet = "teratestnet"
)

// knownNetworks gates validate() and ResolveP2PNetwork.
var knownNetworks = map[string]struct{}{
	NetworkMainnet:     {},
	NetworkTestnet:     {},
	NetworkTeratestnet: {},
}

// ResolveP2PNetwork maps a canonical arcade network name to the values that
// go-teranode-p2p-client expects: the network identifier used to build pubsub
// topic names, and the bootstrap peer list to hand to libp2p.
//
// The upstream library's getDefaultBootstrapPeers returns bootstraps for the
// keys "main"/"test"/"stn" — its "stn" entry points at
// teratestnet.bootstrap.teranode.bsvb.tech while TopicName("stn", …) builds
// ".../stn-node_status". That mismatch is why configuring network=stn fails to
// see any teratestnet data hub URLs: the bootstrap DNS is right but the topic
// name is wrong. We sidestep it by handing the library the canonical topic
// network ("mainnet"/"testnet"/"teratestnet") and injecting the bootstrap peer
// list ourselves — so topic and peers always agree.
func ResolveP2PNetwork(network string) (topicNetwork string, bootstrapPeers []string) {
	switch network {
	case NetworkTestnet:
		return NetworkTestnet, []string{"/dnsaddr/testnet.bootstrap.teranode.bsvb.tech"}
	case NetworkTeratestnet:
		return NetworkTeratestnet, []string{"/dnsaddr/teratestnet.bootstrap.teranode.bsvb.tech"}
	case NetworkMainnet, "":
		fallthrough
	default:
		return NetworkMainnet, []string{"/dnsaddr/mainnet.bootstrap.teranode.bsvb.tech"}
	}
}

// ResolveChaintracksNetwork maps a canonical arcade network name to the value
// go-chaintracks accepts at config.P2P.Network. Its chainmanager.getGenesisHeader
// only knows "main"/"test"/"teratest"/"teratestnet", so we translate at the
// boundary instead of leaking upstream naming into the arcade config surface.
func ResolveChaintracksNetwork(network string) string {
	switch network {
	case NetworkTestnet:
		return "test"
	case NetworkTeratestnet:
		return NetworkTeratestnet
	case NetworkMainnet, "":
		fallthrough
	default:
		return "main"
	}
}

type Config struct {
	Mode          string              `mapstructure:"mode"`
	LogLevel      string              `mapstructure:"log_level"`
	CallbackURL   string              `mapstructure:"callback_url"`
	CallbackToken string              `mapstructure:"callback_token"`
	StoragePath   string              `mapstructure:"storage_path"`
	// Network selects the Bitcoin network everything downstream participates in.
	// Canonical values: "mainnet", "testnet", "teratestnet". Propagated to the
	// libp2p peer-discovery client and to the embedded chaintracks instance so
	// they agree on pubsub topic and bootstrap peers.
	Network       string              `mapstructure:"network"`
	APIServer     API                 `mapstructure:"api"`
	Kafka         Kafka               `mapstructure:"kafka"`
	Store         Store               `mapstructure:"store"`
	DatahubURLs   []string            `mapstructure:"datahub_urls"`
	Teranode      TeranodeConfig      `mapstructure:"teranode"`
	MerkleService MerkleServiceConfig `mapstructure:"merkle_service"`
	P2P           P2PConfig           `mapstructure:"p2p"`
	Health        HealthConfig        `mapstructure:"health"`
	Propagation   PropagationConfig   `mapstructure:"propagation"`
	TxValidator   TxValidatorConfig   `mapstructure:"tx_validator"`
	BumpBuilder   BumpBuilderConfig   `mapstructure:"bump_builder"`
	// ChaintracksServer gates whether the embedded go-chaintracks HTTP API
	// runs alongside api-server. Default is on so the refactor is a drop-in
	// replacement for the original single-binary arcade.
	ChaintracksServer ChaintracksServerConfig `mapstructure:"chaintracks_server"`
	// Chaintracks is the upstream go-chaintracks config; defaults delegated
	// to the library's own SetDefaults so new fields flow through automatically.
	Chaintracks chaintracksconfig.Config `mapstructure:"chaintracks"`
}

// ChaintracksServerConfig toggles the chaintracks HTTP surface. Separate from
// Chaintracks itself so the instance can be disabled without wiping the
// library's config block.
type ChaintracksServerConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type API struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

// Kafka configures the message broker. Backend picks between a real Sarama
// client (production, multi-node) and an in-process memory broker (single-
// binary standalone mode). Brokers is required only when Backend=sarama.
type Kafka struct {
	Backend       string   `mapstructure:"backend"` // "sarama" (default) or "memory"
	Brokers       []string `mapstructure:"brokers"`
	ConsumerGroup string   `mapstructure:"consumer_group"`
	MaxRetries    int      `mapstructure:"max_retries"`
	BufferSize    int      `mapstructure:"buffer_size"`
	// MinPartitions is the minimum number of partitions every hot-path topic
	// must have at startup. Set to the expected replica count of the largest
	// horizontally-scaled consumer (tx-validator or propagation) so arcade
	// fails fast when the cluster can't actually fan out across pods. Leave
	// at 0 or 1 in standalone/single-replica deployments.
	MinPartitions int `mapstructure:"min_partitions"`
}

// Store picks the persistence backend. Backend dispatches construction in
// storefactory.New; sub-blocks are read only when their backend is selected
// so operators don't need to fill in unused sections.
type Store struct {
	Backend   string   `mapstructure:"backend"` // "aerospike" (default), "pebble", or "postgres"
	Aerospike Aero     `mapstructure:"aerospike"`
	Pebble    Pebble   `mapstructure:"pebble"`
	Postgres  Postgres `mapstructure:"postgres"`
}

type Aero struct {
	Hosts           []string `mapstructure:"hosts"`
	Namespace       string   `mapstructure:"namespace"`
	BatchSize       int      `mapstructure:"batch_size"`
	PoolSize        int      `mapstructure:"pool_size"`
	QueryTimeoutMs  int      `mapstructure:"query_timeout_ms"`
	OpTimeoutMs     int      `mapstructure:"op_timeout_ms"`
	SocketTimeoutMs int      `mapstructure:"socket_timeout_ms"`
}

// Postgres configures the Postgres-backed store. Embedded=true spins up
// fergusstrange/embedded-postgres on Start, extracting the bundled Postgres
// binary into EmbeddedCacheDir on first run — so the binary is a one-time
// download, not per-start. DSN is used verbatim when Embedded=false; when
// Embedded=true it's constructed from EmbeddedPort/EmbeddedUser/EmbeddedPass
// and the standalone DataDir.
type Postgres struct {
	DSN              string `mapstructure:"dsn"`
	Embedded         bool   `mapstructure:"embedded"`
	EmbeddedPort     uint32 `mapstructure:"embedded_port"`
	EmbeddedUser     string `mapstructure:"embedded_user"`
	EmbeddedPassword string `mapstructure:"embedded_password"`
	EmbeddedDatabase string `mapstructure:"embedded_database"`
	EmbeddedDataDir  string `mapstructure:"embedded_data_dir"`
	EmbeddedCacheDir string `mapstructure:"embedded_cache_dir"`
	MaxConns         int32  `mapstructure:"max_conns"`
}

// Pebble configures the embedded Pebble KV backend. Path is the data directory
// (Pebble takes an exclusive file lock, so single-process only). MemTableSizeMB
// and L0CompactionThreshold are the two knobs operators usually want to tune;
// the rest of Pebble's defaults are fine for arcade's workload. SyncWrites trades
// durability of the last handful of writes for a ~10x throughput improvement —
// acceptable for standalone since the reaper will re-send unacked transactions.
type Pebble struct {
	Path                  string `mapstructure:"path"`
	MemTableSizeMB        int    `mapstructure:"memtable_size_mb"`
	L0CompactionThreshold int    `mapstructure:"l0_compaction_threshold"`
	SyncWrites            bool   `mapstructure:"sync_writes"`
}

type TeranodeConfig struct {
	AuthToken string `mapstructure:"auth_token"`
}

type MerkleServiceConfig struct {
	URL       string `mapstructure:"url"`
	AuthToken string `mapstructure:"auth_token"`
}

// P2PConfig controls the libp2p-based peer discovery service. Seeds is the
// legacy block-announcement seed list; DatahubDiscovery and its siblings gate
// the node_status subscription that auto-populates the propagation endpoint
// list. When DatahubDiscovery is false, no libp2p client is started and the
// rest of the P2P fields are ignored.
//
// The network is not set here — it comes from the top-level `network` config
// value so the p2p client and embedded chaintracks stay in sync. Operators only
// need BootstrapPeers here to override the canonical defaults resolved from
// ResolveP2PNetwork (useful for private networks or bootstrap migrations).
type P2PConfig struct {
	Seeds            []string `mapstructure:"seeds"`
	DatahubDiscovery bool     `mapstructure:"datahub_discovery"`
	ListenPort       int      `mapstructure:"listen_port"`
	BootstrapPeers   []string `mapstructure:"bootstrap_peers"`
	DHTMode          string   `mapstructure:"dht_mode"`
	StoragePath      string   `mapstructure:"storage_path"`
	EnableMDNS       bool     `mapstructure:"enable_mdns"`
	AllowPrivateURLs bool     `mapstructure:"allow_private_urls"`
}

type HealthConfig struct {
	Port int `mapstructure:"port"`
}

type PropagationConfig struct {
	MerkleConcurrency int `mapstructure:"merkle_concurrency"`
	RetryMaxAttempts  int `mapstructure:"retry_max_attempts"`
	RetryBackoffMs    int `mapstructure:"retry_backoff_ms"`
	ReaperIntervalMs  int `mapstructure:"reaper_interval_ms"`
	ReaperBatchSize   int `mapstructure:"reaper_batch_size"`
	// LeaseTTLMs bounds how long the reaper lease remains valid without a
	// renewal. Set to at least 2–3× reaper_interval_ms so a missed tick
	// doesn't trigger a false-positive failover. Defaults to 3× interval.
	LeaseTTLMs int `mapstructure:"lease_ttl_ms"`
	// TeranodeMaxBatchSize caps the number of transactions per POST /txs call.
	// Teranode rejects oversized batches with "too many transactions" (400),
	// which previously cascaded into a 1k+ per-tx fallback storm. Splitting
	// into chunks keeps the batch endpoint in play even under Kafka backlog.
	TeranodeMaxBatchSize int                  `mapstructure:"teranode_max_batch_size"`
	EndpointHealth       EndpointHealthConfig `mapstructure:"endpoint_health"`
}

// EndpointHealthConfig tunes the per-endpoint circuit-breaker in teranode.Client.
// FailureThreshold is the number of consecutive failures before an endpoint is
// marked unhealthy and excluded from broadcasts. ProbeIntervalMs is how often
// the recovery probe runs against the unhealthy set; ProbeTimeoutMs bounds each
// probe request. MinHealthyEndpoints is an advisory warning threshold — when
// the healthy count drops below it a single WARN is logged, but broadcasts are
// never blocked by this value.
//
// RefreshIntervalMs governs how often each pod's teranode.Client polls the
// shared store for newly registered datahub URLs (the cross-pod discovery
// path). Smaller values mean a fresh pod converges faster; larger values
// reduce store load. Zero or negative values fall back to the documented
// defaults at client construction time.
type EndpointHealthConfig struct {
	FailureThreshold    int `mapstructure:"failure_threshold"`
	ProbeIntervalMs     int `mapstructure:"probe_interval_ms"`
	ProbeTimeoutMs      int `mapstructure:"probe_timeout_ms"`
	MinHealthyEndpoints int `mapstructure:"min_healthy_endpoints"`
	RefreshIntervalMs   int `mapstructure:"refresh_interval_ms"`
}

// BumpBuilderConfig controls the BUMP construction workflow. GraceWindowMs is the
// delay applied after receiving BLOCK_PROCESSED before reading STUMPs from the store,
// giving merkle-service retries time to land for any STUMPs that initially got a 5xx.
type BumpBuilderConfig struct {
	GraceWindowMs int `mapstructure:"grace_window_ms"`
}

// TxValidatorConfig tunes the parallel batch validation pipeline. Parallelism
// caps how many transactions are parsed and validated concurrently inside a
// single flush window — bounded so a huge in-flight batch can't open more
// goroutines than the host has cores. Zero or negative falls back to
// runtime.NumCPU at construction time.
type TxValidatorConfig struct {
	Parallelism int `mapstructure:"parallelism"`
}

func BindFlags(cmd *cobra.Command) {
	cmd.Flags().String("mode", "all", "Service mode: all, api-server, bump-builder, tx-validator, propagation, p2p-client")
	cmd.Flags().String("config", "", "Path to config file")
	cmd.Flags().String("log-level", "info", "Log level: debug, info, warn, error")
	_ = viper.BindPFlag("mode", cmd.Flags().Lookup("mode"))
	_ = viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
}

func Load(cmd *cobra.Command) (*Config, error) {
	cfgFile, _ := cmd.Flags().GetString("config")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("/etc/arcade")
	}

	viper.SetEnvPrefix("ARCADE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := expandPaths(&cfg); err != nil {
		return nil, err
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// expandPaths resolves "~/..." in every filesystem path the config exposes,
// so downstream consumers (pebble, postgres, chaintracks, p2p) never see a
// literal tilde. New path fields should be added here.
func expandPaths(cfg *Config) error {
	for _, p := range []*string{
		&cfg.StoragePath,
		&cfg.Store.Pebble.Path,
		&cfg.Store.Postgres.EmbeddedDataDir,
		&cfg.Store.Postgres.EmbeddedCacheDir,
		&cfg.P2P.StoragePath,
		&cfg.Chaintracks.StoragePath,
	} {
		expanded, err := ExpandHome(*p)
		if err != nil {
			return fmt.Errorf("expanding path %q: %w", *p, err)
		}
		*p = expanded
	}
	return nil
}

func setDefaults() {
	viper.SetDefault("mode", "all")
	viper.SetDefault("log_level", "info")
	viper.SetDefault("api.host", "0.0.0.0")
	viper.SetDefault("api.port", 8080)
	viper.SetDefault("kafka.backend", "sarama")
	viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
	viper.SetDefault("kafka.consumer_group", "arcade")
	viper.SetDefault("kafka.max_retries", 5)
	viper.SetDefault("kafka.buffer_size", 10000)
	viper.SetDefault("store.backend", "aerospike")
	viper.SetDefault("store.aerospike.hosts", []string{"localhost:3000"})
	viper.SetDefault("store.aerospike.namespace", "arcade")
	viper.SetDefault("store.aerospike.batch_size", 500)
	viper.SetDefault("store.aerospike.pool_size", 256)
	viper.SetDefault("store.aerospike.query_timeout_ms", 8000)
	viper.SetDefault("store.aerospike.op_timeout_ms", 3000)
	viper.SetDefault("store.aerospike.socket_timeout_ms", 5000)
	viper.SetDefault("store.pebble.path", "~/.arcade/pebble")
	viper.SetDefault("store.pebble.memtable_size_mb", 64)
	viper.SetDefault("store.pebble.l0_compaction_threshold", 4)
	viper.SetDefault("store.pebble.sync_writes", false)
	viper.SetDefault("store.postgres.embedded", false)
	viper.SetDefault("store.postgres.embedded_port", 0)
	viper.SetDefault("store.postgres.embedded_user", "arcade")
	viper.SetDefault("store.postgres.embedded_password", "arcade")
	viper.SetDefault("store.postgres.embedded_database", "arcade")
	viper.SetDefault("store.postgres.embedded_data_dir", "~/.arcade/postgres")
	viper.SetDefault("store.postgres.embedded_cache_dir", "~/.arcade/postgres-cache")
	viper.SetDefault("store.postgres.max_conns", 16)
	viper.SetDefault("health.port", 8081)
	viper.SetDefault("propagation.merkle_concurrency", 10)
	viper.SetDefault("propagation.retry_max_attempts", 5)
	viper.SetDefault("propagation.retry_backoff_ms", 500)
	viper.SetDefault("propagation.reaper_interval_ms", 30000)
	viper.SetDefault("propagation.reaper_batch_size", 500)
	// 0 keeps New()'s 3×reaper_interval default, so changing reaper_interval
	// automatically moves the lease TTL unless the operator opts into a fixed value.
	viper.SetDefault("propagation.lease_ttl_ms", 0)
	viper.SetDefault("propagation.teranode_max_batch_size", 100)
	viper.SetDefault("propagation.endpoint_health.failure_threshold", 3)
	viper.SetDefault("propagation.endpoint_health.probe_interval_ms", 30000)
	viper.SetDefault("propagation.endpoint_health.probe_timeout_ms", 2000)
	viper.SetDefault("propagation.endpoint_health.min_healthy_endpoints", 0)
	viper.SetDefault("propagation.endpoint_health.refresh_interval_ms", 30000)

	viper.SetDefault("network", NetworkMainnet)

	viper.SetDefault("p2p.datahub_discovery", false)
	viper.SetDefault("p2p.listen_port", 9905)
	viper.SetDefault("p2p.dht_mode", "off")
	viper.SetDefault("p2p.enable_mdns", false)
	viper.SetDefault("p2p.allow_private_urls", false)

	viper.SetDefault("storage_path", "~/.arcade")
	viper.SetDefault("chaintracks_server.enabled", true)
	// Delegate chaintracks-library defaults (mode, network, bootstrap, p2p, …)
	// to the upstream SetDefaults so any new fields are picked up automatically.
	var ct chaintracksconfig.Config
	ct.SetDefaults(viper.GetViper(), "chaintracks")
	viper.SetDefault("bump_builder.grace_window_ms", 30000)
}

func validate(cfg *Config) error {
	switch cfg.Kafka.Backend {
	case "", "sarama":
		if len(cfg.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka.brokers is required when kafka.backend=sarama")
		}
	case "memory":
		// no external config required
	default:
		return fmt.Errorf("unknown kafka.backend %q (expected sarama or memory)", cfg.Kafka.Backend)
	}
	switch cfg.Store.Backend {
	case "", "aerospike":
		if len(cfg.Store.Aerospike.Hosts) == 0 {
			return fmt.Errorf("store.aerospike.hosts is required when store.backend=aerospike")
		}
	case "pebble":
		if cfg.Store.Pebble.Path == "" {
			return fmt.Errorf("store.pebble.path is required when store.backend=pebble")
		}
	case "postgres":
		if !cfg.Store.Postgres.Embedded && cfg.Store.Postgres.DSN == "" {
			return fmt.Errorf("store.postgres.dsn is required when store.backend=postgres and postgres.embedded=false")
		}
	default:
		return fmt.Errorf("unknown store.backend %q (expected aerospike, pebble, or postgres)", cfg.Store.Backend)
	}
	if cfg.Network == "" {
		cfg.Network = NetworkMainnet
	}
	if _, ok := knownNetworks[cfg.Network]; !ok {
		return fmt.Errorf("invalid network %q (expected %s, %s, or %s)",
			cfg.Network, NetworkMainnet, NetworkTestnet, NetworkTeratestnet)
	}
	validModes := map[string]bool{
		"all": true, "api-server": true,
		"bump-builder": true,
		"tx-validator": true, "propagation": true,
		"p2p-client": true,
	}
	if !validModes[cfg.Mode] {
		return fmt.Errorf("invalid mode %q", cfg.Mode)
	}
	return nil
}
