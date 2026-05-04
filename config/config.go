package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	chaintracksconfig "github.com/bsv-blockchain/go-chaintracks/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ExpandHome rewrites a leading "~" or "~/" to the current user's home dir.
// Other paths (absolute, relative, or empty) are returned unchanged. Centralized
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
	NetworkRegtest     = "regtest"
)

// knownNetworks gates validate() and ResolveP2PNetwork.
var knownNetworks = map[string]struct{}{
	NetworkMainnet:     {},
	NetworkTestnet:     {},
	NetworkTeratestnet: {},
	NetworkRegtest:     {},
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
//
// Regtest deliberately returns a nil bootstrap list: there is no canonical
// regtest DNS, so operators must supply p2p.bootstrap_peers themselves.
// validate() enforces this when datahub_discovery is enabled.
func ResolveP2PNetwork(network string) (topicNetwork string, bootstrapPeers []string) {
	switch network {
	case NetworkTestnet:
		return NetworkTestnet, []string{"/dnsaddr/testnet.bootstrap.teranode.bsvb.tech"}
	case NetworkTeratestnet:
		return NetworkTeratestnet, []string{"/dnsaddr/teratestnet.bootstrap.teranode.bsvb.tech"}
	case NetworkRegtest:
		return NetworkRegtest, nil
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
//
// Regtest is intentionally absent: chaintracks has no regtest genesis header,
// so validate() force-disables chaintracks_server when network=regtest and this
// function is never reached with that value.
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
	Mode          string `mapstructure:"mode"`
	LogLevel      string `mapstructure:"log_level"`
	CallbackURL   string `mapstructure:"callback_url"`
	CallbackToken string `mapstructure:"callback_token"`
	StoragePath   string `mapstructure:"storage_path"`
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
	Webhook       WebhookConfig       `mapstructure:"webhook"`
	Callback      CallbackConfig      `mapstructure:"callback"`
	Events        EventsConfig        `mapstructure:"events"`
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
	// IdleTimeoutSec sets ClientPolicy.IdleTimeout — client-side reap of idle
	// pooled connections. Required when the Aerospike server runs with
	// proto-fd-idle-ms=0. Set a few seconds below the server value when
	// nonzero. Default 55.
	IdleTimeoutSec int `mapstructure:"idle_timeout_sec"`
	// MaxErrorRate is the threshold for the per-node circuit breaker — once
	// a node returns this many errors within an ErrorRateWindow tend cycles,
	// requests fast-fail with MAX_ERROR_RATE instead of timing out and
	// holding pool slots. Default 5; the Aerospike default of 100 is too
	// lenient for a multi-tenant cluster.
	MaxErrorRate int `mapstructure:"max_error_rate"`
	// ErrorRateWindow is the number of cluster-tend iterations the breaker
	// observes before resetting. Default 1 (~1 second).
	ErrorRateWindow int `mapstructure:"error_rate_window"`
	// BumpChunkSizeBytes is the target chunk size used by InsertBUMP when
	// splitting a compound BUMP across multiple Aerospike records. A single
	// Aerospike record cannot exceed the namespace's write-block-size (default
	// 1 MiB, hard ceiling 8 MiB), so very large BUMPs from scaling networks
	// must be chunked. Default 768 KiB stays safely under the 1 MiB default
	// write-block-size; operators who raise write-block-size to 8 MiB can
	// raise this to ~6 MiB to reduce chunk fan-out for huge BUMPs.
	BumpChunkSizeBytes int `mapstructure:"bump_chunk_size_bytes"`
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
	// DataHubMaxBlockBytes caps a single /block/<hash> response body fetched
	// from a DataHub endpoint, in bytes. The DataHub serves block metadata
	// (header + subtree-hash list + coinbase tx + coinbase BUMP), so the
	// default of 1 GiB is two-plus orders of magnitude over a realistic
	// Teranode payload while still bounding memory against a hostile or
	// malfunctioning endpoint. A value <= 0 selects bump.DefaultMaxBlockBytes.
	// Mitigates F-007 (DataHub block fetch reads unbounded response bodies
	// into memory).
	DataHubMaxBlockBytes int64 `mapstructure:"datahub_max_block_bytes"`
}

// WebhookConfig tunes the HTTP webhook delivery service. The service
// subscribes to status updates and POSTs them to each submission's
// CallbackURL; failures are retried with exponential backoff persisted via
// the store's UpdateDeliveryStatus.
type WebhookConfig struct {
	// MaxRetries caps how many times a failed POST is re-attempted before
	// the submission is given up on. Mirrors arc's default of 10.
	MaxRetries int `mapstructure:"max_retries"`
	// ExpirationMinutes bounds the total wall-clock lifetime of a webhook
	// delivery. Past this point the service stops retrying even if
	// MaxRetries hasn't been hit. Defaults to 24 hours.
	ExpirationMinutes int `mapstructure:"expiration_minutes"`
	// InitialBackoffMs is the first retry delay; subsequent retries double
	// it (capped). Defaults to 5s, matching arc.
	InitialBackoffMs int `mapstructure:"initial_backoff_ms"`
	// MaxBackoffMs caps how long backoff can grow between retries. Default
	// 5 minutes.
	MaxBackoffMs int `mapstructure:"max_backoff_ms"`
	// HTTPTimeoutMs caps how long a single POST attempt may run. Default
	// 10s — webhook receivers should ack fast or risk being timed out.
	HTTPTimeoutMs int `mapstructure:"http_timeout_ms"`
	// MaxConcurrentDeliveries bounds the worker pool that fans status
	// updates out to callback URLs. The service's channel reader hands
	// each status to the pool and immediately returns to draining the
	// upstream events.Publisher channel — without this decoupling, a
	// single slow callback target (synchronous http.Client.Do up to
	// HTTPTimeoutMs) would block the channel reader and cause the
	// publisher to drop subsequent events as the in-memory buffer
	// filled. Default 32: comfortably above expected concurrent slow
	// callbacks while staying well under the per-pod TCP/connection
	// budget. Increase only if pool saturation
	// (arcade_webhook_pool_saturated_total) shows non-trivial drop rate.
	MaxConcurrentDeliveries int `mapstructure:"max_concurrent_deliveries"`
}

// CallbackConfig governs the SSRF guard that protects api-server's
// X-CallbackUrl registration and the webhook delivery client's outbound
// dials. Both layers share the same knob so an operator who opts in
// for internal-network callbacks doesn't have to remember to flip a
// matching flag elsewhere. See finding F-017 / issue #75.
type CallbackConfig struct {
	// AllowPrivateIPs, when true, disables the SSRF guard. Default false:
	// X-CallbackUrl values whose host parses as a loopback / link-local /
	// metadata / RFC1918 IP are rejected at submit time, and the webhook
	// delivery http.Client refuses to dial those IPs at connect time.
	// Operators running purely against internal services (testing rigs,
	// k8s service DNS, intranet webhooks) can set this true.
	AllowPrivateIPs bool `mapstructure:"allow_private_ips"`
	// MaxBodyBytes caps the size of an inbound POST
	// /api/v1/merkle-service/callback body, in bytes. The callback receives
	// JSON with embedded STUMP payloads (subtree merkle paths) that can be
	// genuinely large for busy subtrees, but unbounded reads let a malicious
	// or malfunctioning peer exhaust memory. Default 16 MiB is well over a
	// realistic STUMP delivery (~hundreds of KiB even for the largest
	// subtrees observed in production) while still bounding worst-case
	// memory use per request. A value <= 0 selects DefaultCallbackMaxBodyBytes.
	// Mitigates F-019 (callback JSON bodies and STUMP payloads are unbounded).
	MaxBodyBytes int64 `mapstructure:"max_body_bytes"`
}

// DefaultCallbackMaxBodyBytes is the fallback value for
// CallbackConfig.MaxBodyBytes when an operator leaves it unset (or sets a
// non-positive value). 16 MiB is generous enough for the largest realistic
// STUMP payload while still bounding memory against a hostile peer; see
// F-019 for the threat model.
const DefaultCallbackMaxBodyBytes int64 = 16 << 20

// EventsConfig tunes the in-process events.Publisher. SubscriberBuffer is
// the channel capacity each Subscribe call mints — when a downstream
// consumer (SSE manager, webhook delivery) can't drain fast enough the
// publisher logs "subscriber channel full, dropping update" and drops.
// Raising the buffer absorbs larger bursts without dropping; the practical
// ceiling is ~65536 (beyond that you're masking a slow consumer rather
// than absorbing a spike). Each Subscribe gets its own buffer, so total
// memory scales with subscribers × SubscriberBuffer × sizeof(*TransactionStatus).
type EventsConfig struct {
	SubscriberBuffer int `mapstructure:"subscriber_buffer"`
}

// DefaultEventsSubscriberBuffer is the fallback channel capacity for each
// events.Publisher.Subscribe channel when SubscriberBuffer is unset or
// non-positive. 4096 is 16× the original 256 — enough headroom for typical
// status-update bursts without committing significant memory upfront.
const DefaultEventsSubscriberBuffer = 4096

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
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
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

	viper.SetDefault("webhook.max_retries", 10)
	viper.SetDefault("webhook.expiration_minutes", 60*24)
	viper.SetDefault("webhook.initial_backoff_ms", 5000)
	viper.SetDefault("webhook.max_backoff_ms", 300000)
	viper.SetDefault("webhook.http_timeout_ms", 10000)
	viper.SetDefault("webhook.max_concurrent_deliveries", 32)

	// Callback SSRF guard defaults to enabled (allow_private_ips=false). See
	// finding F-017 / issue #75 — accepting any X-CallbackUrl turned arcade
	// into a blind SSRF primitive against internal services and cloud
	// metadata endpoints.
	viper.SetDefault("callback.allow_private_ips", false)
	// Inbound callback body cap. 16 MiB headroom over realistic STUMP payloads
	// while bounding memory against a hostile or malfunctioning peer (F-019).
	viper.SetDefault("callback.max_body_bytes", DefaultCallbackMaxBodyBytes)

	// Per-subscription channel capacity for events.Publisher. 4096 is 16× the
	// original 256 — enough to absorb status-update bursts without committing
	// significant memory upfront. Operators seeing "subscriber channel full,
	// dropping update" warnings can raise this up to ~65536.
	viper.SetDefault("events.subscriber_buffer", DefaultEventsSubscriberBuffer)

	viper.SetDefault("storage_path", "~/.arcade")
	viper.SetDefault("chaintracks_server.enabled", true)
	// Delegate chaintracks-library defaults (mode, network, bootstrap, p2p, …)
	// to the upstream SetDefaults so any new fields are picked up automatically.
	var ct chaintracksconfig.Config
	ct.SetDefaults(viper.GetViper(), "chaintracks")
	viper.SetDefault("bump_builder.grace_window_ms", 30000)
	// 1 GiB — DataHub /block/<hash> responses contain block metadata
	// (header + subtree hashes + coinbase tx + coinbase BUMP). 1 GiB is
	// two-plus orders of magnitude of headroom over a realistic payload
	// while still bounding memory against a hostile DataHub. See F-007.
	viper.SetDefault("bump_builder.datahub_max_block_bytes", int64(1*1024*1024*1024))
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
	// merkle_service.url is intentionally optional: an empty value means the
	// Merkle integration is disabled. The runtime treats URL-presence as the
	// toggle — cmd/arcade/main.go only constructs a merkleservice.Client when
	// the URL is set, and propagation.Propagator nil-guards every dereference
	// of the client. The documented standalone and zero-dependency profiles
	// (config.example.standalone.yaml) ship with merkle_service.url: "" for
	// exactly this reason. See issue #59 / finding F-001.
	//
	// When the Merkle integration IS enabled (URL set), callback_token is
	// mandatory. The /api/v1/merkle-service/callback endpoint accepts forged
	// status updates for any txid in the system if it runs without bearer-token
	// auth, so we fail-closed here at config load rather than silently exposing
	// the unauthenticated receiver. See issue #76 / finding F-018.
	//
	// This same check now also gates the OUTBOUND /watch token forwarding:
	// merkleservice.Client.Register/RegisterBatch propagate cfg.CallbackToken
	// to merkle-service so it can attach `Authorization: Bearer <token>` on
	// callbacks. Without a configured token there's nothing to forward AND the
	// inbound receiver would 401 anyway — the same fail-closed posture covers
	// both ends, so a duplicate "outbound token required" check is unnecessary.
	if cfg.MerkleService.URL != "" && cfg.CallbackToken == "" {
		return fmt.Errorf("callback_token is required when merkle_service.url is set " +
			"(unauthenticated /api/v1/merkle-service/callback would accept forged callbacks; see issue #76)")
	}
	if cfg.Network == "" {
		cfg.Network = NetworkMainnet
	}
	if _, ok := knownNetworks[cfg.Network]; !ok {
		return fmt.Errorf("invalid network %q (expected %s, %s, %s, or %s)",
			cfg.Network, NetworkMainnet, NetworkTestnet, NetworkTeratestnet, NetworkRegtest)
	}
	if cfg.Network == NetworkRegtest {
		// chaintracks has no regtest genesis header — initializing it would
		// crash with ErrUnknownNetwork. Force-disable so operators only need to
		// set network: regtest without also remembering chaintracks_server.
		cfg.ChaintracksServer.Enabled = false
		if cfg.P2P.DatahubDiscovery && len(cfg.P2P.BootstrapPeers) == 0 {
			return fmt.Errorf("p2p.bootstrap_peers is required when network=regtest and p2p.datahub_discovery=true")
		}
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
