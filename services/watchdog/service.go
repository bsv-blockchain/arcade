package watchdog

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/store"
)

// Service is the services.Service-conforming wrapper around Watchdog so
// the arcade supervisor can manage it like any other mode. The actual
// recovery logic lives in Watchdog.Run; this layer is purely lifecycle
// plumbing.
type Service struct {
	wd     *Watchdog
	logger *zap.Logger
	cfg    config.WatchdogConfig
}

// NewService constructs a Service from the operator-facing config.
// Returns nil when the merkle-service client or leaser is not wired (no
// /reprocess target or no cluster coordination) — callers should treat a
// nil Service as "don't run the watchdog in this deployment" rather than
// an error.
func NewService(
	cfg *config.Config,
	logger *zap.Logger,
	st store.Store,
	leaser store.Leaser,
	mc *merkleservice.Client,
) *Service {
	if mc == nil || leaser == nil {
		// Defensive: app.BuildServices already gates this, but a misconfig
		// (merkle_service.url unset) should produce a no-op Service rather
		// than a nil-deref at the first tick.
		return nil
	}
	wcfg := ResolveConfig(cfg.Watchdog)
	wd := New(st, leaser, mc, cfg.CallbackURL, cfg.CallbackToken, wcfg, logger)
	return &Service{wd: wd, logger: logger.Named("watchdog"), cfg: cfg.Watchdog}
}

// Name implements services.Service.
func (s *Service) Name() string { return "watchdog" }

// Start blocks until ctx is canceled. Returns nil on graceful shutdown —
// the watchdog has no terminal failure mode; lease loss, store errors,
// and /reprocess failures are all per-tick conditions that the run loop
// recovers from on the next tick.
func (s *Service) Start(ctx context.Context) error {
	s.logger.Info(
		"starting watchdog service",
		zap.Duration("interval", s.wd.cfg.Interval),
		zap.Duration("stale_threshold", s.wd.cfg.StaleThreshold),
		zap.Uint64("recency_depth", s.wd.cfg.RecencyDepth),
	)
	s.wd.Run(ctx)
	return nil
}

// Stop is a no-op — Start's ctx cancellation drives shutdown; the
// supervisor passes the same ctx to both methods.
func (s *Service) Stop() error { return nil }

// ResolveConfig maps the operator-facing config.WatchdogConfig (ms units,
// raw ints) onto the internal Config (Durations). Zero values for any
// duration field fall back to documented defaults so a partially
// specified YAML still produces a sensible runtime.
func ResolveConfig(c config.WatchdogConfig) Config {
	interval := time.Duration(c.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 30 * time.Second
	}
	stale := time.Duration(c.StaleThresholdMs) * time.Millisecond
	if stale <= 0 {
		stale = 2 * time.Minute
	}
	var recency uint64
	if c.RecencyDepth > 0 {
		recency = uint64(c.RecencyDepth)
	} else {
		recency = 144
	}
	batch := c.BatchSize
	if batch <= 0 {
		batch = 100
	}
	maxConc := c.MaxConcurrent
	if maxConc <= 0 {
		maxConc = 4
	}
	leaseTTL := time.Duration(c.LeaseTTLMs) * time.Millisecond
	if leaseTTL <= 0 {
		leaseTTL = 3 * interval
	}
	initBackoff := time.Duration(c.InitialBackoffMs) * time.Millisecond
	if initBackoff <= 0 {
		initBackoff = time.Minute
	}
	maxBackoff := time.Duration(c.MaxBackoffMs) * time.Millisecond
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Minute
	}
	terminal := time.Duration(c.TerminalBackoffMs) * time.Millisecond
	if terminal <= 0 {
		terminal = 4 * time.Hour
	}
	// Pass the cap knobs through directly: a value <= 0 means "disabled", so
	// (unlike the backoff fields above) we must NOT substitute a default here —
	// the viper SetDefault already supplies 10 / 6h when the operator is silent.
	return Config{
		Interval:             interval,
		StaleThreshold:       stale,
		RecencyDepth:         recency,
		BatchSize:            batch,
		MaxConcurrent:        maxConc,
		LeaseTTL:             leaseTTL,
		InitialBackoff:       initBackoff,
		MaxBackoff:           maxBackoff,
		TerminalBackoff:      terminal,
		MaxReprocessAttempts: c.MaxReprocessAttempts,
		MaxStaleAge:          time.Duration(c.MaxStaleAgeMs) * time.Millisecond,
	}
}
