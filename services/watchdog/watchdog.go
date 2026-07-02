// Package watchdog hosts the block-processing watchdog that compensates
// for missed BLOCK_PROCESSED deliveries from merkle-service. It runs as a
// standalone arcade service (mode=watchdog) or alongside other services
// when mode=all.
//
// Design overview: every interval, the watchdog queries the shared
// block_processing table for rows that have a header_seen_at but no
// processed_at and are within RecencyDepth of the active tip. For each
// stale row it issues a POST /reprocess to merkle-service so the
// STUMP + BLOCK_PROCESSED callbacks are re-emitted. Per-block
// exponential backoff (in-memory) keeps a misbehaving merkle-service or
// a permanently un-mineable block from soaking the upstream.
//
// Coordination across replicas is via the standard store.Leaser primitive
// (lease name LeaseName). At most one watchdog instance per cluster
// dispatches per tick; the lease is renewed at LeaseTTL/3 cadence.
package watchdog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	mrand "math/rand"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// backoffJitterFraction is the maximum proportional deviation from the
// nominal backoff delay applied on each scheduled retry. ±20% gives enough
// spread to break fleet-wide synchrony at restart (the documented thundering-
// herd risk when many replicas hit InitialBackoff at the same instant)
// without meaningfully changing per-block recovery latency.
const backoffJitterFraction = 0.20

// LeaseName is the cluster-wide coordination key. One watchdog instance
// holds the lease at a time so only one fleet member fires /reprocess
// calls per tick. Recovery is idempotent on merkle-service's side, but
// multiple replicas hammering the same block burns its quota for no
// benefit.
const LeaseName = "block-processing-watchdog"

// attemptState tracks per-block backoff progress. Lives entirely
// in-process — a redeploy resets the map and the watchdog re-discovers
// stale rows via the usual store query. We accept the bounded wave of
// duplicate /reprocess calls across the fleet at restart time as
// cheaper than threading durable backoff state through the store.
type attemptState struct {
	// lastTriedAt is set every time we send a /reprocess. Used by the GC
	// pass to evict stale map entries (rows that have aged out of the
	// recency window or were processed and dropped from the candidate
	// query).
	lastTriedAt time.Time
	// failures counts consecutive non-success outcomes (transient or
	// terminal). Resets to 0 on a 2xx ack. Drives exponential backoff for
	// 5xx/network failures.
	failures int
	// nextEligibleAt gates the next /reprocess attempt for this block.
	// Compared against time.Now in the tick loop; if it's in the future
	// the candidate is skipped without an HTTP call.
	nextEligibleAt time.Time
}

// Config is the resolved knob bundle the run loop consumes. Construct
// via ConfigFromYAML using the public config.WatchdogConfig, or build
// directly in tests.
type Config struct {
	Interval        time.Duration
	StaleThreshold  time.Duration
	RecencyDepth    uint64
	BatchSize       int
	MaxConcurrent   int
	LeaseTTL        time.Duration
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	TerminalBackoff time.Duration
}

// Watchdog is the background actor. Construction is via New so callers
// always pass fully-resolved deps; the zero value is not usable.
type Watchdog struct {
	store        store.Store
	leaser       store.Leaser
	merkleClient *merkleservice.Client
	cfg          Config
	callbackURL  string
	callbackTok  string
	logger       *zap.Logger
	holderID     string

	// now is overridable for tests. Production uses time.Now.
	now func() time.Time
	// jitter is overridable for tests so deterministic assertions on
	// nextEligibleAt don't depend on the RNG. Production uses
	// proportionalJitter; tests can set it to identityJitter for exact
	// equality checks against InitialBackoff / TerminalBackoff.
	jitter func(time.Duration) time.Duration

	mu       sync.Mutex
	attempts map[string]*attemptState
}

// New builds a Watchdog. leaser may be nil for single-process tests; in
// production every replica should pass a real Leaser so the cluster
// elects a single ticker.
func New(
	st store.Store,
	leaser store.Leaser,
	mc *merkleservice.Client,
	callbackURL, callbackToken string,
	cfg Config,
	logger *zap.Logger,
) *Watchdog {
	return &Watchdog{
		store:        st,
		leaser:       leaser,
		merkleClient: mc,
		cfg:          cfg,
		callbackURL:  callbackURL,
		callbackTok:  callbackToken,
		logger:       logger.Named("watchdog"),
		holderID:     newHolderID(),
		now:          time.Now,
		jitter:       proportionalJitter,
		attempts:     make(map[string]*attemptState),
	}
}

// proportionalJitter returns d perturbed by a uniform random fraction in
// [-backoffJitterFraction, +backoffJitterFraction]. Zero/negative input is
// passed through unchanged so callers with a "no delay" intent stay exact.
// Uses math/rand (not crypto/rand) because the application is non-security:
// any source of independence between replicas suffices.
func proportionalJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := int64(float64(d) * backoffJitterFraction)
	if spread <= 0 {
		return d
	}
	// math/rand is fine here — this is jitter for backoff, not crypto.
	// gosec G404 is suppressed accordingly.
	delta := mrand.Int63n(2*spread+1) - spread //nolint:gosec // jitter, not security-relevant
	out := int64(d) + delta
	if out <= 0 {
		return d
	}
	return time.Duration(out)
}

// identityJitter is a no-op jitter for tests that assert on exact nominal
// backoff values. Wired by the watchdog test helper.
func identityJitter(d time.Duration) time.Duration { return d }

// newHolderID matches the propagation reaper's holder format:
// "<hostname>-<8-hex-chars>". The random suffix prevents a restarted pod
// from accidentally believing it already holds a lease an earlier
// incarnation took.
func newHolderID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return host + "-" + hex.EncodeToString(buf[:])
}

// Run is the goroutine entrypoint. Owns its own ticker and lease loop
// and fires an immediate tick on startup so a freshly-recovered replica
// with a backlog of stale rows makes progress without waiting a full
// interval. Exits when ctx is canceled.
func (w *Watchdog) Run(ctx context.Context) {
	w.tick(ctx)

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if w.leaser != nil {
				releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				_ = w.leaser.Release(releaseCtx, LeaseName, w.holderID)
				cancel()
			}
			return
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

// tick is exposed (lowercase) for tests that want to drive the loop a
// single iteration without spinning up the goroutine. Gating + work
// happen here.
func (w *Watchdog) tick(ctx context.Context) {
	if w.leaser != nil {
		heldUntil, err := w.leaser.TryAcquireOrRenew(ctx, LeaseName, w.holderID, w.cfg.LeaseTTL)
		if err != nil {
			metrics.WatchdogTickTotal.WithLabelValues("lease_error").Inc()
			w.logger.Warn("watchdog: lease check failed, skipping tick", zap.Error(err))
			return
		}
		if heldUntil.IsZero() {
			metrics.WatchdogTickTotal.WithLabelValues("skipped_no_leader").Inc()
			w.logger.Debug("watchdog: not leader, skipping tick")
			return
		}
	}
	metrics.WatchdogTickTotal.WithLabelValues("ran").Inc()
	w.runOnce(ctx)
}

// runOnce performs one tick's worth of work assuming we hold the lease.
func (w *Watchdog) runOnce(ctx context.Context) {
	now := w.now()

	tip, err := w.store.GetActiveTipBlockHeight(ctx)
	if err != nil {
		w.logger.Warn("watchdog: failed to read active tip height", zap.Error(err))
		return
	}
	var minHeight uint64
	if tip > w.cfg.RecencyDepth {
		minHeight = tip - w.cfg.RecencyDepth
	}
	olderThan := now.Add(-w.cfg.StaleThreshold)

	candidates, err := w.store.ListStaleBlockProcessingStatus(ctx, olderThan, minHeight, w.cfg.BatchSize)
	if err != nil {
		w.logger.Warn("watchdog: failed to list stale block_processing rows", zap.Error(err))
		return
	}
	metrics.WatchdogStaleCount.Set(float64(len(candidates)))
	if len(candidates) == 0 {
		w.gc(now)
		return
	}

	eligible := w.filterEligible(candidates, now)
	if len(eligible) == 0 {
		w.gc(now)
		return
	}

	w.dispatch(ctx, eligible)
	w.gc(now)
}

// filterEligible drops candidates whose in-memory backoff hasn't expired.
// Returns the rows that should actually trigger a /reprocess this tick.
func (w *Watchdog) filterEligible(rows []*models.BlockProcessingStatus, now time.Time) []*models.BlockProcessingStatus {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := rows[:0:cap(rows)]
	for _, r := range rows {
		st := w.attempts[r.BlockHash]
		if st != nil && st.nextEligibleAt.After(now) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// dispatch fires /reprocess calls in parallel, bounded by MaxConcurrent.
// Each call's outcome updates the in-memory backoff map.
func (w *Watchdog) dispatch(ctx context.Context, rows []*models.BlockProcessingStatus) {
	g, gctx := errgroup.WithContext(ctx)
	if w.cfg.MaxConcurrent > 0 {
		g.SetLimit(w.cfg.MaxConcurrent)
	}
	for _, r := range rows {
		g.Go(func() error {
			w.reprocessOne(gctx, r)
			return nil
		})
	}
	_ = g.Wait()
}

// reprocessOne fires a single /reprocess and updates this block's backoff
// state based on the outcome.
func (w *Watchdog) reprocessOne(ctx context.Context, row *models.BlockProcessingStatus) {
	logger := w.logger.With(
		logfields.BlockHash(row.BlockHash),
		logfields.BlockHeight(row.BlockHeight),
	)
	logger.Info("watchdog: requesting /reprocess for stale block")

	err := w.merkleClient.Reprocess(ctx, row.BlockHash, w.callbackURL, w.callbackTok)
	now := w.now()
	if err == nil {
		metrics.WatchdogReprocessTotal.WithLabelValues("success").Inc()
		w.recordSuccess(row.BlockHash, now)
		return
	}

	var fail *merkleservice.ReprocessError
	switch {
	case errors.As(err, &fail) && fail.StatusCode >= 400 && fail.StatusCode < 500:
		// Terminal: block isn't on the consensus chain (or merkle-service
		// considers the request malformed). Back off heavily — re-trying
		// in seconds would just retry the same disagreement.
		metrics.WatchdogReprocessTotal.WithLabelValues("err_4xx").Inc()
		logger.Warn("watchdog: /reprocess returned 4xx, applying terminal backoff",
			zap.Int("status_code", fail.StatusCode))
		w.recordFailure(row.BlockHash, now, w.cfg.TerminalBackoff)
	case errors.As(err, &fail):
		// 5xx: transient infrastructure issue on merkle-service. Exponential
		// backoff capped at MaxBackoff.
		metrics.WatchdogReprocessTotal.WithLabelValues("err_5xx").Inc()
		logger.Warn("watchdog: /reprocess returned 5xx, applying transient backoff",
			zap.Int("status_code", fail.StatusCode))
		w.recordTransientFailure(row.BlockHash, now)
	default:
		// Network / dial / context error before merkle-service was reached.
		// Same exponential backoff as 5xx — the failure mode is identical
		// from the watchdog's perspective.
		metrics.WatchdogReprocessTotal.WithLabelValues("err_network").Inc()
		logger.Warn("watchdog: /reprocess request failed", zap.Error(err))
		w.recordTransientFailure(row.BlockHash, now)
	}
}

// recordSuccess resets the failure count and parks the block out of the
// candidate set for at least StaleThreshold. We don't actually KNOW
// merkle-service will deliver the callback successfully — only that it
// accepted the request — so we re-evaluate after one stale window. By
// then either the BLOCK_PROCESSED callback landed (and processed_at is
// set, so the row drops out of the query) or it didn't and we'll try
// again.
func (w *Watchdog) recordSuccess(hash string, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempts[hash] = &attemptState{
		lastTriedAt:    now,
		failures:       0,
		nextEligibleAt: now.Add(w.cfg.StaleThreshold),
	}
	metrics.WatchdogBackoffDepth.Set(float64(len(w.attempts)))
}

func (w *Watchdog) recordFailure(hash string, now time.Time, delay time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.attempts[hash]
	if st == nil {
		st = &attemptState{}
		w.attempts[hash] = st
	}
	st.lastTriedAt = now
	st.failures++
	st.nextEligibleAt = now.Add(w.jitter(delay))
	metrics.WatchdogBackoffDepth.Set(float64(len(w.attempts)))
}

func (w *Watchdog) recordTransientFailure(hash string, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.attempts[hash]
	if st == nil {
		st = &attemptState{}
		w.attempts[hash] = st
	}
	st.lastTriedAt = now
	st.failures++
	delay := transientBackoff(w.cfg.InitialBackoff, w.cfg.MaxBackoff, st.failures)
	st.nextEligibleAt = now.Add(w.jitter(delay))
	metrics.WatchdogBackoffDepth.Set(float64(len(w.attempts)))
}

// transientBackoff doubles initial each failure, capped at maxBackoff.
// failures must be >= 1.
func transientBackoff(initial, maxBackoff time.Duration, failures int) time.Duration {
	if failures <= 1 {
		return initial
	}
	delay := time.Duration(float64(initial) * math.Pow(2, float64(failures-1)))
	if delay <= 0 || delay > maxBackoff {
		return maxBackoff
	}
	return delay
}

// gc evicts attempts entries whose lastTriedAt is older than the
// stale-row recency horizon. After 24h the block has either been
// processed (so it would never have been listed again as stale) or has
// fallen out of the recency window (height < tip - RecencyDepth). Either
// way the in-memory state is dead weight.
func (w *Watchdog) gc(now time.Time) {
	const maxAge = 24 * time.Hour
	w.mu.Lock()
	defer w.mu.Unlock()
	for hash, st := range w.attempts {
		if now.Sub(st.lastTriedAt) > maxAge {
			delete(w.attempts, hash)
		}
	}
	metrics.WatchdogBackoffDepth.Set(float64(len(w.attempts)))
}
