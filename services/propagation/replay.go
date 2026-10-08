package propagation

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/models"
)

// defaultReplayLookback is the IterateStatusesSince window used when the
// operator hasn't pinned register_replay_lookback_hours. 24h covers the
// confirmation horizon and watchdog recency window while keeping startup
// work bounded — a non-terminal tx older than this is almost certainly
// stuck, and re-registering it on every restart won't unstick it (issue
// #145, was 7 days).
const defaultReplayLookback = 24 * time.Hour

// defaultReplaySkipRecent is the merkle_registered_at recency window used
// when MerkleReplaySkipRecentMinutes isn't set. Matches merkle-service's
// postMineTTLSec (1800s = 30min): if we registered within this window,
// merkle-service almost certainly still has the row.
const defaultReplaySkipRecent = 30 * time.Minute

// defaultReplayRPS caps the average requests-per-second the replay loop
// issues against merkle-service when MerkleReplayRPS isn't set.
const defaultReplayRPS = 50

// recoveryReplaySlack is subtracted from the breaker's open time when an
// endpoint recovers: the three failures that tripped the breaker belong to
// txs registered just before it opened, and those txs are part of what the
// endpoint missed.
const recoveryReplaySlack = 5 * time.Minute

// replayParams parameterizes one replay pass. The startup pass and the
// per-endpoint recovery pass differ only in target, window and the
// merkle_registered_at skip.
type replayParams struct {
	// target receives the /watch calls: the whole pool on startup, a single
	// endpoint on recovery.
	target merkleservice.Service
	// since bounds IterateStatusesSince.
	since time.Time
	// skipRecent skips rows whose merkle_registered_at is within this
	// window; 0 disables the skip. The stamp is pool-wide (set when any
	// endpoint accepted), so the recovery pass must pass 0: it cannot tell
	// which endpoint a recent stamp came from.
	skipRecent time.Duration
	// label tags the log lines ("startup" or "recovery").
	label string
	// endpoint is the recovered endpoint's base URL for the recovery pass;
	// empty on startup.
	endpoint string
}

// runMerkleReplay re-registers every non-terminal tx in the store with
// merkle-service /watch. Runs once on startup and exits.
//
// The /watch endpoint is idempotent on merkle-service (ON CONFLICT DO
// NOTHING on the registrations table), and arcade only resubmits txs that
// its own store still considers in-flight, so the replay is safe to run
// every boot without leaking work. The motivation is recovery: when
// merkle-service loses its registration state (data wipe, recreated
// namespace, schema migration that drops rows) arcade's in-flight txs are
// silently no longer watched and no STUMP callbacks will ever fire. Without
// replay the only fix is operator action per-tx; with replay every restart
// resyncs the watch set against the durable state.
//
// Bounded by the existing MerkleConcurrency knob and gated by the
// propagation.register_replay_on_start config flag (default true). Aborts
// silently when merkle-service isn't configured.
func (p *Propagator) runMerkleReplay(ctx context.Context) {
	if p.merkleClient == nil || p.cfg.CallbackURL == "" {
		return
	}
	if p.cfg.Propagation.RegisterReplayOnStart != nil && !*p.cfg.Propagation.RegisterReplayOnStart {
		return
	}

	lookback := time.Duration(p.cfg.Propagation.RegisterReplayLookbackHours) * time.Hour
	if lookback <= 0 {
		lookback = defaultReplayLookback
	}
	// skipRecent: rows registered within this window are skipped. 0 disables
	// the skip — useful for forcing a full re-sync after a known
	// merkle-service wipe (issue #145).
	var skipRecent time.Duration
	switch m := p.cfg.Propagation.MerkleReplaySkipRecentMinutes; {
	case m < 0:
		skipRecent = defaultReplaySkipRecent
	case m == 0:
		skipRecent = 0
	default:
		skipRecent = time.Duration(m) * time.Minute
	}

	p.replayTo(ctx, replayParams{
		target:     p.merkleClient,
		since:      time.Now().Add(-lookback),
		skipRecent: skipRecent,
		label:      "startup",
	})
}

// runRecoveryReplay re-registers, with ONE endpoint, every non-terminal tx
// whose status row moved since that endpoint's breaker opened. While the
// breaker was open the pool kept registering txs with the other endpoints
// (a tx is registered once any endpoint accepts it), so this endpoint's
// watch set is now a strict subset and the STUMPs it emits would lack those
// txs' paths. Re-registering closes that gap for every block mined after the
// pass completes; the bump-builder's content-addressed STUMP handling covers
// a block mined before then.
//
// The pass bypasses the breaker (merkleservice.Recoverable.Endpoint) because
// the pool would otherwise fan the calls out to every endpoint again.
func (p *Propagator) runRecoveryReplay(ctx context.Context, rp merkleservice.Recoverable, endpoint string, openedAt time.Time) {
	target := rp.Endpoint(endpoint)
	if target == nil || p.cfg.CallbackURL == "" {
		return
	}
	p.replayTo(ctx, replayParams{
		target:   target,
		since:    openedAt.Add(-recoveryReplaySlack),
		label:    "recovery",
		endpoint: endpoint,
	})
}

// replayTo is the shared body of the startup and recovery replays.
func (p *Propagator) replayTo(ctx context.Context, rp replayParams) {
	concurrency := p.merkleConcurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	// rps: average rate cap on RegisterBatch calls. 0 disables throttling.
	rps := p.cfg.Propagation.MerkleReplayRPS
	if rps < 0 {
		rps = defaultReplayRPS
	}
	// batchSize bounds the in-memory accumulator before each RegisterBatch
	// round. Small enough that a stalled merkle-service doesn't pin tens of
	// MB of strings while we wait; large enough that the per-batch fixed
	// overhead amortizes well.
	const batchSize = 1000

	logger := p.logger.With(zap.String("replay", rp.label))
	if rp.endpoint != "" {
		logger = logger.With(zap.String("endpoint", rp.endpoint))
	}

	start := time.Now()
	logger.Info(
		"merkle-service replay starting",
		zap.Time("since", rp.since),
		zap.Int("concurrency", concurrency),
		zap.Duration("skip_recent", rp.skipRecent),
		zap.Int("rps", rps),
	)

	var scanned, queued, failures, skippedRecent int
	var throttled time.Duration
	batch := make([]merkleservice.Registration, 0, batchSize)
	now := time.Now()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Average-rate throttle: sleep proportional to batch size so the
		// long-run RPS converges on the configured cap. Simpler than a
		// token bucket and good enough for boot-time catch-up.
		if rps > 0 {
			delay := time.Duration(float64(len(batch))/float64(rps)) * time.Second
			if delay > 0 {
				throttled += delay
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
			}
		}
		// Per-tx results, not fail-fast: one refused /watch must not drop
		// the merkle_registered_at stamp for the rest of the batch, or the
		// next boot re-registers them all again.
		errs := rp.target.RegisterBatchWithResults(ctx, batch, concurrency)
		okTxIDs := make([]string, 0, len(batch))
		var sampleErr error
		for i, err := range errs {
			if err != nil {
				failures++
				if sampleErr == nil {
					sampleErr = err
				}
				continue
			}
			okTxIDs = append(okTxIDs, batch[i].TxID)
		}
		if sampleErr != nil {
			logger.Warn(
				"merkle-service replay batch partially failed",
				zap.Int("batch_size", len(batch)),
				zap.Int("failed", len(batch)-len(okTxIDs)),
				zap.Error(sampleErr),
			)
		}
		// Stamp merkle_registered_at on the rows that landed so future
		// startup replays can skip them. The stamp is pool-wide: a recovery
		// pass stamping it is harmless (the row IS registered with every
		// live endpoint now).
		if len(okTxIDs) > 0 {
			if err := p.store.MarkMerkleRegisteredByTxIDs(ctx, okTxIDs, time.Now()); err != nil {
				logger.Warn(
					"merkle-service replay mark failed",
					zap.Int("count", len(okTxIDs)),
					zap.Error(err),
				)
			}
		}
		batch = batch[:0]
	}

	err := p.store.IterateStatusesSince(ctx, rp.since, func(status *models.TransactionStatus) error {
		scanned++
		// Skip rows that are already terminal — re-registering MINED txs is
		// wasted bandwidth and risks resurrecting watches merkle-service may
		// have legitimately retired.
		if status.Status.IsTerminal() {
			return nil
		}
		if status.TxID == "" {
			return nil
		}
		// Skip rows we registered recently — merkle-service almost certainly
		// still has them, and POST /watch doesn't refresh expires_at anyway
		// (issue #145). skipRecent == 0 disables.
		if rp.skipRecent > 0 && !status.MerkleRegisteredAt.IsZero() && now.Sub(status.MerkleRegisteredAt) < rp.skipRecent {
			skippedRecent++
			return nil
		}
		batch = append(batch, merkleservice.Registration{
			TxID:          status.TxID,
			CallbackURL:   p.cfg.CallbackURL,
			CallbackToken: p.cfg.CallbackToken,
		})
		queued++
		if len(batch) >= batchSize {
			flush()
		}
		// Honor context cancellation between batches so a fast SIGTERM
		// doesn't have to wait for the entire scan.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	})
	flush()

	if err != nil && !errors.Is(err, ctx.Err()) {
		logger.Warn(
			"merkle-service replay scan ended with error",
			zap.Error(err),
			zap.Int("scanned", scanned),
			zap.Int("queued", queued),
		)
	}
	logger.Info(
		"merkle-service replay complete",
		zap.Duration("elapsed", time.Since(start)),
		zap.Int("scanned", scanned),
		zap.Int("queued", queued),
		zap.Int("skipped_recent", skippedRecent),
		zap.Int("failures", failures),
		zap.Duration("throttled", throttled),
	)
}

// recoveryState coalesces breaker-closed events per endpoint: while a
// recovery replay for an endpoint is running, a second event for the same
// endpoint only records the earliest open time and asks the running pass to
// go again, so a flapping endpoint never stacks concurrent replays.
type recoveryState struct {
	running  bool
	again    bool
	openedAt time.Time
}

// scheduleRecoveryReplay is the merkleservice.Recoverable hook. It runs on
// the pool's probe goroutine and must not block, so the replay itself is
// spawned under backgroundWG (Stop waits for it) and guarded by
// recoveryStopped so a hook firing during shutdown cannot Add to a WaitGroup
// that Stop is already waiting on.
func (p *Propagator) scheduleRecoveryReplay(ctx context.Context, rp merkleservice.Recoverable, endpoint string, openedAt time.Time) {
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	if p.recoveryStopped || ctx.Err() != nil {
		return
	}
	if p.recovery == nil {
		p.recovery = make(map[string]*recoveryState)
	}
	st := p.recovery[endpoint]
	if st == nil {
		st = &recoveryState{}
		p.recovery[endpoint] = st
	}
	if st.openedAt.IsZero() || openedAt.Before(st.openedAt) {
		st.openedAt = openedAt
	}
	if st.running {
		st.again = true
		return
	}
	st.running = true
	p.backgroundWG.Add(1)
	go p.recoveryLoop(ctx, rp, endpoint, st)
}

// recoveryLoop runs recovery passes for one endpoint until no further
// breaker-closed event arrived while the last pass was running.
func (p *Propagator) recoveryLoop(ctx context.Context, rp merkleservice.Recoverable, endpoint string, st *recoveryState) {
	defer p.backgroundWG.Done()
	for {
		p.recoveryMu.Lock()
		openedAt := st.openedAt
		st.openedAt = time.Time{}
		st.again = false
		p.recoveryMu.Unlock()

		p.runRecoveryReplay(ctx, rp, endpoint, openedAt)

		p.recoveryMu.Lock()
		if !st.again || ctx.Err() != nil || p.recoveryStopped {
			st.running = false
			p.recoveryMu.Unlock()
			return
		}
		p.recoveryMu.Unlock()
	}
}
