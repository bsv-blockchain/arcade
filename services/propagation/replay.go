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
// operator hasn't pinned register_replay_lookback_hours. 7 days covers the
// typical confirmation horizon (deep reorgs + watchdog recency_depth) while
// keeping startup work bounded for accounts with months of history.
const defaultReplayLookback = 7 * 24 * time.Hour

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
	since := time.Now().Add(-lookback)

	concurrency := p.merkleConcurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	// batchSize bounds the in-memory accumulator before each RegisterBatch
	// round. Small enough that a stalled merkle-service doesn't pin tens of
	// MB of strings while we wait; large enough that the per-batch fixed
	// overhead amortizes well.
	const batchSize = 1000

	start := time.Now()
	p.logger.Info(
		"merkle-service replay starting",
		zap.Time("since", since),
		zap.Int("concurrency", concurrency),
	)

	var scanned, queued, failures int
	batch := make([]merkleservice.Registration, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := p.merkleClient.RegisterBatch(ctx, batch, concurrency); err != nil {
			failures += len(batch)
			p.logger.Warn(
				"merkle-service replay batch failed",
				zap.Int("batch_size", len(batch)),
				zap.Error(err),
			)
		}
		batch = batch[:0]
	}

	err := p.store.IterateStatusesSince(ctx, since, func(status *models.TransactionStatus) error {
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
		p.logger.Warn(
			"merkle-service replay scan ended with error",
			zap.Error(err),
			zap.Int("scanned", scanned),
			zap.Int("queued", queued),
		)
	}
	p.logger.Info(
		"merkle-service replay complete",
		zap.Duration("elapsed", time.Since(start)),
		zap.Int("scanned", scanned),
		zap.Int("queued", queued),
		zap.Int("failures", failures),
	)
}
