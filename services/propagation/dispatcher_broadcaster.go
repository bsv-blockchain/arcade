package propagation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// dispatcherBroadcaster is the broadcast side of the dep-aware
// pipeline. It consumes batches of in-flight entries from the
// dispatcher's outgoingBatch channel, submits each batch to Teranode
// via the configured client, writes terminal status outcomes to the
// store, and reports per-tx status flips back to the dispatcher so its
// dep index can release waiters or cascade rejections.
//
// Why this exists alongside the existing propagator broadcast helpers:
// the current Propagator wires broadcasting through merkle-service
// registration, chunk-and-fallback, and reaper-driven retries. The new
// design removes the reaper, moves dependency ordering into the
// dispatcher, and treats the broadcast worker as a pure executor. This
// broadcaster reimplements only the parts still needed:
//
//   - Fan out the batch to all healthy endpoints in parallel
//   - First 2xx wins, cancel siblings
//   - Per-tx fallback when the batch endpoint reports all-rejected,
//     since /txs returns aggregate-only status
//   - Per-tx terminal store write + statusFlip emission
//
// The dispatcher's own cascade logic handles rejected children of
// rejected parents — this broadcaster only emits direct outcomes.
type dispatcherBroadcaster struct {
	teranodeClient *teranode.Client
	store          store.Store
	incoming       <-chan []*inFlightEntry
	flips          chan<- statusFlip
	logger         *zap.Logger
	submitTimeout  time.Duration
}

// dispatcherBroadcasterConfig collects construction parameters.
type dispatcherBroadcasterConfig struct {
	TeranodeClient *teranode.Client
	Store          store.Store
	Incoming       <-chan []*inFlightEntry
	Flips          chan<- statusFlip
	Logger         *zap.Logger
	SubmitTimeout  time.Duration // 0 → default 15s, matches existing propagator
}

func newDispatcherBroadcaster(cfg dispatcherBroadcasterConfig) (*dispatcherBroadcaster, error) {
	if cfg.TeranodeClient == nil {
		return nil, fmt.Errorf("dispatcherBroadcaster: TeranodeClient required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("dispatcherBroadcaster: Store required")
	}
	if cfg.Incoming == nil {
		return nil, fmt.Errorf("dispatcherBroadcaster: Incoming required")
	}
	if cfg.Flips == nil {
		return nil, fmt.Errorf("dispatcherBroadcaster: Flips required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	timeout := cfg.SubmitTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &dispatcherBroadcaster{
		teranodeClient: cfg.TeranodeClient,
		store:          cfg.Store,
		incoming:       cfg.Incoming,
		flips:          cfg.Flips,
		logger:         logger,
		submitTimeout:  timeout,
	}, nil
}

// Run consumes batches from incoming until ctx is canceled or the
// channel closes. Each batch is processed serially within this
// goroutine — fan-out parallelism is per-batch (across Teranode
// endpoints), not across batches. The dispatcher emits one batch at a
// time and waits for the channel send to succeed before composing the
// next batch, so this single-batch-at-a-time processing matches the
// upstream cadence.
func (b *dispatcherBroadcaster) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-b.incoming:
			if !ok {
				return
			}
			b.processBatch(ctx, batch)
		}
	}
}

// processBatch submits a batch to Teranode, classifies per-tx outcomes,
// writes terminal statuses to the store, and emits a statusFlip per tx.
//
// On a multi-tx batch we try /txs first. If Teranode rejects the entire
// batch (HTTP non-2xx), fall back to per-tx /tx calls so we get
// per-tx status codes — /txs returns aggregate-only error info, so
// without the fallback we couldn't tell which tx in a batch was bad.
// A single-tx batch goes directly to /tx.
func (b *dispatcherBroadcaster) processBatch(ctx context.Context, batch []*inFlightEntry) {
	if len(batch) == 0 {
		return
	}

	if len(batch) == 1 {
		b.broadcastSingle(ctx, batch[0])
		return
	}

	rawTxs := make([][]byte, len(batch))
	for i, entry := range batch {
		rawTxs[i] = entry.rawTx
	}

	statusCode, err := b.submitBatchToHealthy(ctx, rawTxs)
	switch {
	case statusCode >= 200 && statusCode < 300 && err == nil:
		// Batch accepted by at least one endpoint — every tx gets
		// ACCEPTED_BY_NETWORK. Per-tx outcomes aren't available from a
		// /txs success response, but Teranode handles intra-batch
		// dependency ordering internally, so an HTTP 200 means every
		// tx in the batch was accepted into the mempool.
		for _, entry := range batch {
			b.emitOutcome(ctx, entry, models.StatusAcceptedByNetwork, "", http.StatusOK)
		}
	default:
		// All endpoints rejected (or none healthy). Fall back to per-tx
		// so each tx gets its own status code. Some will succeed (if
		// the batch failure was caused by one bad tx), some will fail
		// with specific reasons.
		b.logger.Debug(
			"batch broadcast rejected, falling back to per-tx",
			zap.Int("batch_size", len(batch)),
			zap.Int("status_code", statusCode),
			zap.Error(err),
		)
		b.fallbackPerTx(ctx, batch)
	}
}

// broadcastSingle handles a single-tx batch by calling /tx directly
// across healthy endpoints. The first 2xx response wins and cancels
// siblings.
func (b *dispatcherBroadcaster) broadcastSingle(ctx context.Context, entry *inFlightEntry) {
	statusCode, errMsg := b.submitSingleToHealthy(ctx, entry.rawTx)
	switch {
	case statusCode >= 200 && statusCode < 300:
		b.emitOutcome(ctx, entry, models.StatusAcceptedByNetwork, "", statusCode)
	default:
		b.emitOutcome(ctx, entry, models.StatusRejected, errMsg, statusCode)
	}
}

// fallbackPerTx invokes broadcastSingle for every entry in a batch
// when the batch endpoint rejected the whole set. Used after /txs
// returns all-rejected so we can attribute outcomes per tx.
func (b *dispatcherBroadcaster) fallbackPerTx(ctx context.Context, batch []*inFlightEntry) {
	var wg sync.WaitGroup
	for _, entry := range batch {
		wg.Add(1)
		go func(e *inFlightEntry) {
			defer wg.Done()
			b.broadcastSingle(ctx, e)
		}(entry)
	}
	wg.Wait()
}

// submitBatchToHealthy fans out a /txs request to every healthy
// endpoint in parallel. First 2xx wins and cancels siblings; if every
// endpoint fails, returns the last observed status code and error.
// Returns (0, ErrNoHealthyEndpoints) if no endpoints are available.
func (b *dispatcherBroadcaster) submitBatchToHealthy(parentCtx context.Context, rawTxs [][]byte) (int, error) {
	endpoints := b.teranodeClient.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		return 0, errNoHealthyEndpoints
	}
	submitCtx, submitCancel := context.WithTimeout(parentCtx, b.submitTimeout)
	defer submitCancel()
	broadcastCtx, broadcastCancel := context.WithCancel(submitCtx)
	defer broadcastCancel()

	type result struct {
		statusCode int
		err        error
	}
	resultCh := make(chan result, len(endpoints))
	for _, endpoint := range endpoints {
		ep := endpoint
		go func() {
			sc, err := b.teranodeClient.SubmitTransactions(broadcastCtx, ep, rawTxs)
			select {
			case resultCh <- result{statusCode: sc, err: err}:
			case <-broadcastCtx.Done():
			}
		}()
	}

	var lastStatus int
	var lastErr error
	for i := 0; i < len(endpoints); i++ {
		select {
		case r := <-resultCh:
			if isCanceledByBroadcastCtx(broadcastCtx, r.err) {
				continue
			}
			if r.err == nil && r.statusCode >= 200 && r.statusCode < 300 {
				broadcastCancel()
				return r.statusCode, nil
			}
			lastStatus = r.statusCode
			lastErr = r.err
		case <-submitCtx.Done():
			return lastStatus, submitCtx.Err()
		}
	}
	return lastStatus, lastErr
}

// submitSingleToHealthy is the /tx equivalent of submitBatchToHealthy:
// fan out a single-tx submit across endpoints, first 2xx wins. Returns
// the winning status code on success, or the last observed status code
// and error message on failure.
func (b *dispatcherBroadcaster) submitSingleToHealthy(parentCtx context.Context, rawTx []byte) (int, string) {
	endpoints := b.teranodeClient.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		return 0, errNoHealthyEndpoints.Error()
	}
	submitCtx, submitCancel := context.WithTimeout(parentCtx, b.submitTimeout)
	defer submitCancel()
	broadcastCtx, broadcastCancel := context.WithCancel(submitCtx)
	defer broadcastCancel()

	type result struct {
		statusCode int
		err        error
	}
	resultCh := make(chan result, len(endpoints))
	for _, endpoint := range endpoints {
		ep := endpoint
		go func() {
			sc, err := b.teranodeClient.SubmitTransaction(broadcastCtx, ep, rawTx)
			select {
			case resultCh <- result{statusCode: sc, err: err}:
			case <-broadcastCtx.Done():
			}
		}()
	}

	var lastStatus int
	var lastErrMsg string
	for i := 0; i < len(endpoints); i++ {
		select {
		case r := <-resultCh:
			if isCanceledByBroadcastCtx(broadcastCtx, r.err) {
				continue
			}
			if r.err == nil && r.statusCode >= 200 && r.statusCode < 300 {
				broadcastCancel()
				return r.statusCode, ""
			}
			lastStatus = r.statusCode
			if r.err != nil {
				lastErrMsg = r.err.Error()
			}
		case <-submitCtx.Done():
			return lastStatus, submitCtx.Err().Error()
		}
	}
	return lastStatus, lastErrMsg
}

// emitOutcome writes the terminal status to the store and sends a
// statusFlip to the dispatcher. ACCEPTED_BY_NETWORK writes its row
// directly; REJECTED writes through the store's batch helpers so reason
// is captured. The statusFlip is sent regardless — the dispatcher needs
// to advance its in-flight state even when the store write fails.
//
// Store write failures are logged but not retried here; the dispatcher
// has already updated its in-memory state by the time the flip lands,
// so a transient store outage manifests as a missing status row, not as
// a stuck tx. Reconciliation on restart picks up the divergence (the tx
// is gone from the dispatcher's view but still RECEIVED in the store —
// the Kafka replay re-creates it and the broadcast retries).
func (b *dispatcherBroadcaster) emitOutcome(ctx context.Context, entry *inFlightEntry, status models.Status, errMsg string, statusCode int) {
	now := time.Now()
	row := &models.TransactionStatus{
		TxID:      entry.txid,
		Status:    status,
		Timestamp: now,
		ExtraInfo: errMsg,
	}
	if err := b.store.BatchUpdateStatus(ctx, []*models.TransactionStatus{row}); err != nil {
		b.logger.Warn(
			"dispatcherBroadcaster: status write failed",
			zap.String("txid", entry.txid),
			zap.String("status", string(status)),
			zap.Error(err),
		)
	}

	flip := statusFlip{
		txid:       entry.txid,
		status:     status,
		errorMsg:   errMsg,
		statusCode: statusCode,
	}
	select {
	case b.flips <- flip:
	case <-ctx.Done():
	}
}

// errNoHealthyEndpoints is returned by the submit helpers when the
// Teranode client reports no reachable endpoints. The dispatcher
// classifies an empty status code (0) as retryable, so this surfaces
// naturally as a transient failure rather than a terminal rejection.
var errNoHealthyEndpoints = errors.New("no healthy teranode endpoints")

// isCanceledByBroadcastCtx reports whether the given error is a
// "siblings canceled" artifact (because broadcastCtx was canceled by a
// winning result) rather than a real failure. We don't want to count
// these against the endpoint's health when one of its peers won the
// race.
func isCanceledByBroadcastCtx(broadcastCtx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if broadcastCtx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled)
}
