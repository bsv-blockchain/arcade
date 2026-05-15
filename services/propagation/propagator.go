package propagation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

type propagationMsg struct {
	TXID string `json:"txid"`
	// RawTx is the serialized transaction as raw bytes. encoding/json encodes
	// []byte as base64 — still smaller than hex (4/3 expansion vs 2x) and
	// avoids the per-hop hex encode/decode the pipeline used to do.
	RawTx []byte `json:"raw_tx"`
	// InputTXIDs lists the txids this tx spends from. Populated by the
	// upstream producer (tx_validator) so the propagator can decide
	// eligibility without re-parsing the raw bytes. Empty/absent is
	// treated as "no in-flight parents" — older producers that haven't
	// been updated continue to work, just without dep-aware ordering.
	InputTXIDs []string `json:"input_txids,omitempty"`
}

type Propagator struct {
	cfg            *config.Config
	logger         *zap.Logger
	producer       *kafka.Producer
	publisher      events.Publisher // nil-safe; broadcasts post-broadcast status updates to SSE/webhooks
	store          store.Store
	leaser         store.Leaser
	teranodeClient *teranode.Client
	merkleClient   *merkleservice.Client
	consumer       *kafka.ConsumerGroup

	maxPending int
	// admitCh, terminalCh, drainCh, and requeueCh feed runDispatcher's
	// single state-owning loop. The loop selects on these channels (and,
	// in production, claim.Messages()) and runs ALL dep-aware state
	// mutations — inFlight, waiters, heldMsgs, pendingMsgs, the
	// offsetTracker, and the pendingMarks map — inside the goroutine
	// that owns the loop. No locks, no atomics. See dispatcher.go.
	admitCh           chan admitRequest
	terminalCh        chan terminalEvent
	drainCh           chan drainRequest
	requeueCh         chan requeueRequest
	dispatcherCancel  context.CancelFunc
	merkleConcurrency int
	reaperInterval    time.Duration
	reaperBatchSize   int
	teranodeBatchCap  int
	broadcastWorkers  int
	maxParallelChunks int
	holderID          string
	leaseTTL          time.Duration

	// broadcastJobs feeds the persistent worker pool that runs all
	// per-endpoint SubmitTransaction / SubmitTransactions calls. Replaces
	// the previous per-broadcast `go func(ep)` spawn loop so sustained
	// 50+ TPS doesn't produce constant goroutine churn — total broadcast
	// goroutine count stays bounded at broadcastWorkers regardless of
	// flush rate. Workers exit when broadcastJobs is closed in Stop().
	broadcastJobs    chan broadcastJob
	broadcastWG      sync.WaitGroup
	broadcastRunning atomic.Bool // true while Start() workers are running

	// processBatchSem caps how many flushed batches run their register+
	// broadcast pipeline concurrently. With cap=1 (the historical default
	// before pipelining), batch N+1 cannot start its merkle /watch until
	// batch N's broadcast completes — at sustained 100 TPS that costs
	// ~half-a-pipeline-cycle of queue wait per tx. Cap>1 lets register and
	// broadcast overlap across adjacent batches. flushBatch acquires a
	// slot before spawning the processBatch goroutine, providing natural
	// backpressure to the kafka consumer.
	processBatchSem chan struct{}
	// inflightBatches counts processBatch goroutines that are still
	// running. Stop() blocks on this before tearing down the broadcast
	// worker pool so an in-flight batch doesn't lose its broadcast
	// results to a closed jobs channel.
	inflightBatches sync.WaitGroup
}

// broadcastJob is the unit of work the persistent broadcast pool consumes.
// One job represents one HTTP call to one endpoint; the caller bundles a
// per-call result channel so it can collect outcomes from multiple endpoints
// in parallel without each worker carrying that bookkeeping.
//
// The ctx field is intentionally part of the value — the job travels through
// a channel so the cancellation token has to ride with it. The standard
// "context as first arg" pattern doesn't apply to message-passing handoffs;
// containedctx is suppressed deliberately at the type declaration.
type broadcastJob struct {
	ctx      context.Context //nolint:containedctx // travels with the work item through broadcastJobs channel
	endpoint string
	// Exactly one of rawTx (single /tx) or rawTxs (batch /txs) is set.
	rawTx    []byte
	rawTxs   [][]byte
	resultCh chan<- broadcastJobResult
}

type broadcastJobResult struct {
	endpoint   string
	statusCode int
	// perSlot is the per-submission-slot result list from /txs (post-#881).
	// nil for /tx (single-tx) jobs, nil for /txs jobs whose body couldn't
	// be parsed into one line per submission slot, and nil for any HTTP
	// outcome other than 500-with-parseable-body. Each non-nil entry is
	// either teranode.TxsResultOK ("OK") or a Teranode error code string
	// like "TX_INVALID (31)".
	perSlot []string
	err     error
}

// txResultClass categorizes a per-tx broadcast outcome into the action
// the caller should take. The dep-aware pipeline collapses Teranode's
// rich error vocabulary into four buckets.
type txResultClass int

const (
	// txResultClassUnknown is the zero value — a result that hasn't been
	// classified yet. Should never reach the caller; if it does, the
	// default branch in processBatch's classification loop treats it as
	// a requeue to avoid silently losing the tx.
	txResultClassUnknown txResultClass = iota //nolint:unused // reserved for the zero-value defensive branch in processBatch
	// txResultClassAccepted: terminalize as ACCEPTED_BY_NETWORK, dispatcher
	// releases waiters.
	txResultClassAccepted
	// txResultClassRejected: terminalize as REJECTED, dispatcher cascade-
	// rejects descendants. errMsg carries the Teranode code (e.g.
	// "TX_INVALID (31)") so it shows up in the wallet-visible row.
	txResultClassRejected
	// txResultClassRequeue: infra failure (no peer reachable, batch 500
	// with no parseable body, per-slot PROCESSING code). Tx is not
	// terminal — dispatcher keeps the offset alive and processBatch
	// sends the original propagation message back to the dispatcher
	// after a short flat wait.
	txResultClassRequeue
	// txResultClassInFlight: at least one peer 2xx'd the broadcast but
	// no peer gave a verdict (typically all 202). The tx is in-flight
	// on the network and merkle-service's eventual callback flips its
	// status. processBatch writes nothing, requeues nothing, and the
	// dispatcher's offset stays alive until the callback handler sends
	// a terminal event.
	txResultClassInFlight
)

// broadcastJobBuffer sizes the job channel between broadcast helpers and the
// worker pool. Generous enough that flush-time fan-out doesn't block in
// steady state; bounded so a stalled pool can't grow unboundedly.
const broadcastJobBuffer = 1024

// defaultBroadcastWorkers is the fallback when cfg.Propagation.BroadcastWorkers
// is non-positive. Sized to cover the peak concurrent-job estimate at the
// other shipped defaults (8 concurrent batches × 4 parallel chunks ×
// ~8 healthy datahub endpoints = 256).
const defaultBroadcastWorkers = 256

// defaultMaxParallelChunks caps the per-batch chunk fan-out when
// cfg.Propagation.MaxParallelChunks is non-positive. Each chunk already fans
// out to every healthy endpoint, so the effective concurrency is
// defaultMaxParallelChunks × len(endpoints).
const defaultMaxParallelChunks = 4

// New constructs a Propagator. leaser may be nil, in which case the reaper
// runs unguarded — appropriate for tests and single-process deployments that
// don't need coordination. In production every replica should receive a
// non-nil Leaser so only one reaper is active at a time across the cluster.
func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, publisher events.Publisher, st store.Store, leaser store.Leaser, tc *teranode.Client, mc *merkleservice.Client) *Propagator {
	merkleConcurrency := cfg.Propagation.MerkleConcurrency
	if merkleConcurrency <= 0 {
		merkleConcurrency = 10
	}
	// retry / reaper / lease config fields are preserved on
	// config.Propagation for backwards-compatible YAML, but the
	// dep-aware design no longer drives a reaper or PENDING_RETRY
	// path. Defaults are kept just so any consumer reading the
	// (now-unused) Propagator fields gets a sensible value.
	reaperInterval := time.Duration(cfg.Propagation.ReaperIntervalMs) * time.Millisecond
	if reaperInterval <= 0 {
		reaperInterval = 30 * time.Second
	}
	reaperBatch := cfg.Propagation.ReaperBatchSize
	if reaperBatch <= 0 {
		reaperBatch = 500
	}
	leaseTTL := time.Duration(cfg.Propagation.LeaseTTLMs) * time.Millisecond
	if leaseTTL <= 0 {
		leaseTTL = 3 * reaperInterval
	}
	teranodeBatchCap := cfg.Propagation.TeranodeMaxBatchSize
	if teranodeBatchCap <= 0 {
		teranodeBatchCap = 100
	}
	maxPending := cfg.Propagation.MaxPending
	if maxPending <= 0 {
		maxPending = 50000
	}
	maxConcurrentBatches := cfg.Propagation.MaxConcurrentBatches
	if maxConcurrentBatches <= 0 {
		maxConcurrentBatches = 4
	}
	broadcastWorkers := cfg.Propagation.BroadcastWorkers
	if broadcastWorkers <= 0 {
		broadcastWorkers = defaultBroadcastWorkers
	}
	maxParallelChunks := cfg.Propagation.MaxParallelChunks
	if maxParallelChunks <= 0 {
		maxParallelChunks = defaultMaxParallelChunks
	}
	p := &Propagator{
		cfg:               cfg,
		logger:            logger.Named("propagation"),
		producer:          producer,
		publisher:         publisher,
		store:             st,
		leaser:            leaser,
		teranodeClient:    tc,
		merkleClient:      mc,
		maxPending:        maxPending,
		merkleConcurrency: merkleConcurrency,
		reaperInterval:    reaperInterval,
		reaperBatchSize:   reaperBatch,
		teranodeBatchCap:  teranodeBatchCap,
		broadcastWorkers:  broadcastWorkers,
		maxParallelChunks: maxParallelChunks,
		holderID:          newHolderID(),
		leaseTTL:          leaseTTL,
		broadcastJobs:     make(chan broadcastJob, broadcastJobBuffer),
		processBatchSem:   make(chan struct{}, maxConcurrentBatches),
		admitCh:           make(chan admitRequest, dispatcherChannelBuffer),
		terminalCh:        make(chan terminalEvent, dispatcherChannelBuffer),
		drainCh:           make(chan drainRequest),
		requeueCh:         make(chan requeueRequest, dispatcherChannelBuffer),
	}
	// Start a dispatcher goroutine with a nil claim so tests that
	// construct via New and drive via admitCh / drainCh have a running
	// state machine without needing to invoke Start. In production
	// Start replaces this with the same loop running inside the kafka
	// ClaimHandler — see Start. The two paths can't both be live at
	// once: Start cancels this context before subscribing.
	dispatcherCtx, dispatcherCancel := context.WithCancel(context.Background())
	p.dispatcherCancel = dispatcherCancel
	go func() {
		if err := p.runDispatcher(dispatcherCtx, nil, dispatcherConfig{maxPending: maxPending}); err != nil {
			p.logger.Error("test-mode dispatcher exited with error", zap.Error(err))
		}
	}()
	return p
}

// runBroadcastWorker pulls jobs off broadcastJobs and runs the HTTP submit
// against the named endpoint. Exits when broadcastJobs is closed (Stop()).
// The job's context governs cancellation — a winning sibling cancels the
// per-call broadcastCtx and a 15s deadline bounds worst-case wall time.
func (p *Propagator) runBroadcastWorker() {
	defer p.broadcastWG.Done()
	for job := range p.broadcastJobs {
		var statusCode int
		var perSlot []string
		var err error
		if job.rawTxs != nil {
			statusCode, perSlot, err = p.teranodeClient.SubmitTransactions(job.ctx, job.endpoint, job.rawTxs)
		} else {
			statusCode, err = p.teranodeClient.SubmitTransaction(job.ctx, job.endpoint, job.rawTx)
		}
		// Non-blocking send — the caller always allocates resultCh with
		// capacity ≥ number of jobs it submits, so this never blocks. Using
		// non-blocking lets a Stop() racing with in-flight broadcasts not
		// deadlock the worker on an abandoned channel.
		select {
		case job.resultCh <- broadcastJobResult{endpoint: job.endpoint, statusCode: statusCode, perSlot: perSlot, err: err}:
		default:
		}
	}
}

// newHolderID returns a lease-holder identifier stable for this process's
// lifetime: "<hostname>-<8-hex-chars>". The random suffix disambiguates
// restarts — if an old expired-but-not-yet-purged record still names the
// previous incarnation by hostname alone, the new process will see it as a
// foreign holder and wait for TTL rather than believe it already owns the
// lease.
func newHolderID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return host + "-" + hex.EncodeToString(buf[:])
}

func (p *Propagator) Name() string { return "propagation" }

// applyTerminalStatuses persists the per-tx terminal statuses produced by
// processBatch in one BatchUpdateStatusReturning call, observes the
// RECEIVED→{ACCEPTED_BY_NETWORK,REJECTED} transition age, and emits one
// PublishBulk per terminal status. Lattice no-ops (prev.Status == st.Status)
// and unknown txids (prev == nil — row reaped between RECEIVED and
// broadcast) are excluded from the bulk publish to avoid phantom events.
// Split out of processBatch so the surrounding flush loop stays under
// nesting-complexity limits.
func (p *Propagator) applyTerminalStatuses(ctx context.Context, terminalStatuses []*models.TransactionStatus, accepted, rejected int) {
	if len(terminalStatuses) == 0 {
		return
	}
	prevs, err := p.store.BatchUpdateStatusReturning(ctx, terminalStatuses)
	if err != nil {
		p.logger.Error(
			"batch update propagation status failed",
			zap.Int("batch_size", len(terminalStatuses)),
			zap.Error(err),
		)
		// Continue: per-row entries may still be valid; bulk-publish
		// those whose prev row is populated below.
	}

	acceptedTxIDs := make([]string, 0, accepted)
	rejectedTxIDs := make([]string, 0, rejected)
	now := time.Now()
	for i, st := range terminalStatuses {
		var prev *models.TransactionStatus
		if i < len(prevs) {
			prev = prevs[i]
		}
		// Unknown txid (row was reaped between RECEIVED and broadcast)
		// or per-row store error. Skip publish to avoid phantom events.
		if prev == nil {
			continue
		}
		if !prev.Timestamp.IsZero() {
			metrics.StatusTransitionAge.
				WithLabelValues(string(prev.Status), string(st.Status)).
				Observe(time.Since(prev.Timestamp).Seconds())
		}
		// Lattice no-op — no transition to fan out.
		if prev.Status == st.Status {
			continue
		}
		switch st.Status {
		case models.StatusAcceptedByNetwork:
			acceptedTxIDs = append(acceptedTxIDs, st.TxID)
		case models.StatusRejected:
			rejectedTxIDs = append(rejectedTxIDs, st.TxID)
		default:
			// processBatch only routes ACCEPTED_BY_NETWORK and REJECTED
			// terminal statuses into this slice; other statuses are
			// either retryable (re-queued) or no_verdict (no store
			// update). A defensive default keeps the switch exhaustive.
		}
	}

	p.publishBulkStatus(ctx, models.StatusAcceptedByNetwork, acceptedTxIDs, now)
	p.publishBulkStatus(ctx, models.StatusRejected, rejectedTxIDs, now)

	// Notify the dispatcher of every terminal status flip. ACCEPTED
	// releases waiters via the dispatcher itself (no caller action
	// needed — released msgs are appended directly to the
	// dispatcher's pendingMsgs). REJECTED returns cascaded
	// descendants we write REJECTED rows for; the cascade reason is
	// always "parent rejected" regardless of the parent's actual
	// cause — see persistCascadeRejections.
	var allCascaded []string
	for _, txid := range acceptedTxIDs {
		p.notifyTerminalToDispatcher(txid, models.StatusAcceptedByNetwork)
	}
	for _, txid := range rejectedTxIDs {
		r := p.notifyTerminalToDispatcher(txid, models.StatusRejected)
		allCascaded = append(allCascaded, r.cascaded...)
	}
	if len(allCascaded) > 0 {
		p.persistCascadeRejections(ctx, allCascaded, now)
	}
}

// persistCascadeRejections writes terminal REJECTED rows for txs the
// dep cascade rejected without ever broadcasting them, then emits one
// bulk publish so SSE/webhook subscribers learn about the outcome.
// Best-effort: a store write failure is logged but doesn't undo the
// in-memory cascade state (the dispatcher has already terminalized
// them; we'd be reconciling at restart via Kafka replay anyway).
func (p *Propagator) persistCascadeRejections(ctx context.Context, txids []string, now time.Time) {
	statuses := make([]*models.TransactionStatus, len(txids))
	for i, txid := range txids {
		statuses[i] = &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusRejected,
			Timestamp: now,
			// "parent rejected" is the only structural reason that
			// applies to a cascaded child — it didn't fail for any
			// reason of its own. The parent's actual cause lives on
			// the parent's row; downstream consumers can correlate
			// via the dep graph if they care.
			ExtraInfo: "parent rejected",
		}
	}
	if _, err := p.store.BatchUpdateStatusReturning(ctx, statuses); err != nil {
		p.logger.Warn(
			"cascade rejection write failed",
			zap.Int("count", len(txids)),
			zap.Error(err),
		)
	}
	p.publishBulkStatus(ctx, models.StatusRejected, txids, now)
}

// publishBulkStatus fans a post-broadcast batch status update onto the
// events Publisher as a single bulk event. txids is the list of
// transactions that just transitioned to the same terminal status.
// Non-fatal: the durable store rows are already written, and SSE catchup
// recovers any dropped events.
func (p *Propagator) publishBulkStatus(ctx context.Context, status models.Status, txids []string, ts time.Time) {
	if p.publisher == nil || len(txids) == 0 {
		return
	}
	template := &models.TransactionStatus{
		Status:    status,
		Timestamp: ts,
		TxIDs:     txids,
	}
	if err := p.publisher.PublishBulk(ctx, template); err != nil {
		p.logger.Warn(
			"failed to publish bulk propagation status",
			zap.String("status", string(status)),
			zap.Int("count", len(txids)),
			zap.Error(err),
		)
	}
}

func (p *Propagator) Start(ctx context.Context) error {
	// Stop the test-mode dispatcher goroutine started in New(); the
	// production lifecycle runs the same loop inside the kafka
	// ClaimHandler so dep state + offset marking stay on a single
	// goroutine.
	if p.dispatcherCancel != nil {
		p.dispatcherCancel()
		p.dispatcherCancel = nil
	}

	consumer, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:       p.producer.Broker(),
		GroupID:      p.cfg.Kafka.ConsumerGroup + "-propagation",
		Topics:       []string{kafka.TopicPropagation},
		Producer:     p.producer,
		MaxRetries:   p.cfg.Kafka.MaxRetries,
		Logger:       p.logger,
		ClaimHandler: p.handleClaim(ctx),
	})
	if err != nil {
		return fmt.Errorf("creating consumer group: %w", err)
	}
	p.consumer = consumer

	// Spin up the persistent broadcast worker pool. Workers exit when
	// Stop() closes the job channel; the WaitGroup lets Stop() block until
	// all in-flight submits drain. broadcastRunning gates submitBroadcastJobs
	// so callers don't push into an undrained channel before workers start
	// or after they exit (Stop, or never started in tests).
	p.broadcastRunning.Store(true)
	for i := 0; i < p.broadcastWorkers; i++ {
		p.broadcastWG.Add(1)
		go p.runBroadcastWorker()
	}

	// Replay in-flight registrations to merkle-service. One-shot; exits on
	// its own. Compensates for /watch state loss on the merkle-service side
	// (recreated namespace, data wipe, schema migration) which otherwise
	// silently disables STUMP callbacks for every previously-submitted tx.
	go p.runMerkleReplay(ctx)

	p.logger.Info(
		"propagation service started",
		zap.Duration("reaper_interval", p.reaperInterval),
		zap.Int("reaper_batch_size", p.reaperBatchSize),
		zap.Int("broadcast_workers", p.broadcastWorkers),
		zap.Int("max_parallel_chunks", p.maxParallelChunks),
	)
	return consumer.Run(ctx)
}

// handleClaim returns the kafka.ClaimHandler that owns each per-partition
// session. The dispatcher loop runs in the goroutine Sarama hands us via
// claim, so dep state, Kafka offset tracking, and claim.MarkMessage all
// happen on the same goroutine.
func (p *Propagator) handleClaim(ctx context.Context) kafka.ClaimHandler {
	cfg := dispatcherConfig{maxPending: p.maxPending}
	return func(claim kafka.Claim) error {
		// Use the claim's context as a child of the service context so
		// shutdown OR a rebalance both unblock the loop.
		claimCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-claim.Context().Done():
				cancel()
			case <-claimCtx.Done():
			}
		}()
		return p.runDispatcher(claimCtx, claim, cfg)
	}
}

// WaitForBatches blocks until every processBatch goroutine spawned by
// flushBatch has finished. Used by tests to assert post-flush invariants
// against the in-memory mockStore, and reused by Stop() to drain in-flight
// pipelines before tearing down the broadcast worker pool.
func (p *Propagator) WaitForBatches() {
	p.inflightBatches.Wait()
}

func (p *Propagator) Stop() error {
	p.logger.Info("stopping propagation service")
	var consumerErr error
	if p.consumer != nil {
		consumerErr = p.consumer.Close()
	}
	// Wait for in-flight processBatch goroutines to finish before tearing
	// down the broadcast worker pool. Otherwise an in-flight batch would
	// push jobs into a channel we're about to close, deadlocking the
	// broadcast collect loop on a resultCh that never receives.
	p.inflightBatches.Wait()
	// Closing broadcastJobs lets every worker drain its current iteration
	// and exit. Flip broadcastRunning first so any in-flight submit fan-out
	// falls back to the goroutine path rather than pushing into a channel
	// we're about to close.
	if p.broadcastRunning.Swap(false) {
		close(p.broadcastJobs)
		p.broadcastWG.Wait()
	}
	// Cancel the dispatcher goroutine started in New. Safe to call
	// multiple times (CancelFunc is idempotent).
	if p.dispatcherCancel != nil {
		p.dispatcherCancel()
	}
	return consumerErr
}

// handleMessage decodes the propagation envelope and queues it for the next
// flushBatch. Cheap on purpose: no HTTP, no DB. The Kafka consumer's drain
// loop can race through messages at memory-broker speed, and the batched
// register-then-broadcast happens in flushBatch.
//
// F-024 durability is preserved at the batch level: flushBatch runs
// RegisterBatchWithResults before broadcasting, and any tx whose registration
// failed is routed to handleRetryableFailure (durable PENDING_RETRY) and
// excluded from broadcast. The retry+DLQ semantics that used to live here per
// message are now reaper-driven.
//
// A high-water-mark on pendingMsgs guards against unbounded growth if a
// downstream stall lasts longer than the consumer's offset commit window.
func (p *Propagator) handleMessage(_ context.Context, msg *kafka.Message) error {
	var propMsg propagationMsg
	if err := json.Unmarshal(msg.Value, &propMsg); err != nil {
		return fmt.Errorf("unmarshaling propagation message: %w", err)
	}

	if len(propMsg.RawTx) == 0 {
		return fmt.Errorf("propagation message has empty raw_tx")
	}

	// All admission logic — parent dep check, pendingMsgs append,
	// offset tracker bookkeeping — happens on the dispatcher
	// goroutine. When pending is at its cap, the dispatcher's select
	// excludes admitCh, so this send blocks. The Kafka consumer
	// goroutine waits here until the dispatcher has room, which
	// naturally pauses Kafka pulls and lets backpressure flow back to
	// the broker. No DLQ, no error to the client; the only observable
	// effect is briefly increased consumer lag.
	_ = p.admitToDispatcher(propMsg, msg.Offset)
	return nil
}

// flushBatch hands the drained pending slice off to a processBatch goroutine
// and returns. Concurrency is bounded by processBatchSem: while batch N runs
// its register+broadcast pipeline (~4s at 100 TPS), the kafka consumer can
// drain batch N+1 and begin its own pipeline in parallel up to the configured
// cap (MaxConcurrentBatches, default 4). Sustained-100-TPS RECEIVED→
// ACCEPTED_BY_NETWORK latency benefits roughly by half-a-pipeline-cycle per
// tx because pendingMsgs no longer sits idle waiting for the prior batch's
// broadcast to finish.
//
// Acquiring the semaphore inside flushBatch (rather than firing the goroutine
// unconditionally) provides natural backpressure: when MaxConcurrentBatches
// pipelines are already in flight, the kafka consumer's flush call blocks
// here until a slot frees. This bounds peak in-memory pendingMsgs depth and
// gives the kafka claim a clean cancellation point.
//
// The context comes from the current Kafka claim — it is canceled when the
// claim ends (shutdown or rebalance). Downstream HTTP broadcasts and store
// writes observe that cancellation and unwind cleanly, so a revoked partition
// doesn't keep doing work on behalf of a partition it no longer owns.
//
// F-024 ("register before broadcast") is preserved per-batch: each goroutine
// drives one batch through registerBatch and broadcastInChunks sequentially.
// Across batches, status writes pass through the lattice so a slower batch's
// ACCEPTED_BY_NETWORK can't regress a tx that a faster sibling already moved
// to SEEN_ON_NETWORK.
func (p *Propagator) flushBatch(ctx context.Context) error {
	// Drain pendingMsgs from the dispatcher goroutine via the
	// drainCh request/reply. The dispatcher owns the slice; we
	// receive a snapshot and own it from here on.
	batch := p.drainPending()
	metrics.PropagationPendingDepth.Set(0)

	if len(batch) == 0 {
		return nil
	}

	select {
	case p.processBatchSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.inflightBatches.Add(1)
	metrics.PropagationInflightBatches.Set(float64(len(p.processBatchSem)))
	go func() {
		defer func() {
			<-p.processBatchSem
			p.inflightBatches.Done()
			metrics.PropagationInflightBatches.Set(float64(len(p.processBatchSem)))
		}()
		p.processBatch(ctx, batch)
	}()
	return nil
}

// registerBatch invokes merkle-service /watch for every tx in the batch and
// partitions the result into "registered" (broadcast-eligible) and "failed"
// registers every tx with merkle-service /watch before broadcast.
//
// Merkle-service /watch is binary: the only per-tx variation is the txid
// string, so a failure means the upstream is unavailable, not that any
// particular tx is malformed. The function retries the whole batch
// inline with capped-exponential backoff (merkleRetryBackoffs), forever.
// While this loop is running it holds the caller's processBatchSem slot,
// which produces the backpressure we want: once all slots are held, the
// consumer stops admitting new work until merkle-service recovers.
//
// When the merkle integration is disabled (client nil or no callback URL),
// every tx is treated as registered — there's no registration step to fail.
func (p *Propagator) registerBatch(ctx context.Context, batch []propagationMsg) []propagationMsg {
	if p.merkleClient == nil || p.cfg.CallbackURL == "" {
		return batch
	}

	regs := make([]merkleservice.Registration, len(batch))
	for i, m := range batch {
		regs[i] = merkleservice.Registration{
			TxID:          m.TXID,
			CallbackURL:   p.cfg.CallbackURL,
			CallbackToken: p.cfg.CallbackToken,
		}
	}

	attempt := 0
	for {
		start := time.Now()
		errs := p.merkleClient.RegisterBatchWithResults(ctx, regs, p.merkleConcurrency)
		metrics.PropagationMerkleRegisterDuration.Observe(time.Since(start).Seconds())

		var failedCount int
		for _, err := range errs {
			if err != nil {
				failedCount++
			}
		}

		if failedCount == 0 {
			metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok").Inc()
			// Stamp merkle_registered_at on every tx we successfully registered.
			// The startup replay loop reads this and skips rows registered within
			// MerkleReplaySkipRecentMinutes — without it, every restart re-walks
			// the whole watchlist regardless of whether merkle-service already
			// has it (issue #145). A store-write failure here must not block
			// broadcast: the mark is a hint, not part of the F-024 invariant.
			txids := make([]string, len(batch))
			for i, m := range batch {
				txids[i] = m.TXID
			}
			if err := p.store.MarkMerkleRegisteredByTxIDs(ctx, txids, time.Now()); err != nil {
				p.logger.Warn(
					"mark merkle-registered failed",
					zap.Int("count", len(txids)),
					zap.Error(err),
				)
			}
			return batch
		}

		// At least one /watch failed. Treat as upstream-unavailable and
		// retry the WHOLE batch — a partial failure on a per-tx-uniform
		// payload (only the txid varies) indicates the merkle-service
		// itself is in distress, not that a specific tx is special.
		metrics.PropagationMerkleRegisterFailures.WithLabelValues("register_error").Add(float64(failedCount))
		if failedCount == len(batch) {
			metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("all_failed").Inc()
		} else {
			metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("partial").Inc()
		}

		// Pull a representative error for the log line.
		var sampleErr error
		for _, err := range errs {
			if err != nil {
				sampleErr = err
				break
			}
		}
		p.logger.Warn(
			"merkle-service /watch failing; retrying batch",
			zap.Int("batch_size", len(batch)),
			zap.Int("failed", failedCount),
			zap.Int("attempt", attempt+1),
			zap.Error(sampleErr),
		)

		backoff := merkleRetryBackoff(attempt)
		attempt++
		select {
		case <-ctx.Done():
			// Claim ended (shutdown / rebalance). Return the original
			// batch unregistered; the next consumer of this partition
			// will replay these messages because their Kafka offsets
			// remain in flight on the dispatcher's tracker.
			return nil
		case <-time.After(backoff):
		}
	}
}

// merkleRetryBackoffs is the capped-exponential schedule for the
// registerBatch inline retry: 100ms → 500ms → 2s → 5s → 10s, then 10s
// steady. Driven by the agreed "retry forever for infra failures" rule
// from docs/plans/dependency-aware-dispatch.md.
var merkleRetryBackoffs = []time.Duration{
	100 * time.Millisecond,
	500 * time.Millisecond,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
}

// merkleRetryBackoff returns the backoff for retry attempt n (0-indexed).
// Clamps to the last entry of merkleRetryBackoffs once the schedule is
// exhausted — every attempt after the schedule waits the cap.
func merkleRetryBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(merkleRetryBackoffs) {
		return merkleRetryBackoffs[len(merkleRetryBackoffs)-1]
	}
	return merkleRetryBackoffs[attempt]
}

// txResult carries per-tx outcome of a broadcast. class is the
// authoritative bucket (accepted / rejected / requeue) that processBatch
// switches on; the other fields are diagnostic / status-write inputs.
//
// successEndpoint is the URL of the peer whose response drove the
// accepted status (empty when no peer accepted or the broadcast
// produced no verdict). errMsg carries a short Teranode code string
// like "TX_INVALID (31)" when the class is rejected.
type txResult struct {
	class           txResultClass
	status          *models.TransactionStatus
	errMsg          string
	rawTx           []byte
	successEndpoint string
}

// classifyPerSlotLine maps a /txs per-slot line (post-#881) into the
// dispatcher action bucket. "OK" → accepted. A terminal Teranode code
// → rejected. An infra-bucket code (or anything unrecognized) →
// requeue. The errMsg returned is the per-slot line itself, kept
// verbatim so wallet rows surface the actual Teranode code.
func classifyPerSlotLine(line string) (txResultClass, string) {
	if line == teranode.TxsResultOK {
		return txResultClassAccepted, ""
	}
	if isTeranodeTerminalCode(line) {
		return txResultClassRejected, line
	}
	return txResultClassRequeue, line
}

// teranodeTerminalCodes is the set of post-#881 Teranode error code
// strings (NAME-portion only) that classify a tx as terminally rejected.
// PROCESSING and anything else falls through to requeue (infra failure).
var teranodeTerminalCodes = map[string]struct{}{
	"TX_INVALID":              {},
	"TX_INVALID_DOUBLE_SPEND": {},
	"TX_CONFLICTING":          {},
	"TX_LOCKED":               {},
	"TX_LOCK_TIME":            {},
	"TX_POLICY":               {},
	"TX_COINBASE_IMMATURE":    {},
	"TX_MISSING_PARENT":       {},
	"UTXO_FROZEN":             {},
	"UTXO_SPENT":              {},
	"UTXO_NON_FINAL":          {},
	"UTXO_INVALID_SIZE":       {},
	"INVALID_ARGUMENT":        {},
}

// isTeranodeTerminalCode reports whether a per-slot line names a
// Teranode code in the terminal-rejection bucket. Matches the NAME
// portion of "NAME (num)" or just "NAME". Anything else (including
// the default PROCESSING wrapper, network-only codes, or unrecognized
// strings) is treated as infra → requeue.
func isTeranodeTerminalCode(line string) bool {
	name := line
	if idx := strings.IndexByte(name, ' '); idx >= 0 {
		name = name[:idx]
	}
	_, ok := teranodeTerminalCodes[name]
	return ok
}

// requeueWait is the short flat delay before processBatch sends an
// infra-failed sub-batch back to the dispatcher. Kept short enough to
// pick up Teranode/merkle-service recovery promptly, long enough that a
// sustained outage doesn't busy-loop the processBatch slot.
const requeueWait = 2 * time.Second

// processBatch handles a batch of propagation messages:
//  1. Register every tx with merkle-service (batched, bounded concurrency).
//     Failed-register txs go to handleRetryableFailure and are EXCLUDED from
//     the broadcast pass — preserves the F-024 "register before broadcast"
//     invariant at batch granularity.
//  2. Broadcast registered txs to teranode endpoints, chunked to
//     teranodeBatchCap.
//  3. Update status for each transaction.
//
// All failure paths are absorbed internally: per-tx failures route to
// PENDING_RETRY or get logged-and-skipped, and a batch-wide store error is
// logged on the goroutine spawned by flushBatch. There is no caller that
// reacts to an aggregate error here, so the function returns void.
func (p *Propagator) processBatch(ctx context.Context, batch []propagationMsg) {
	// Step 1: register all txs with merkle-service in parallel. Drops any tx
	// whose registration failed — that tx is already queued for durable retry
	// via handleRetryableFailure inside registerBatch.
	batch = p.registerBatch(ctx, batch)
	if len(batch) == 0 {
		return
	}

	// Log batch summary for traceability
	txidSample := make([]string, 0, 5)
	for i, msg := range batch {
		if i >= 5 {
			break
		}
		txidSample = append(txidSample, msg.TXID)
	}
	p.logger.Info(
		"processing batch",
		zap.Int("count", len(batch)),
		zap.Strings("txids_sample", txidSample),
	)

	metrics.PropagationBatchSize.Observe(float64(len(batch)))

	// Step 2: Broadcast in chunks bounded by teranodeBatchCap so a single
	// oversized Kafka flush doesn't blow past Teranode's /txs size limit.
	rawTxs := make([][]byte, len(batch))
	for i, msg := range batch {
		rawTxs[i] = msg.RawTx
	}
	results := p.broadcastInChunks(ctx, batch, rawTxs)

	// Step 2: Classify per-tx outcomes by txResultClass.
	//   accepted → terminal ACCEPTED status write, dispatcher releases waiters
	//   rejected → terminal REJECTED status write, dispatcher cascade-rejects
	//   inFlight → no terminal write; merkle-service callback drives forward
	//   requeue  → no terminal write; the original propagationMsg goes back
	//              to the dispatcher after a short flat wait
	seenEndpoints := make(map[string]struct{})
	var successEndpoints []string
	var accepted, rejected, inFlight int
	terminalStatuses := make([]*models.TransactionStatus, 0, len(results))
	requeueMsgs := make([]propagationMsg, 0)
	for i, res := range results {
		if res.successEndpoint != "" {
			if _, ok := seenEndpoints[res.successEndpoint]; !ok {
				seenEndpoints[res.successEndpoint] = struct{}{}
				successEndpoints = append(successEndpoints, res.successEndpoint)
			}
		}
		switch res.class {
		case txResultClassAccepted:
			accepted++
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassRejected:
			rejected++
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassInFlight:
			inFlight++
		case txResultClassRequeue:
			requeueMsgs = append(requeueMsgs, batch[i])
		default:
			// Unknown class — defensive: requeue so we don't silently lose
			// the tx and the dispatcher's offset stays alive.
			requeueMsgs = append(requeueMsgs, batch[i])
		}
	}

	p.applyTerminalStatuses(ctx, terminalStatuses, accepted, rejected)
	metrics.PropagationOutcomeTotal.WithLabelValues("accepted").Add(float64(accepted))
	metrics.PropagationOutcomeTotal.WithLabelValues("rejected").Add(float64(rejected))
	metrics.PropagationOutcomeTotal.WithLabelValues("no_verdict").Add(float64(inFlight))
	metrics.PropagationOutcomeTotal.WithLabelValues("requeue").Add(float64(len(requeueMsgs)))

	p.logger.Info(
		"batch propagated",
		zap.Int("count", len(batch)),
		zap.Int("accepted", accepted),
		zap.Int("rejected", rejected),
		zap.Int("in_flight", inFlight),
		zap.Int("requeue", len(requeueMsgs)),
		zap.Strings("success_endpoints", successEndpoints),
	)

	if len(requeueMsgs) > 0 {
		// Wait a short flat period so a brief upstream blip has time to
		// recover before we re-enter the batch into the dispatcher. The
		// processBatch slot stays held during the wait, which is the
		// natural backpressure we want when upstream is unhealthy.
		select {
		case <-ctx.Done():
			return
		case <-time.After(requeueWait):
		}
		p.requeueToDispatcher(requeueMsgs)
	}
}

// Per-batch chunk parallelism is now config-driven via
// cfg.Propagation.MaxParallelChunks (see Propagator.maxParallelChunks);
// defaults to defaultMaxParallelChunks.

// broadcastInChunks splits a batch into teranodeBatchCap-sized chunks and
// broadcasts each via /txs. Chunks run in parallel bounded by
// p.maxParallelChunks so a large flush doesn't serialize behind one slow
// endpoint. Returns per-tx results in the same order as the input.
func (p *Propagator) broadcastInChunks(ctx context.Context, batch []propagationMsg, rawTxs [][]byte) []txResult {
	results := make([]txResult, len(batch))
	chunkSize := p.teranodeBatchCap
	if chunkSize <= 0 {
		chunkSize = len(batch)
	}

	type chunk struct {
		start, end int
	}
	var chunks []chunk
	for start := 0; start < len(batch); start += chunkSize {
		end := start + chunkSize
		if end > len(batch) {
			end = len(batch)
		}
		chunks = append(chunks, chunk{start: start, end: end})
	}

	if len(chunks) <= 1 {
		if len(chunks) == 1 {
			c := chunks[0]
			p.broadcastChunk(ctx, batch[c.start:c.end], rawTxs[c.start:c.end], results[c.start:c.end])
		}
		return results
	}

	sem := make(chan struct{}, p.maxParallelChunks)
	var wg sync.WaitGroup
	for _, c := range chunks {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			p.broadcastChunk(ctx, batch[c.start:c.end], rawTxs[c.start:c.end], results[c.start:c.end])
		}()
	}
	wg.Wait()
	return results
}

// broadcastChunk broadcasts a single chunk (≤ teranodeBatchCap) via /txs
// (multi-tx) or /tx (single-tx) and writes per-tx classifications into
// out. No per-tx fallback: with #879+#881 the /txs response carries
// per-slot info for arcade to classify each tx directly.
func (p *Propagator) broadcastChunk(ctx context.Context, chunk []propagationMsg, rawTxs [][]byte, out []txResult) {
	metrics.PropagationChunkTotal.WithLabelValues("none").Inc()
	if len(chunk) == 1 {
		br := p.broadcastSingleToEndpoints(ctx, rawTxs[0], chunk[0].TXID)
		out[0] = singleResultToTxResult(br, chunk[0])
		return
	}

	results, _ := p.broadcastBatchToEndpoints(ctx, rawTxs, chunk)
	copy(out, results)
}

// singleResultToTxResult maps a single-tx broadcastResult (returned by
// the /tx fan-out) into a txResult with the dispatcher action class set.
// Without per-slot info from /tx the classification comes from the
// HTTP status code that one peer returned: 200 → accepted, 4xx → rejected
// with the Teranode body as errMsg, 5xx / unreachable → requeue. The
// "in-flight, no verdict" case (every peer 202 without a definitive
// answer) leaves status nil and class accepted: the merkle-service
// callback will move the tx forward.
func singleResultToTxResult(br broadcastResult, msg propagationMsg) txResult {
	if br.Status != nil {
		switch br.Status.Status { //nolint:exhaustive // only ACCEPTED and REJECTED are emitted by broadcastSingleOnce
		case models.StatusAcceptedByNetwork:
			return txResult{
				class:           txResultClassAccepted,
				status:          br.Status,
				rawTx:           msg.RawTx,
				successEndpoint: br.SuccessEndpoint,
			}
		case models.StatusRejected:
			return txResult{
				class:  txResultClassRejected,
				status: br.Status,
				errMsg: br.ErrorMsg,
				rawTx:  msg.RawTx,
			}
		}
	}
	if br.Acknowledged {
		// In-flight: at least one peer 2xx'd but no verdict. Leave the
		// tx alone — merkle-service callback drives it forward. The
		// dispatcher's offset stays in flight; no terminal write.
		return txResult{
			class:           txResultClassInFlight,
			rawTx:           msg.RawTx,
			successEndpoint: br.SuccessEndpoint,
		}
	}
	// No peer reachable / all timed out — infra failure, requeue.
	return txResult{
		class: txResultClassRequeue,
		rawTx: msg.RawTx,
	}
}

// broadcastResult holds the outcome of a single-tx broadcast across all endpoints.
type broadcastResult struct {
	Status          *models.TransactionStatus
	ErrorMsg        string // best error message from endpoints (for retryable classification)
	SuccessEndpoint string // URL of the peer that accepted the tx (empty if none did)
	// Acknowledged is true when at least one endpoint responded with 2xx
	// (200 OK or 202 Accepted). Distinguishes "peer received the tx but
	// won't issue a definitive verdict yet" (Status=nil, Acknowledged=true)
	// from "no peer was reachable" (Status=nil, Acknowledged=false). The
	// former is in-flight and shouldn't be retried; the latter must go to
	// PENDING_RETRY or the tx stays stuck in RECEIVED forever.
	Acknowledged bool
}

// inlineRetryAttempts is the number of *additional* attempts to make after
// the first broadcast fails with a retryable error. Total attempts therefore
// are inlineRetryAttempts+1. Kept small because each attempt already fans out
// across all healthy endpoints; the goal is to ride out transient network
// blips without waiting the full reaper_interval for PENDING_RETRY.
// broadcastSingleToEndpoints submits a single transaction to each healthy
// teranode endpoint using POST /tx. On the first accepting endpoint (200 OK)
// the shared broadcast context is canceled so slower sibling requests don't
// gate wall-time on the slowest peer. Per-endpoint outcomes are recorded into
// the teranode client's circuit-breaker so repeatedly failing peers are
// sidelined from future broadcasts.
//
// No inline retry: transient failures classify as txResultClassRequeue at
// the call site (see singleResultToTxResult), and the dispatcher's requeue
// path picks them up after a short flat wait.
func (p *Propagator) broadcastSingleToEndpoints(ctx context.Context, rawTx []byte, txid string) broadcastResult {
	return p.broadcastSingleOnce(ctx, rawTx, txid)
}

// broadcastSingleOnce is one attempt of a single-tx broadcast. Split out so
// broadcastSingleToEndpoints can loop around it for inline retries.
func (p *Propagator) broadcastSingleOnce(ctx context.Context, rawTx []byte, txid string) broadcastResult {
	start := time.Now()
	defer func() {
		metrics.PropagationBroadcastDuration.WithLabelValues("single").Observe(time.Since(start).Seconds())
	}()
	endpoints := p.teranodeClient.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		p.logger.Error("no healthy teranode endpoints")
		return broadcastResult{}
	}

	submitCtx, cancelSubmit := context.WithTimeout(ctx, 15*time.Second)
	defer cancelSubmit()
	broadcastCtx, cancelBroadcast := context.WithCancel(submitCtx)
	defer cancelBroadcast()

	// Submit one job per endpoint to the persistent worker pool. resultCh is
	// sized to len(endpoints) so worker sends never block. The for-range
	// drains exactly len(endpoints) results before exiting — no separate
	// goroutine to close the channel.
	resultCh := make(chan broadcastJobResult, len(endpoints))
	submitted := p.submitBroadcastJobs(broadcastCtx, endpoints, rawTx, nil, resultCh)

	// Collect every non-canceled result first so the circuit-breaker can
	// reason about the whole broadcast attempt at once (network-consensus
	// detection). Per-result aggregation of bestStatus stays in the loop —
	// we still need it to decide the tx's status update.
	outcomes := make([]endpointOutcome, 0, submitted)
	var bestStatus models.Status
	var lastErrMsg string
	var successEndpoint string
	acknowledged := false
	for i := 0; i < submitted; i++ {
		result := <-resultCh
		// A sibling request canceled by a winning race is not a real failure —
		// the peer didn't misbehave, we called off the race. Skip health
		// recording and status aggregation for that case.
		if isCanceledByBroadcast(broadcastCtx, result.err) {
			continue
		}
		outcomes = append(outcomes, endpointOutcome{endpoint: result.endpoint, statusCode: result.statusCode})
		if result.err != nil {
			lastErrMsg = result.err.Error()
			if statusPriority(models.StatusRejected) > statusPriority(bestStatus) {
				bestStatus = models.StatusRejected
			}
			continue
		}
		switch result.statusCode {
		case http.StatusOK:
			acknowledged = true
			if statusPriority(models.StatusAcceptedByNetwork) > statusPriority(bestStatus) {
				bestStatus = models.StatusAcceptedByNetwork
				successEndpoint = result.endpoint
			}
			// Early-cancel: the first 200 is the verdict. Sibling requests
			// observe broadcastCtx.Err() and return quickly.
			cancelBroadcast()
		case http.StatusAccepted:
			// 202 means the peer accepted the tx but won't tell us yet —
			// matching original behavior, no tx-level status update, but
			// flag Acknowledged so the caller knows the tx reached a peer.
			acknowledged = true
			cancelBroadcast()
		}
	}
	recordBroadcastOutcomes(p.teranodeClient, outcomes)

	if bestStatus == "" {
		return broadcastResult{Acknowledged: acknowledged}
	}

	return broadcastResult{
		Status: &models.TransactionStatus{
			TxID:      txid,
			Status:    bestStatus,
			Timestamp: time.Now(),
		},
		ErrorMsg:        lastErrMsg,
		SuccessEndpoint: successEndpoint,
		Acknowledged:    acknowledged,
	}
}

// isCanceledByBroadcast reports whether err is a context.Canceled directly
// caused by the broadcast's own cancel signal (i.e. the winning race). A
// context.Canceled that wasn't triggered by the broadcast cancel still counts
// as a real failure — e.g. the outer submitCtx timing out.
func isCanceledByBroadcast(broadcastCtx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if broadcastCtx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled)
}

// endpointOutcome is one (endpoint, statusCode) tuple ready for batched
// circuit-breaker accounting. Carried as a slice so recordBroadcastOutcomes
// can reason about the whole broadcast attempt at once.
type endpointOutcome struct {
	endpoint   string
	statusCode int
}

// recordBroadcastOutcomes applies circuit-breaker accounting to a complete
// set of per-endpoint outcomes from one broadcast attempt. Distinguishes
// peer-health signals from network-consensus signals so persistent invalid
// tx submissions don't progressively sideline every peer:
//
//   - statusCode == 0 (no HTTP response): always RecordFailure. Reachability
//     is per-peer; one stuck DNS or transport doesn't speak to the network.
//   - At least one 2xx in the set: classic mode. Each non-2xx peer is
//     penalized via RecordBroadcastFailure (they disagreed with a peer who
//     accepted — they're the outliers), each 2xx peer is credited.
//   - Zero 2xx but every responder returned non-2xx: unanimous network
//     reject. The peers are doing their job (responding) and they all agree
//     the tx is bad — penalizing them would punish the messenger. Treat as
//     RecordSuccess for the responding peers. Transport errors still get
//     RecordFailure (they didn't respond at all).
//
// The intent of the unanimous-reject branch is the resilience tunable from
// the 02:07 EDT incident: when the tx generator produces a double-spend
// storm, every honest peer returns 500 "failed to validate" and the old
// per-result code sidelined them all, leaving us with zero healthy peers
// and 1.6M no_verdict outcomes. With this branch, peers stay healthy and
// the txs flow through to UpdateStatus(REJECTED) — the correct signal that
// our outgoing payload is the problem.
func recordBroadcastOutcomes(tc *teranode.Client, outcomes []endpointOutcome) {
	if len(outcomes) == 0 {
		return
	}
	any2xx := false
	anyResponded := false
	for _, o := range outcomes {
		if o.statusCode >= 200 && o.statusCode < 300 {
			any2xx = true
		}
		if o.statusCode != 0 {
			anyResponded = true
		}
	}
	unanimousReject := !any2xx && anyResponded
	switch {
	case any2xx:
		metrics.PropagationBroadcastConsensus.WithLabelValues("accepted").Inc()
	case unanimousReject:
		metrics.PropagationBroadcastConsensus.WithLabelValues("unanimous_reject").Inc()
	default:
		// no 2xx, no non-zero responses → everyone was unreachable
		metrics.PropagationBroadcastConsensus.WithLabelValues("unreachable").Inc()
	}
	for _, o := range outcomes {
		switch {
		case o.statusCode == 0:
			tc.RecordFailure(o.endpoint)
		case o.statusCode >= 200 && o.statusCode < 300:
			tc.RecordSuccess(o.endpoint)
		case unanimousReject:
			// Network consensus — peer responded, did its job. Reset its
			// counters so a long rejection storm doesn't progressively
			// sideline the entire fleet.
			tc.RecordSuccess(o.endpoint)
		default:
			tc.RecordBroadcastFailure(o.endpoint)
		}
	}
}

// submitBroadcastJobs enqueues one broadcast job per endpoint to the
// persistent worker pool, returning the number of jobs actually queued.
// Exactly one of rawTx (single /tx) or rawTxs (batch /txs) must be non-nil.
//
// If the job channel is full or nil (tests construct Propagator without
// Start() — broadcastJobs is initialized in New but the pool may not be
// running), this falls back to spawning a one-shot goroutine per endpoint
// so behavior matches the persistent-pool path. Result channel ordering is
// independent of submission order, which is fine — callers aggregate by
// endpoint, not position.
func (p *Propagator) submitBroadcastJobs(ctx context.Context, endpoints []string, rawTx []byte, rawTxs [][]byte, resultCh chan<- broadcastJobResult) int {
	submitted := 0
	useChannel := p.broadcastRunning.Load()
	for _, endpoint := range endpoints {
		job := broadcastJob{
			ctx:      ctx,
			endpoint: endpoint,
			rawTx:    rawTx,
			rawTxs:   rawTxs,
			resultCh: resultCh,
		}
		if useChannel {
			select {
			case p.broadcastJobs <- job:
				submitted++
				continue
			default:
				// Pool saturated — fall through to goroutine spawn so the
				// flush path can still progress.
			}
		}
		submitted++
		go func(j broadcastJob) {
			var statusCode int
			var perSlot []string
			var err error
			if j.rawTxs != nil {
				statusCode, perSlot, err = p.teranodeClient.SubmitTransactions(j.ctx, j.endpoint, j.rawTxs)
			} else {
				statusCode, err = p.teranodeClient.SubmitTransaction(j.ctx, j.endpoint, j.rawTx)
			}
			select {
			case j.resultCh <- broadcastJobResult{endpoint: j.endpoint, statusCode: statusCode, perSlot: perSlot, err: err}:
			default:
			}
		}(job)
	}
	return submitted
}

// broadcastBatchToEndpoints submits a batch to each healthy teranode
// endpoint via /txs and produces per-tx classifications:
//
//   - Any endpoint returning 200 → every slot accepted (the peer accepted
//     the whole batch; per-slot info from other peers' 500 responses is
//     superseded).
//   - All endpoints failed with HTTP 500 and at least one carried a
//     parseable per-slot body (post-#881) → per-slot classification.
//     Each slot is accepted, rejected (named Teranode code), or
//     requeued (infra-bucket code).
//   - All endpoints failed without per-slot info, or no healthy endpoints
//     existed → every slot requeued (pure batch-level infra failure).
//
// Per-endpoint outcomes are recorded into the circuit-breaker regardless
// of verdict so a peer returning 500 doesn't get sidelined when the
// 500 was a per-tx verdict, not the peer's fault. The returned
// successEndpoint is the URL of the first peer that accepted the batch
// (empty when none did).
func (p *Propagator) broadcastBatchToEndpoints(ctx context.Context, rawTxs [][]byte, batch []propagationMsg) (results []txResult, successEndpoint string) {
	start := time.Now()
	defer func() {
		metrics.PropagationBroadcastDuration.WithLabelValues("batch").Observe(time.Since(start).Seconds())
	}()
	now := time.Now()
	endpoints := p.teranodeClient.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		p.logger.Error("no healthy teranode endpoints")
		return makeRequeueResults(batch), ""
	}

	submitCtx, cancelSubmit := context.WithTimeout(ctx, 15*time.Second)
	defer cancelSubmit()
	broadcastCtx, cancelBroadcast := context.WithCancel(submitCtx)
	defer cancelBroadcast()

	resultCh := make(chan broadcastJobResult, len(endpoints))
	submitted := p.submitBroadcastJobs(broadcastCtx, endpoints, nil, rawTxs, resultCh)

	outcomes := make([]endpointOutcome, 0, submitted)
	anySuccess := false
	var perSlot []string // first endpoint's per-slot body, used when no peer succeeded
	for i := 0; i < submitted; i++ {
		result := <-resultCh
		if isCanceledByBroadcast(broadcastCtx, result.err) {
			continue
		}
		outcomes = append(outcomes, endpointOutcome{endpoint: result.endpoint, statusCode: result.statusCode})
		if result.err != nil {
			p.logger.Warn(
				"batch broadcast endpoint failed",
				zap.String("endpoint", result.endpoint),
				zap.Int("batch_size", len(batch)),
				zap.Int("status_code", result.statusCode),
				zap.Error(result.err),
			)
			// Save the first per-slot body we see for the fallback path.
			if perSlot == nil && len(result.perSlot) == len(batch) {
				perSlot = result.perSlot
			}
			continue
		}
		p.logger.Debug(
			"batch broadcast endpoint succeeded",
			zap.String("endpoint", result.endpoint),
			zap.Int("batch_size", len(batch)),
		)
		if !anySuccess {
			successEndpoint = result.endpoint
		}
		anySuccess = true
		// Early-cancel: an accepting endpoint settles the batch's network
		// verdict; siblings still in flight stop wasting time.
		cancelBroadcast()
	}
	recordBroadcastOutcomes(p.teranodeClient, outcomes)

	p.logger.Debug(
		"batch broadcast complete",
		zap.Int("batch_size", len(batch)),
		zap.Bool("any_success", anySuccess),
		zap.Int("endpoint_count", len(endpoints)),
	)

	results = make([]txResult, len(batch))

	if anySuccess {
		// Network accepted — every tx becomes ACCEPTED_BY_NETWORK.
		for i, msg := range batch {
			results[i] = txResult{
				class: txResultClassAccepted,
				status: &models.TransactionStatus{
					TxID:      msg.TXID,
					Status:    models.StatusAcceptedByNetwork,
					Timestamp: now,
				},
				rawTx:           msg.RawTx,
				successEndpoint: successEndpoint,
			}
		}
		return results, successEndpoint
	}

	if perSlot != nil {
		// All endpoints failed but at least one carried a per-slot body
		// (post-#881). Classify each slot from its line.
		for i, msg := range batch {
			class, errMsg := classifyPerSlotLine(perSlot[i])
			switch class {
			case txResultClassAccepted:
				results[i] = txResult{
					class: txResultClassAccepted,
					status: &models.TransactionStatus{
						TxID:      msg.TXID,
						Status:    models.StatusAcceptedByNetwork,
						Timestamp: now,
					},
					rawTx: msg.RawTx,
				}
			case txResultClassRejected:
				results[i] = txResult{
					class:  txResultClassRejected,
					errMsg: errMsg,
					status: &models.TransactionStatus{
						TxID:      msg.TXID,
						Status:    models.StatusRejected,
						Timestamp: now,
						ExtraInfo: errMsg,
					},
					rawTx: msg.RawTx,
				}
			default:
				results[i] = txResult{
					class:  txResultClassRequeue,
					errMsg: errMsg,
					rawTx:  msg.RawTx,
				}
			}
		}
		return results, ""
	}

	// All endpoints failed and none had a parseable per-slot body —
	// treat the whole batch as infra and requeue every tx.
	return makeRequeueResults(batch), ""
}

// makeRequeueResults builds a per-tx requeue result list for a batch
// that hit a pure infra failure (no healthy endpoints, no parseable
// per-slot body, etc.).
func makeRequeueResults(batch []propagationMsg) []txResult {
	results := make([]txResult, len(batch))
	for i, msg := range batch {
		results[i] = txResult{
			class: txResultClassRequeue,
			rawTx: msg.RawTx,
		}
	}
	return results
}

// statusPriority returns a numeric priority for broadcast result aggregation.
func statusPriority(s models.Status) int {
	switch s {
	case models.StatusAcceptedByNetwork:
		return 3
	case models.StatusSentToNetwork:
		return 2
	case models.StatusRejected:
		return 1
	default:
		return 0
	}
}
