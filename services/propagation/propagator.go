package propagation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	// InputTXIDs lists the txids this tx spends from. Populated at intake
	// so the propagator can decide eligibility without re-parsing the raw
	// bytes. Empty means coinbase or no in-flight parents.
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
	// admitCh, requeueCh, terminalCh, and drainCh feed runDispatcher's
	// single state-owning loop. The loop selects on these channels (and,
	// in production, claim.Messages()) and runs ALL dep-aware state
	// mutations — inFlight, waiters, heldMsgs, pendingMsgs, the
	// offsetTracker, and the pendingMarks map — inside the goroutine
	// that owns the loop. No locks, no atomics. See dispatcher.go.
	admitCh           chan admitRequest
	requeueCh         chan requeueRequest
	terminalCh        chan terminalEvent
	drainCh           chan drainRequest
	dispatcherCancel  context.CancelFunc
	dispatcherDone    chan struct{}
	merkleConcurrency int
	reaperInterval    time.Duration
	reaperBatchSize   int
	teranodeBatchCap  int
	broadcastWorkers  int
	maxParallelChunks int
	holderID          string
	leaseTTL          time.Duration

	// broadcastJobs feeds the persistent worker pool that runs every
	// per-endpoint POST /txs call. Replaces the previous per-broadcast
	// `go func(ep)` spawn loop so sustained 50+ TPS doesn't produce
	// constant goroutine churn — total broadcast goroutine count stays
	// bounded at broadcastWorkers regardless of flush rate. Workers
	// exit when broadcastJobs is closed in Stop().
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
	// backgroundWG tracks the long-running Start-spawned goroutines —
	// runReaper, runMerkleReplay — so Stop can wait for them to exit
	// before the surrounding app cleanup closes the store backing them.
	// Without this, a reaper mid-lease-release or mid-status-scan
	// races with store.Close and the test framework attributes the
	// resulting goroutine panic to the test's Cleanup callback.
	backgroundWG sync.WaitGroup
}

// broadcastJob is the unit of work the persistent broadcast pool consumes.
// One job represents one POST /txs call to one endpoint; the caller bundles
// a per-call result channel so it can collect outcomes from multiple
// endpoints in parallel without each worker carrying that bookkeeping.
//
// The ctx field is intentionally part of the value — the job travels through
// a channel so the cancellation token has to ride with it. The standard
// "context as first arg" pattern doesn't apply to message-passing handoffs;
// containedctx is suppressed deliberately at the type declaration.
type broadcastJob struct {
	ctx      context.Context //nolint:containedctx // travels with the work item through broadcastJobs channel
	endpoint string
	rawTxs   [][]byte
	resultCh chan<- broadcastJobResult
}

type broadcastJobResult struct {
	endpoint   string
	statusCode int
	// failures is the per-txid failure map extracted from a /txs HTTP 500
	// "Failed to process transactions:" body (Teranode upstream main, post
	// #879). Each entry is keyed by the txid embedded in
	// "[ProcessTransaction][<txid>]" and the value is the full error line
	// verbatim (e.g. "TX_INVALID (31): [ProcessTransaction][<txid>] tx is
	// invalid because..."). Absent here means accepted (after a peer 500).
	// nil for 200, transport errors, and any 5xx that doesn't match the
	// Teranode failure-list shape — those cases drive the whole-batch
	// requeue path.
	failures map[string]string
	err      error
}

// txResultClass categorizes a per-tx broadcast outcome into the action
// the caller should take. The dep-aware pipeline collapses Teranode's
// rich error vocabulary into three buckets.
type txResultClass int

const (
	// txResultClassUnknown is the zero value — a result that hasn't been
	// classified yet. Should never reach the caller; if it does,
	// processBatch's default branch treats it the same as Requeue.
	txResultClassUnknown txResultClass = iota
	// txResultClassAccepted: terminalize as ACCEPTED_BY_NETWORK, dispatcher
	// releases waiters.
	txResultClassAccepted
	// txResultClassRejected: terminalize as REJECTED, dispatcher cascade-
	// rejects descendants. errMsg carries the Teranode code (e.g.
	// "TX_INVALID (31)") so it shows up in the wallet-visible row.
	txResultClassRejected
	// txResultClassRequeue: broadcast didn't produce a verdict for this
	// tx (no peer reachable, /txs returned 4xx/5xx with no per-slot body,
	// per-slot PROCESSING / infra-bucket code). processBatch routes the
	// tx through requeueAfterDelay → dispatcher.requeueCh after a flat
	// wait, which re-runs admission so the tx gets another flush. The
	// row stays at RECEIVED in the DB; the dispatcher inFlight entry and
	// pinned Kafka offset both persist across the requeue.
	txResultClassRequeue
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
	// Reaper config drives the periodic SEEN_ON_NETWORK rebroadcast scan
	// and the lease that coordinates it across replicas.
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
		requeueCh:         make(chan requeueRequest, dispatcherChannelBuffer),
		terminalCh:        make(chan terminalEvent, dispatcherChannelBuffer),
		drainCh:           make(chan drainRequest),
	}
	// Start a dispatcher goroutine with a nil claim so tests that
	// construct via New and drive via admitCh / drainCh have a running
	// state machine without needing to invoke Start. In production
	// Start replaces this with the same loop running inside the kafka
	// ClaimHandler — see Start. The two paths can't both be live at
	// once: Start cancels this context AND waits on dispatcherDone
	// before subscribing, guaranteeing the test-mode goroutine has
	// fully exited before any production dispatcher loop runs.
	dispatcherCtx, dispatcherCancel := context.WithCancel(context.Background())
	p.dispatcherCancel = dispatcherCancel
	p.dispatcherDone = make(chan struct{})
	go func() {
		defer close(p.dispatcherDone)
		if err := p.runDispatcher(dispatcherCtx, nil, dispatcherConfig{maxPending: maxPending}); err != nil {
			p.logger.Error("test-mode dispatcher exited with error", zap.Error(err))
		}
	}()
	return p
}

// runBroadcastWorker pulls jobs off broadcastJobs and runs POST /txs against
// the named endpoint. Exits when broadcastJobs is closed (Stop()). The job's
// context governs cancellation — a winning sibling cancels the per-call
// broadcastCtx and a 15s deadline bounds worst-case wall time.
func (p *Propagator) runBroadcastWorker() {
	defer p.broadcastWG.Done()
	for job := range p.broadcastJobs {
		statusCode, failures, err := p.teranodeClient.SubmitTransactions(job.ctx, job.endpoint, job.rawTxs)
		// Non-blocking send — the caller always allocates resultCh with
		// capacity ≥ number of jobs it submits, so this never blocks. Using
		// non-blocking lets a Stop() racing with in-flight broadcasts not
		// deadlock the worker on an abandoned channel.
		select {
		case job.resultCh <- broadcastJobResult{endpoint: job.endpoint, statusCode: statusCode, failures: failures, err: err}:
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
	// goroutine. Wait for dispatcherDone before subscribing — cancel()
	// returns immediately, and the loop may still be parked on a
	// channel read when it does. Any future caller that pushes to
	// admitCh / requeueCh / terminalCh / drainCh during Start would
	// otherwise race the old goroutine.
	if p.dispatcherCancel != nil {
		p.dispatcherCancel()
		p.dispatcherCancel = nil
		if p.dispatcherDone != nil {
			<-p.dispatcherDone
			p.dispatcherDone = nil
		}
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
	p.backgroundWG.Add(1)
	go func() {
		defer p.backgroundWG.Done()
		p.runMerkleReplay(ctx)
	}()

	// Reaper: scans the status store for non-terminal rows that have
	// been stuck longer than the per-status thresholds and rebroadcasts
	// them. The reaper is the durable retry surface — processBatch
	// itself runs each tx through the broadcast pipeline exactly once
	// and relies on the reaper to retry anything that didn't reach a
	// terminal verdict.
	p.backgroundWG.Add(1)
	go func() {
		defer p.backgroundWG.Done()
		p.runReaper(ctx)
	}()

	p.logger.Info(
		"propagation service started",
		zap.Duration("reaper_interval", p.reaperInterval),
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
	// Cancel the test-mode dispatcher goroutine started in New (if
	// Start was never called and so didn't already drain it). Wait on
	// dispatcherDone so the goroutine has fully exited before Stop
	// returns — otherwise it may still be holding references to the
	// store the surrounding test cleanup is about to close.
	if p.dispatcherCancel != nil {
		p.dispatcherCancel()
		if p.dispatcherDone != nil {
			<-p.dispatcherDone
		}
	}
	// Wait for the long-running Start-spawned goroutines (reaper,
	// merkle replay) to exit before returning. The surrounding app
	// cleanup will close the store as soon as Stop returns; if the
	// reaper is mid-IterateStatusesSince or mid-lease-release on a
	// closed store, the goroutine panics and the test framework
	// attributes the failure to t.Cleanup. Their parent ctx has
	// already been canceled by the caller, so this only blocks on
	// in-flight scan/release work.
	p.backgroundWG.Wait()
	return consumerErr
}

// handleMessage decodes the propagation envelope and queues it for the next
// flushBatch. Cheap on purpose: no HTTP, no DB.
//
// F-024 durability is preserved at the batch level: processBatch runs
// RegisterBatchWithResults before broadcasting, and any tx whose registration
// failed is excluded from the broadcast and left at RECEIVED for the reaper
// to retry on its next tick.
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
// returns the subset that registered successfully. Txs whose /watch failed
// returns the successfully-registered subset for broadcast and the failed
// subset for caller-driven requeue. Per-tx errors: failures (network
// blip on one /watch call, etc.) go to the failed slice so processBatch
// can requeue them through the dispatcher after a short flat wait.
//
// When the merkle integration is disabled (client nil or no callback URL),
// every tx is treated as registered.
func (p *Propagator) registerBatch(ctx context.Context, batch []propagationMsg) (registered, failed []propagationMsg) {
	if p.merkleClient == nil || p.cfg.CallbackURL == "" {
		return batch, nil
	}

	regs := make([]merkleservice.Registration, len(batch))
	for i, m := range batch {
		regs[i] = merkleservice.Registration{
			TxID:          m.TXID,
			CallbackURL:   p.cfg.CallbackURL,
			CallbackToken: p.cfg.CallbackToken,
		}
	}

	start := time.Now()
	errs := p.merkleClient.RegisterBatchWithResults(ctx, regs, p.merkleConcurrency)
	metrics.PropagationMerkleRegisterDuration.Observe(time.Since(start).Seconds())

	registered = make([]propagationMsg, 0, len(batch))
	failed = make([]propagationMsg, 0)
	successTxIDs := make([]string, 0, len(batch))
	var sampleErr error
	for i, err := range errs {
		if err == nil {
			registered = append(registered, batch[i])
			successTxIDs = append(successTxIDs, batch[i].TXID)
			continue
		}
		failed = append(failed, batch[i])
		if sampleErr == nil {
			sampleErr = err
		}
		metrics.PropagationMerkleRegisterFailures.WithLabelValues("register_error").Inc()
	}

	switch {
	case len(failed) == 0:
		metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok").Inc()
	case len(registered) == 0:
		metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("all_failed").Inc()
	default:
		metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("partial").Inc()
	}
	if len(failed) > 0 {
		p.logger.Warn(
			"merkle-service /watch partial/all failure; requeueing failed subset",
			zap.Int("batch_size", len(batch)),
			zap.Int("failed", len(failed)),
			zap.Int("registered", len(registered)),
			zap.Error(sampleErr),
		)
	}

	// Stamp merkle_registered_at on the successful subset so the startup
	// replay loop can skip them. A store-write failure here must not block
	// broadcast — the mark is a hint, not part of the F-024 invariant.
	if len(successTxIDs) > 0 {
		if err := p.store.MarkMerkleRegisteredByTxIDs(ctx, successTxIDs, time.Now()); err != nil {
			p.logger.Warn(
				"mark merkle-registered failed",
				zap.Int("count", len(successTxIDs)),
				zap.Error(err),
			)
		}
	}
	return registered, failed
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

// classifyFailureLine maps one Teranode /txs failure line into a dispatcher
// action bucket. The line is the full UserMessage produced by Teranode,
// e.g. "TX_INVALID (31): [ProcessTransaction][<txid>] ...". A line in the
// terminal-rejection bucket (TX_INVALID, UTXO_SPENT, etc.) → rejected.
// Anything else (PROCESSING wrapper, unrecognized code, malformed line) →
// requeue so the next attempt has a chance to produce a verdict. errMsg is
// the line verbatim so wallet rows surface the actual Teranode code.
func classifyFailureLine(line string) (txResultClass, string) {
	if isTeranodeTerminalCode(line) {
		return txResultClassRejected, line
	}
	return txResultClassRequeue, line
}

// teranodeTerminalCodes is the set of upstream Teranode error code names
// that classify a tx as terminally rejected. PROCESSING and anything else
// falls through to requeue (infra-bucket failure that should be retried).
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

// isTeranodeTerminalCode reports whether a failure line names a Teranode
// code in the terminal-rejection bucket. Matches the leading NAME of
// "NAME (num): <message>" (the first whitespace-delimited token).
// Anything else (PROCESSING wrapper, network-only codes, unrecognized
// strings) is treated as infra → requeue.
func isTeranodeTerminalCode(line string) bool {
	name := line
	if idx := strings.IndexByte(name, ' '); idx >= 0 {
		name = name[:idx]
	}
	_, ok := teranodeTerminalCodes[name]
	return ok
}

// processBatch handles one drained batch:
//  1. Register every tx with merkle-service. Txs whose /watch failed are
//     requeued through the dispatcher after a short flat wait.
//  2. Broadcast the registered subset to teranode in /txs chunks.
//  3. For each per-tx result, apply the corresponding action:
//     - Accepted → terminal ACCEPTED row + dispatcher notify (releases waiters)
//     - Rejected → terminal REJECTED row + dispatcher notify (cascade)
//     - Requeue  → requeue through the dispatcher after a short flat wait
//     (transient infra failure: peer 5xx with no per-slot info, no peer
//     reachable, per-slot infra code). The Kafka offset stays pinned via
//     the existing inFlight entry.
//
// Failure paths are absorbed internally — there's no caller that reacts
// to an aggregate error here, so the function returns void.
func (p *Propagator) processBatch(ctx context.Context, batch []propagationMsg) {
	registered, failedRegister := p.registerBatch(ctx, batch)
	if len(failedRegister) > 0 {
		p.requeueAfterDelay(ctx, failedRegister)
	}
	if len(registered) == 0 {
		return
	}
	batch = registered

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

	rawTxs := make([][]byte, len(batch))
	for i, msg := range batch {
		rawTxs[i] = msg.RawTx
	}
	results := p.broadcastInChunks(ctx, batch, rawTxs)

	seenEndpoints := make(map[string]struct{})
	var successEndpoints []string
	var accepted, rejected int
	terminalStatuses := make([]*models.TransactionStatus, 0, len(results))
	var toRequeue []propagationMsg
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
		default:
			// Requeue / Unknown: transient infra failure. Collect for
			// requeue after a short flat wait so the dispatcher re-runs
			// admission (dep-aware) and the tx flows through the next
			// flush.
			toRequeue = append(toRequeue, batch[i])
		}
	}

	p.applyTerminalStatuses(ctx, terminalStatuses, accepted, rejected)
	if len(toRequeue) > 0 {
		p.requeueAfterDelay(ctx, toRequeue)
	}
	metrics.PropagationOutcomeTotal.WithLabelValues("accepted").Add(float64(accepted))
	metrics.PropagationOutcomeTotal.WithLabelValues("rejected").Add(float64(rejected))
	metrics.PropagationOutcomeTotal.WithLabelValues("skipped").Add(float64(len(toRequeue)))

	p.logger.Info(
		"batch propagated",
		zap.Int("count", len(batch)),
		zap.Int("accepted", accepted),
		zap.Int("rejected", rejected),
		zap.Int("requeued", len(toRequeue)),
		zap.Strings("success_endpoints", successEndpoints),
	)
}

// requeueDelay is the flat wait before a requeue lands back on the
// dispatcher. Long enough to ride out a brief upstream blip; short
// enough that submitter-visible latency stays acceptable. Tunable if
// observed retry traffic suggests a different cadence works better.
const requeueDelay = 2 * time.Second

// requeueAfterDelay schedules a delayed requeue of msgs through the
// dispatcher. Spawns a goroutine that sleeps requeueDelay then sends
// each msg to requeueCh. The goroutine bails on ctx cancellation so
// claim revocation and shutdown don't hold txs in limbo.
func (p *Propagator) requeueAfterDelay(ctx context.Context, msgs []propagationMsg) {
	if len(msgs) == 0 {
		return
	}
	// Track pending requeue goroutines on the metric so sustained
	// upstream pressure shows up in dashboards without needing to
	// introspect goroutines. Per-call goroutine + timer is intentional
	// (see the requeueDelay comment) — the gauge is the observability
	// hook for catching the failure mode where TPS × requeueDelay
	// fans out further than capacity tolerates.
	metrics.PropagationPendingRequeues.Inc()
	go func(msgs []propagationMsg) {
		defer metrics.PropagationPendingRequeues.Dec()
		select {
		case <-time.After(requeueDelay):
		case <-ctx.Done():
			return
		}
		for _, m := range msgs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			p.requeueToDispatcher(m)
		}
	}(msgs)
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

// broadcastChunk broadcasts a single chunk (≤ teranodeBatchCap) via POST
// /txs and writes per-tx classifications into out. /txs handles any chunk
// size — a chunk of one is just one tx's bytes — so there's a single
// classification path regardless of count.
func (p *Propagator) broadcastChunk(ctx context.Context, chunk []propagationMsg, rawTxs [][]byte, out []txResult) {
	metrics.PropagationChunkTotal.WithLabelValues("none").Inc()
	results, _ := p.broadcastBatchToEndpoints(ctx, rawTxs, chunk)
	copy(out, results)
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
// set of per-endpoint outcomes from one broadcast attempt:
//
//   - statusCode == 0 (no HTTP response): RecordFailure (per-peer reachability).
//   - At least one 2xx in the set: each non-2xx is RecordBroadcastFailure,
//     each 2xx is RecordSuccess.
//   - Zero 2xx but every responder returned non-2xx: unanimous network reject.
//     The peers responded and they all agree the tx is bad — don't penalize
//     them for being correct. RecordSuccess for responders, RecordFailure for
//     transport errors.
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
// Each job POSTs the same rawTxs slice to its endpoint.
//
// Sends block on the worker pool's job channel — when the pool is
// saturated, backpressure flows back to the caller (and ultimately to
// the dispatcher's pendingMsgs cap). ctx cancellation unblocks a stuck
// send so shutdown and per-claim revocation observe the cancel.
//
// In tests that construct a Propagator without Start() (broadcastRunning
// stays false), the channel is unbuffered for delivery and falls through
// to a per-endpoint goroutine — preserves test ergonomics without
// affecting production behavior.
func (p *Propagator) submitBroadcastJobs(ctx context.Context, endpoints []string, rawTxs [][]byte, resultCh chan<- broadcastJobResult) int {
	submitted := 0
	useChannel := p.broadcastRunning.Load()
	for _, endpoint := range endpoints {
		job := broadcastJob{
			ctx:      ctx,
			endpoint: endpoint,
			rawTxs:   rawTxs,
			resultCh: resultCh,
		}
		if useChannel {
			select {
			case p.broadcastJobs <- job:
				submitted++
				continue
			case <-ctx.Done():
				return submitted
			}
		}
		submitted++
		go func(j broadcastJob) {
			statusCode, failures, err := p.teranodeClient.SubmitTransactions(j.ctx, j.endpoint, j.rawTxs)
			select {
			case j.resultCh <- broadcastJobResult{endpoint: j.endpoint, statusCode: statusCode, failures: failures, err: err}:
			default:
			}
		}(job)
	}
	return submitted
}

// broadcastBatchToEndpoints submits a batch to each healthy teranode
// endpoint via /txs and produces per-tx classifications:
//
//   - Any endpoint returning 200 → every tx accepted (the peer accepted
//     the whole batch; per-tx failure info from other peers' 500 responses
//     is superseded).
//   - No endpoint returned 200, but at least one returned the Teranode
//     "Failed to process transactions:" 500 body → per-tx classification.
//     Each txid named in the failure body is rejected (terminal Teranode
//     code) or requeued (infra-bucket code); every other tx in the batch
//     is treated as accepted.
//   - All endpoints failed without a parseable Teranode failure body, or
//     no healthy endpoints existed → every tx requeued (pure batch-level
//     infra failure).
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
	submitted := p.submitBroadcastJobs(broadcastCtx, endpoints, rawTxs, resultCh)

	outcomes := make([]endpointOutcome, 0, submitted)
	anySuccess := false
	var failures map[string]string // first endpoint's failure map, used when no peer succeeded
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
			// Save the first parseable failure map we see for the fallback path.
			if failures == nil && len(result.failures) > 0 {
				failures = result.failures
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

	if failures != nil {
		// All endpoints failed but at least one carried a Teranode
		// failure-list body. A txid in the map failed; absent means the
		// peer accepted that tx into its pipeline.
		for i, msg := range batch {
			line, failed := failures[strings.ToLower(msg.TXID)]
			if !failed {
				results[i] = txResult{
					class: txResultClassAccepted,
					status: &models.TransactionStatus{
						TxID:      msg.TXID,
						Status:    models.StatusAcceptedByNetwork,
						Timestamp: now,
					},
					rawTx: msg.RawTx,
				}
				continue
			}
			class, errMsg := classifyFailureLine(line)
			switch class {
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

	// All endpoints failed and none had a parseable Teranode failure body —
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
