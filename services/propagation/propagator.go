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

// reaperLeaseName is the well-known key every replica uses to coordinate
// reaper ownership. A single lease per deployment — if you run separate
// propagation deployments against the same store, give them distinct consumer
// groups (which already differ by namespace) or override this constant.
const reaperLeaseName = "propagation-reaper"

type propagationMsg struct {
	TXID string `json:"txid"`
	// RawTx is the serialized transaction as raw bytes. encoding/json encodes
	// []byte as base64 — still smaller than hex (4/3 expansion vs 2x) and
	// avoids the per-hop hex encode/decode the pipeline used to do.
	RawTx []byte `json:"raw_tx"`
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

	mu                sync.Mutex
	pendingMsgs       []propagationMsg
	maxPending        int
	merkleConcurrency int
	retryMaxAttempts  int
	retryBackoffMs    int
	reaperInterval    time.Duration
	reaperBatchSize   int
	teranodeBatchCap  int
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
}

// broadcastJob is the unit of work the persistent broadcast pool consumes.
// One job represents one HTTP call to one endpoint; the caller bundles a
// per-call result channel so it can collect outcomes from multiple endpoints
// in parallel without each worker carrying that bookkeeping.
type broadcastJob struct {
	ctx      context.Context
	endpoint string
	// Exactly one of rawTx (single /tx) or rawTxs (batch /txs) is set.
	rawTx    []byte
	rawTxs   [][]byte
	resultCh chan<- broadcastJobResult
}

type broadcastJobResult struct {
	endpoint   string
	statusCode int
	err        error
}

// broadcastWorkers caps the total in-flight HTTP submit goroutines across
// every chunk and every endpoint. Sized to comfortably saturate the typical
// 6-10 datahub endpoint fleet while bounding peak goroutine count under
// load. Each worker call is bounded by the per-job context (15s) so a stuck
// endpoint can't permanently consume a slot.
const broadcastWorkers = 64

// broadcastJobBuffer sizes the job channel between broadcast helpers and the
// worker pool. Generous enough that flush-time fan-out doesn't block in
// steady state; bounded so a stalled pool can't grow unboundedly.
const broadcastJobBuffer = 1024

// New constructs a Propagator. leaser may be nil, in which case the reaper
// runs unguarded — appropriate for tests and single-process deployments that
// don't need coordination. In production every replica should receive a
// non-nil Leaser so only one reaper is active at a time across the cluster.
func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, publisher events.Publisher, st store.Store, leaser store.Leaser, tc *teranode.Client, mc *merkleservice.Client) *Propagator {
	merkleConcurrency := cfg.Propagation.MerkleConcurrency
	if merkleConcurrency <= 0 {
		merkleConcurrency = 10
	}
	retryMax := cfg.Propagation.RetryMaxAttempts
	if retryMax <= 0 {
		retryMax = 5
	}
	retryBackoff := cfg.Propagation.RetryBackoffMs
	if retryBackoff <= 0 {
		retryBackoff = 500
	}
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
		// Default to 3× the tick interval so a slow or delayed tick doesn't
		// trigger a false-positive failover. This is the standard safety
		// factor for heartbeat-style leases.
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
	return &Propagator{
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
		retryMaxAttempts:  retryMax,
		retryBackoffMs:    retryBackoff,
		reaperInterval:    reaperInterval,
		reaperBatchSize:   reaperBatch,
		teranodeBatchCap:  teranodeBatchCap,
		holderID:          newHolderID(),
		leaseTTL:          leaseTTL,
		broadcastJobs:     make(chan broadcastJob, broadcastJobBuffer),
	}
}

// runBroadcastWorker pulls jobs off broadcastJobs and runs the HTTP submit
// against the named endpoint. Exits when broadcastJobs is closed (Stop()).
// The job's context governs cancellation — a winning sibling cancels the
// per-call broadcastCtx and a 15s deadline bounds worst-case wall time.
func (p *Propagator) runBroadcastWorker() {
	defer p.broadcastWG.Done()
	for job := range p.broadcastJobs {
		var statusCode int
		var err error
		if job.rawTxs != nil {
			statusCode, err = p.teranodeClient.SubmitTransactions(job.ctx, job.endpoint, job.rawTxs)
		} else {
			statusCode, err = p.teranodeClient.SubmitTransaction(job.ctx, job.endpoint, job.rawTx)
		}
		// Non-blocking send — the caller always allocates resultCh with
		// capacity ≥ number of jobs it submits, so this never blocks. Using
		// non-blocking lets a Stop() racing with in-flight broadcasts not
		// deadlock the worker on an abandoned channel.
		select {
		case job.resultCh <- broadcastJobResult{endpoint: job.endpoint, statusCode: statusCode, err: err}:
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

// publishStatus fans a post-broadcast status update onto the events
// Publisher. Non-fatal: the durable store row is already written, and SSE
// catchup recovers any dropped events.
func (p *Propagator) publishStatus(ctx context.Context, status *models.TransactionStatus) {
	if p.publisher == nil || status == nil {
		return
	}
	if err := p.publisher.Publish(ctx, status); err != nil {
		p.logger.Warn("failed to publish status update",
			zap.String("txid", status.TxID),
			zap.String("status", string(status.Status)),
			zap.Error(err),
		)
	}
}

func (p *Propagator) Start(ctx context.Context) error {
	consumer, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:     p.producer.Broker(),
		GroupID:    p.cfg.Kafka.ConsumerGroup + "-propagation",
		Topics:     []string{kafka.TopicPropagation},
		Handler:    p.handleMessage,
		FlushFunc:  p.flushBatch,
		Producer:   p.producer,
		MaxRetries: p.cfg.Kafka.MaxRetries,
		Logger:     p.logger,
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
	for i := 0; i < broadcastWorkers; i++ {
		p.broadcastWG.Add(1)
		go p.runBroadcastWorker()
	}

	// Kick off the durable-retry reaper alongside the Kafka consumer. It owns
	// all rebroadcast work for PENDING_RETRY rows, decoupled from the incoming
	// message flush cycle so a retry storm can't starve live traffic.
	go p.runReaper(ctx)

	// Replay in-flight registrations to merkle-service. One-shot; exits on
	// its own. Compensates for /watch state loss on the merkle-service side
	// (recreated namespace, data wipe, schema migration) which otherwise
	// silently disables STUMP callbacks for every previously-submitted tx.
	go p.runMerkleReplay(ctx)

	p.logger.Info("propagation service started",
		zap.Duration("reaper_interval", p.reaperInterval),
		zap.Int("reaper_batch_size", p.reaperBatchSize),
		zap.Int("broadcast_workers", broadcastWorkers),
	)
	return consumer.Run(ctx)
}

func (p *Propagator) Stop() error {
	p.logger.Info("stopping propagation service")
	var consumerErr error
	if p.consumer != nil {
		consumerErr = p.consumer.Close()
	}
	// Closing broadcastJobs lets every worker drain its current iteration
	// and exit. Flip broadcastRunning first so any in-flight submit fan-out
	// falls back to the goroutine path rather than pushing into a channel
	// we're about to close.
	if p.broadcastRunning.Swap(false) {
		close(p.broadcastJobs)
		p.broadcastWG.Wait()
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

	p.mu.Lock()
	if p.maxPending > 0 && len(p.pendingMsgs) >= p.maxPending {
		depth := len(p.pendingMsgs)
		p.mu.Unlock()
		metrics.PropagationPendingDepth.Set(float64(depth))
		// Returning an error here surfaces back to the consumer's retry+DLQ
		// path. That's the desired behavior under sustained backpressure —
		// shedding into DLQ is preferable to ballooning memory until OOM.
		return fmt.Errorf("propagation pending queue full (depth=%d, max=%d)", depth, p.maxPending)
	}
	p.pendingMsgs = append(p.pendingMsgs, propMsg)
	depth := len(p.pendingMsgs)
	p.mu.Unlock()
	metrics.PropagationPendingDepth.Set(float64(depth))

	return nil
}

// flushBatch processes all accumulated messages as a batch. Retry work
// belongs to the reaper goroutine — it is no longer coupled to the consumer's
// drain-flush cycle, so live ingest doesn't have to wait on rebroadcasts.
//
// The context comes from the current Kafka claim — it is canceled when the
// claim ends (shutdown or rebalance). Downstream HTTP broadcasts and store
// writes observe that cancellation and unwind cleanly, so a revoked partition
// doesn't keep doing work on behalf of a partition it no longer owns.
func (p *Propagator) flushBatch(ctx context.Context) error {
	p.mu.Lock()
	batch := p.pendingMsgs
	p.pendingMsgs = nil
	p.mu.Unlock()
	metrics.PropagationPendingDepth.Set(0)

	if len(batch) == 0 {
		return nil
	}
	return p.processBatch(ctx, batch)
}

// registerBatch invokes merkle-service /watch for every tx in the batch and
// partitions the result into "registered" (broadcast-eligible) and "failed"
// (routed to handleRetryableFailure). Preserves F-024: every tx that
// downstream broadcasts is provably registered.
//
// When the merkle integration is disabled (client nil or no callback URL),
// every tx is treated as registered — there's no registration step to fail.
func (p *Propagator) registerBatch(ctx context.Context, batch []propagationMsg) (registered []propagationMsg) {
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

	start := time.Now()
	errs := p.merkleClient.RegisterBatchWithResults(ctx, regs, p.merkleConcurrency)
	metrics.PropagationMerkleRegisterDuration.Observe(time.Since(start).Seconds())

	registered = make([]propagationMsg, 0, len(batch))
	var failedCount int
	for i, err := range errs {
		if err == nil {
			registered = append(registered, batch[i])
			continue
		}
		failedCount++
		metrics.PropagationMerkleRegisterFailures.WithLabelValues("register_error").Inc()
		// Mirror the prior per-message contract: a failed register must not
		// broadcast and must not be lost. Route to durable PENDING_RETRY so
		// the reaper re-attempts registration+broadcast on its own cadence.
		p.handleRetryableFailure(ctx, batch[i].TXID, batch[i].RawTx)
	}
	if failedCount > 0 {
		p.logger.Warn("merkle-service registration partial failure",
			zap.Int("batch_size", len(batch)),
			zap.Int("failed", failedCount),
			zap.Int("registered", len(registered)),
		)
	}
	return registered
}

// txResult carries per-tx outcome of a broadcast, used by both the initial
// processBatch path and the reaper. successEndpoint is the URL of the peer
// whose response drove the accepted status (empty when no peer accepted or
// the broadcast produced no verdict), useful for operator-visible logs.
type txResult struct {
	status          *models.TransactionStatus
	errMsg          string
	rawTx           []byte
	successEndpoint string
	// acknowledged mirrors broadcastResult.Acknowledged: a peer responded
	// with 2xx but no definitive verdict (typically all 202s). Lets
	// processBatch tell apart "tx is in flight" (don't retry) from "no
	// peer was reachable" (retry durably).
	acknowledged bool
}

// processBatch handles a batch of propagation messages:
//  1. Register every tx with merkle-service (batched, bounded concurrency).
//     Failed-register txs go to handleRetryableFailure and are EXCLUDED from
//     the broadcast pass — preserves the F-024 "register before broadcast"
//     invariant at batch granularity.
//  2. Broadcast registered txs to teranode endpoints, chunked to
//     teranodeBatchCap.
//  3. Update status for each transaction.
func (p *Propagator) processBatch(ctx context.Context, batch []propagationMsg) error {
	// Step 1: register all txs with merkle-service in parallel. Drops any tx
	// whose registration failed — that tx is already queued for durable retry
	// via handleRetryableFailure inside registerBatch.
	batch = p.registerBatch(ctx, batch)
	if len(batch) == 0 {
		return nil
	}

	// Log batch summary for traceability
	txidSample := make([]string, 0, 5)
	for i, msg := range batch {
		if i >= 5 {
			break
		}
		txidSample = append(txidSample, msg.TXID)
	}
	p.logger.Info("processing batch",
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

	// Step 2: Update status for each transaction, with retry classification
	seenEndpoints := make(map[string]struct{})
	var successEndpoints []string
	var accepted, rejected, retryable, noVerdict int
	for i, res := range results {
		if res.successEndpoint != "" {
			if _, ok := seenEndpoints[res.successEndpoint]; !ok {
				seenEndpoints[res.successEndpoint] = struct{}{}
				successEndpoints = append(successEndpoints, res.successEndpoint)
			}
		}
		if res.status == nil {
			noVerdict++
			// Two sub-cases:
			//   - acknowledged: at least one peer responded 2xx (typically
			//     202 — accepted, will report back). Tx is in flight — don't
			//     queue a retry, the merkle-service callback / next broadcast
			//     will move it forward.
			//   - !acknowledged: no peer was reachable at all (every endpoint
			//     sidelined, or every responder canceled-by-broadcast). The
			//     tx is stuck in RECEIVED unless we route to PENDING_RETRY so
			//     the reaper picks it up on its next tick. This is the fix
			//     for the 02:07 EDT incident where ~1.6M txs sat in RECEIVED
			//     forever while the breaker had sidelined every endpoint.
			if !res.acknowledged {
				p.handleRetryableFailure(ctx, batch[i].TXID, res.rawTx)
			}
			continue
		}
		if res.status.Status == models.StatusRejected && IsRetryableError(res.errMsg) {
			retryable++
			p.handleRetryableFailure(ctx, batch[i].TXID, res.rawTx)
			continue
		}
		switch res.status.Status {
		case models.StatusAcceptedByNetwork:
			accepted++
		case models.StatusRejected:
			rejected++
		default:
			// Other statuses (Mined, SeenOnNetwork, etc.) flow through
			// without affecting the accepted/rejected counters.
		}
		if err := p.store.UpdateStatus(ctx, res.status); err != nil {
			p.logger.Error("failed to update status",
				zap.String("txid", batch[i].TXID),
				zap.Error(err),
			)
			continue
		}
		p.publishStatus(ctx, res.status)
	}
	metrics.PropagationOutcomeTotal.WithLabelValues("accepted").Add(float64(accepted))
	metrics.PropagationOutcomeTotal.WithLabelValues("rejected").Add(float64(rejected))
	metrics.PropagationOutcomeTotal.WithLabelValues("retryable").Add(float64(retryable))
	metrics.PropagationOutcomeTotal.WithLabelValues("no_verdict").Add(float64(noVerdict))

	p.logger.Info("batch propagated",
		zap.Int("count", len(batch)),
		zap.Strings("success_endpoints", successEndpoints),
	)
	return nil
}

// maxParallelChunks caps how many chunk broadcasts run concurrently. Each
// chunk already fans out to every healthy endpoint, so the real concurrency is
// maxParallelChunks × len(endpoints). Keep this modest so a huge flush doesn't
// open thousands of sockets at once.
const maxParallelChunks = 4

// fallbackParallelism caps concurrent per-tx broadcasts when an all-rejected
// chunk falls back to per-tx classification. Each single-tx broadcast already
// fans out across endpoints, so the effective in-flight count per chunk is
// fallbackParallelism × len(endpoints). Sized to keep a single failing chunk
// from monopolising the HTTP client's connection pool.
const fallbackParallelism = 8

// broadcastInChunks splits a batch into teranodeBatchCap-sized chunks and
// broadcasts each via /txs, falling back to per-tx /tx within a chunk only
// when that chunk's batch broadcast is all-rejected. Chunks run in parallel
// bounded by maxParallelChunks so a large flush doesn't serialize behind one
// slow endpoint. Returns per-tx results in the same order as the input.
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

	sem := make(chan struct{}, maxParallelChunks)
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

// broadcastChunk broadcasts a single chunk (≤ teranodeBatchCap). Single-tx
// chunks go to /tx; multi-tx chunks try /txs first and fall back to per-tx
// only on all-rejected. The per-tx fallback is logged as a single summary
// line to avoid flooding the log with one entry per transaction.
func (p *Propagator) broadcastChunk(ctx context.Context, chunk []propagationMsg, rawTxs [][]byte, out []txResult) {
	if len(chunk) == 1 {
		metrics.PropagationChunkTotal.WithLabelValues("none").Inc()
		br := p.broadcastSingleToEndpoints(ctx, rawTxs[0], chunk[0].TXID)
		out[0] = txResult{status: br.Status, errMsg: br.ErrorMsg, rawTx: chunk[0].RawTx, successEndpoint: br.SuccessEndpoint, acknowledged: br.Acknowledged}
		return
	}

	batchStatuses, batchSuccessEndpoint := p.broadcastBatchToEndpoints(ctx, rawTxs, chunk)
	allRejected := len(batchStatuses) > 0
	for _, s := range batchStatuses {
		if s != nil && s.Status != models.StatusRejected {
			allRejected = false
			break
		}
	}
	if !allRejected {
		metrics.PropagationChunkTotal.WithLabelValues("none").Inc()
		for i, s := range batchStatuses {
			out[i] = txResult{status: s, rawTx: chunk[i].RawTx, successEndpoint: batchSuccessEndpoint}
		}
		return
	}
	metrics.PropagationChunkTotal.WithLabelValues("per_tx_after_all_rejected").Inc()

	// Fallback: per-tx classification for this chunk only. Summarize instead
	// of logging per call — one line per fallback, not N.
	//
	// Cap concurrency so a 100-tx chunk in all-rejected state doesn't spawn
	// 100 goroutines × N endpoints of in-flight HTTP requests at once. Each
	// single-tx broadcast already fans out across endpoints internally.
	var accepted, rejected, retryable int
	var mu sync.Mutex
	sem := make(chan struct{}, fallbackParallelism)
	var wg sync.WaitGroup
	for i, msg := range chunk {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			br := p.broadcastSingleToEndpoints(ctx, rawTxs[i], msg.TXID)
			out[i] = txResult{status: br.Status, errMsg: br.ErrorMsg, rawTx: msg.RawTx, successEndpoint: br.SuccessEndpoint, acknowledged: br.Acknowledged}
			mu.Lock()
			defer mu.Unlock()
			switch {
			case br.Status == nil:
				// no verdict (202 or all timed out)
			case br.Status.Status == models.StatusAcceptedByNetwork:
				accepted++
			case br.Status.Status == models.StatusRejected:
				if IsRetryableError(br.ErrorMsg) {
					retryable++
				} else {
					rejected++
				}
			}
		}()
	}
	wg.Wait()
	p.logger.Info("per-tx fallback complete",
		zap.Int("chunk_size", len(chunk)),
		zap.Int("accepted", accepted),
		zap.Int("rejected", rejected),
		zap.Int("retryable", retryable),
	)
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
const inlineRetryAttempts = 2

// inlineRetryDelay is the base sleep between inline retry attempts.
var inlineRetryDelay = 100 * time.Millisecond

// broadcastSingleToEndpoints submits a single transaction to each healthy
// teranode endpoint using POST /tx. On the first accepting endpoint (200 OK)
// the shared broadcast context is canceled so slower sibling requests don't
// gate wall-time on the slowest peer. Per-endpoint outcomes are recorded into
// the teranode client's circuit-breaker so repeatedly failing peers are
// sidelined from future broadcasts.
//
// Transient all-failure broadcasts (every endpoint returned a retryable error)
// are retried inline up to inlineRetryAttempts times before returning, so a
// brief network blip doesn't force a 30s PENDING_RETRY trip.
func (p *Propagator) broadcastSingleToEndpoints(ctx context.Context, rawTx []byte, txid string) broadcastResult {
	var result broadcastResult
	attempt := 0
	for ; attempt <= inlineRetryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return result
			case <-time.After(time.Duration(attempt) * inlineRetryDelay):
			}
		}
		result = p.broadcastSingleOnce(ctx, rawTx, txid)
		// Accepted (200) or no-verdict (202/all timeouts) — no point retrying.
		if result.SuccessEndpoint != "" || result.Status == nil {
			if attempt > 0 {
				metrics.PropagationInlineRetryTotal.WithLabelValues("recovered").Inc()
			}
			return result
		}
		// Only retry if the aggregate outcome looks transient. Non-retryable
		// rejections are terminal and stay terminal.
		if !IsRetryableError(result.ErrorMsg) {
			return result
		}
	}
	metrics.PropagationInlineRetryTotal.WithLabelValues("exhausted").Inc()
	return result
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
//     the tx is bad — penalising them would punish the messenger. Treat as
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
			var err error
			if j.rawTxs != nil {
				statusCode, err = p.teranodeClient.SubmitTransactions(j.ctx, j.endpoint, j.rawTxs)
			} else {
				statusCode, err = p.teranodeClient.SubmitTransaction(j.ctx, j.endpoint, j.rawTx)
			}
			select {
			case j.resultCh <- broadcastJobResult{endpoint: j.endpoint, statusCode: statusCode, err: err}:
			default:
			}
		}(job)
	}
	return submitted
}

// broadcastBatchToEndpoints submits all transactions to each healthy teranode
// endpoint using the batch POST /txs endpoint. Binary outcome matches the
// original: any endpoint success → AcceptedByNetwork for all, all fail →
// Rejected for all. The first accepting endpoint cancels sibling requests so
// a slow peer doesn't gate wall-time. Per-endpoint outcomes are recorded into
// the circuit-breaker regardless of the batch verdict. The returned
// successEndpoint is the URL of the first peer that accepted the batch (empty
// if none did) — surfaced so operator logs can show which peer served a batch.
func (p *Propagator) broadcastBatchToEndpoints(ctx context.Context, rawTxs [][]byte, batch []propagationMsg) (statuses []*models.TransactionStatus, successEndpoint string) {
	start := time.Now()
	defer func() {
		metrics.PropagationBroadcastDuration.WithLabelValues("batch").Observe(time.Since(start).Seconds())
	}()
	endpoints := p.teranodeClient.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		p.logger.Error("no healthy teranode endpoints")
		return make([]*models.TransactionStatus, len(batch)), ""
	}

	submitCtx, cancelSubmit := context.WithTimeout(ctx, 15*time.Second)
	defer cancelSubmit()
	broadcastCtx, cancelBroadcast := context.WithCancel(submitCtx)
	defer cancelBroadcast()

	resultCh := make(chan broadcastJobResult, len(endpoints))
	submitted := p.submitBroadcastJobs(broadcastCtx, endpoints, nil, rawTxs, resultCh)

	// Collect every non-canceled result first so the circuit-breaker can
	// distinguish "peer disagrees" (one of many failed) from "network
	// rejects this batch" (all responding peers agreed it's bad). The
	// latter case must NOT penalize the peers — they're behaving correctly.
	outcomes := make([]endpointOutcome, 0, submitted)
	anySuccess := false
	for i := 0; i < submitted; i++ {
		result := <-resultCh
		if isCanceledByBroadcast(broadcastCtx, result.err) {
			continue
		}
		outcomes = append(outcomes, endpointOutcome{endpoint: result.endpoint, statusCode: result.statusCode})
		if result.err != nil {
			p.logger.Warn("batch broadcast endpoint failed",
				zap.String("endpoint", result.endpoint),
				zap.Int("batch_size", len(batch)),
				zap.Int("status_code", result.statusCode),
				zap.Error(result.err),
			)
			continue
		}
		p.logger.Debug("batch broadcast endpoint succeeded",
			zap.String("endpoint", result.endpoint),
			zap.Int("batch_size", len(batch)),
		)
		if !anySuccess {
			successEndpoint = result.endpoint
		}
		anySuccess = true
		// Early-cancel: binary verdict is already known once any endpoint
		// accepts; siblings running against slow peers stop wasting time.
		cancelBroadcast()
	}
	recordBroadcastOutcomes(p.teranodeClient, outcomes)

	p.logger.Debug("batch broadcast complete",
		zap.Int("batch_size", len(batch)),
		zap.Bool("any_success", anySuccess),
		zap.Int("endpoint_count", len(endpoints)),
	)

	now := time.Now()
	statuses = make([]*models.TransactionStatus, len(batch))
	status := models.StatusRejected
	if anySuccess {
		status = models.StatusAcceptedByNetwork
	}
	for i, msg := range batch {
		statuses[i] = &models.TransactionStatus{
			TxID:      msg.TXID,
			Status:    status,
			Timestamp: now,
		}
	}

	return statuses, successEndpoint
}

// handleRetryableFailure marks a tx for durable retry. The two-call pattern
// (BumpRetryCount then SetPendingRetryFields) lets us compute the real
// exponential backoff from the post-increment count without double-
// incrementing. If retry_count exceeds retryMaxAttempts, the tx is rejected
// immediately and its retry bins are cleared.
func (p *Propagator) handleRetryableFailure(ctx context.Context, txid string, rawTx []byte) {
	retryCount, err := p.store.BumpRetryCount(ctx, txid)
	if err != nil {
		p.logger.Error("failed to bump retry count", zap.String("txid", txid), zap.Error(err))
		return
	}

	if retryCount > p.retryMaxAttempts {
		if err := p.store.ClearRetryState(ctx, txid, models.StatusRejected, "broadcast retries exhausted"); err != nil {
			p.logger.Error("failed to reject after retries exhausted", zap.String("txid", txid), zap.Error(err))
		}
		return
	}

	nextRetryAt := ComputeBackoff(p.retryBackoffMs, retryCount)
	if err := p.store.SetPendingRetryFields(ctx, txid, rawTx, nextRetryAt); err != nil {
		p.logger.Error("failed to set pending retry fields", zap.String("txid", txid), zap.Error(err))
		return
	}

	p.logger.Debug("transaction queued for retry",
		zap.String("txid", txid),
		zap.Int("attempt", retryCount),
		zap.Time("next_retry_at", nextRetryAt),
	)
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
