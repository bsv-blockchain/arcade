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
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
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
	store          store.Store
	leaser         store.Leaser
	teranodeClient *teranode.Client
	merkleClient   *merkleservice.Client
	consumer       *kafka.ConsumerGroup

	mu                sync.Mutex
	pendingMsgs       []propagationMsg
	merkleConcurrency int
	retryMaxAttempts  int
	retryBackoffMs    int
	reaperInterval    time.Duration
	reaperBatchSize   int
	teranodeBatchCap  int
	holderID          string
	leaseTTL          time.Duration
}

// New constructs a Propagator. leaser may be nil, in which case the reaper
// runs unguarded — appropriate for tests and single-process deployments that
// don't need coordination. In production every replica should receive a
// non-nil Leaser so only one reaper is active at a time across the cluster.
func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, st store.Store, leaser store.Leaser, tc *teranode.Client, mc *merkleservice.Client) *Propagator {
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
	return &Propagator{
		cfg:               cfg,
		logger:            logger.Named("propagation"),
		producer:          producer,
		store:             st,
		leaser:            leaser,
		teranodeClient:    tc,
		merkleClient:      mc,
		merkleConcurrency: merkleConcurrency,
		retryMaxAttempts:  retryMax,
		retryBackoffMs:    retryBackoff,
		reaperInterval:    reaperInterval,
		reaperBatchSize:   reaperBatch,
		teranodeBatchCap:  teranodeBatchCap,
		holderID:          newHolderID(),
		leaseTTL:          leaseTTL,
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

	// Kick off the durable-retry reaper alongside the Kafka consumer. It owns
	// all rebroadcast work for PENDING_RETRY rows, decoupled from the incoming
	// message flush cycle so a retry storm can't starve live traffic.
	go p.runReaper(ctx)

	p.logger.Info("propagation service started",
		zap.Duration("reaper_interval", p.reaperInterval),
		zap.Int("reaper_batch_size", p.reaperBatchSize),
	)
	return consumer.Run(ctx)
}

func (p *Propagator) Stop() error {
	p.logger.Info("stopping propagation service")
	if p.consumer != nil {
		return p.consumer.Close()
	}
	return nil
}

// handleMessage accumulates a single propagation message into the pending batch.
// The consumer's drain-then-flush pattern calls flushBatch after all immediately
// available messages have been processed, so the batch size naturally matches
// what the client submitted — no configured threshold needed.
func (p *Propagator) handleMessage(ctx context.Context, msg *kafka.Message) error {
	var propMsg propagationMsg
	if err := json.Unmarshal(msg.Value, &propMsg); err != nil {
		return fmt.Errorf("unmarshaling propagation message: %w", err)
	}

	if len(propMsg.RawTx) == 0 {
		return fmt.Errorf("propagation message has empty raw_tx")
	}

	p.mu.Lock()
	p.pendingMsgs = append(p.pendingMsgs, propMsg)
	p.mu.Unlock()

	return nil
}

// flushBatch processes all accumulated messages as a batch. Retry work
// belongs to the reaper goroutine — it is no longer coupled to the consumer's
// drain-flush cycle, so live ingest doesn't have to wait on rebroadcasts.
//
// The context comes from the current Kafka claim — it is cancelled when the
// claim ends (shutdown or rebalance). Downstream HTTP broadcasts and store
// writes observe that cancellation and unwind cleanly, so a revoked partition
// doesn't keep doing work on behalf of a partition it no longer owns.
func (p *Propagator) flushBatch(ctx context.Context) error {
	p.mu.Lock()
	batch := p.pendingMsgs
	p.pendingMsgs = nil
	p.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}
	return p.processBatch(ctx, batch)
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
}

// processBatch handles a batch of propagation messages:
// 1. Register all txids with merkle service concurrently
// 2. Broadcast all raw txs to teranode endpoints, chunked to teranodeBatchCap
// 3. Update status for each transaction
func (p *Propagator) processBatch(ctx context.Context, batch []propagationMsg) error {
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

	// Step 1: Register all txids with merkle service (mandatory)
	if p.merkleClient != nil && p.cfg.CallbackURL != "" {
		regs := make([]merkleservice.Registration, len(batch))
		for i, msg := range batch {
			regs[i] = merkleservice.Registration{
				TxID:        msg.TXID,
				CallbackURL: p.cfg.CallbackURL,
			}
		}
		mStart := time.Now()
		if err := p.merkleClient.RegisterBatch(ctx, regs, p.merkleConcurrency); err != nil {
			metrics.PropagationMerkleRegisterDuration.Observe(time.Since(mStart).Seconds())
			return fmt.Errorf("merkle-service batch registration failed: %w", err)
		}
		metrics.PropagationMerkleRegisterDuration.Observe(time.Since(mStart).Seconds())
		p.logger.Debug("batch registered with merkle-service", zap.Int("count", len(batch)))
	}

	// Step 2: Broadcast in chunks bounded by teranodeBatchCap so a single
	// oversized Kafka flush doesn't blow past Teranode's /txs size limit.
	rawTxs := make([][]byte, len(batch))
	for i, msg := range batch {
		rawTxs[i] = msg.RawTx
	}
	results := p.broadcastInChunks(ctx, batch, rawTxs)

	// Step 3: Update status for each transaction, with retry classification
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
		}
		if err := p.store.UpdateStatus(ctx, res.status); err != nil {
			p.logger.Error("failed to update status",
				zap.String("txid", batch[i].TXID),
				zap.Error(err),
			)
		}
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
// bounded by maxParallelChunks so a large flush doesn't serialise behind one
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
		c := c
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
		out[0] = txResult{status: br.Status, errMsg: br.ErrorMsg, rawTx: chunk[0].RawTx, successEndpoint: br.SuccessEndpoint}
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

	// Fallback: per-tx classification for this chunk only. Summarise instead
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
		i, msg := i, msg
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			br := p.broadcastSingleToEndpoints(ctx, rawTxs[i], msg.TXID)
			out[i] = txResult{status: br.Status, errMsg: br.ErrorMsg, rawTx: msg.RawTx, successEndpoint: br.SuccessEndpoint}
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
// the shared broadcast context is cancelled so slower sibling requests don't
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

	type endpointResult struct {
		endpoint   string
		statusCode int
		err        error
	}

	submitCtx, cancelSubmit := context.WithTimeout(ctx, 15*time.Second)
	defer cancelSubmit()
	broadcastCtx, cancelBroadcast := context.WithCancel(submitCtx)
	defer cancelBroadcast()

	resultCh := make(chan endpointResult, len(endpoints))
	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			statusCode, err := p.teranodeClient.SubmitTransaction(broadcastCtx, ep, rawTx)
			resultCh <- endpointResult{endpoint: ep, statusCode: statusCode, err: err}
		}(endpoint)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var bestStatus models.Status
	var lastErrMsg string
	var successEndpoint string
	for result := range resultCh {
		// A sibling request cancelled by a winning race is not a real failure —
		// the peer didn't misbehave, we called off the race. Skip health
		// recording and status aggregation for that case.
		if isCancelledByBroadcast(result.err, broadcastCtx) {
			continue
		}
		// Health recording is about *reachability*, not about whether the peer
		// accepted this specific tx. statusCode == 0 means the HTTP client never
		// received a response (transport error / timeout / DNS failure) — the
		// peer is unreachable and that counts against the circuit-breaker. Any
		// non-zero status code means the peer responded, so it is alive; a
		// 4xx/5xx body is a legitimate "your tx was rejected" answer, not a
		// reason to sideline the peer.
		recordEndpointOutcome(p.teranodeClient, result.endpoint, result.statusCode)
		if result.err != nil {
			lastErrMsg = result.err.Error()
			if statusPriority(models.StatusRejected) > statusPriority(bestStatus) {
				bestStatus = models.StatusRejected
			}
			continue
		}
		switch result.statusCode {
		case http.StatusOK:
			if statusPriority(models.StatusAcceptedByNetwork) > statusPriority(bestStatus) {
				bestStatus = models.StatusAcceptedByNetwork
				successEndpoint = result.endpoint
			}
			// Early-cancel: the first 200 is the verdict. Sibling requests
			// observe broadcastCtx.Err() and return quickly.
			cancelBroadcast()
		case http.StatusAccepted:
			// 202 means the peer accepted the tx but won't tell us yet —
			// matching original behavior, no tx-level status update.
			cancelBroadcast()
		}
	}

	if bestStatus == "" {
		return broadcastResult{}
	}

	return broadcastResult{
		Status: &models.TransactionStatus{
			TxID:      txid,
			Status:    bestStatus,
			Timestamp: time.Now(),
		},
		ErrorMsg:        lastErrMsg,
		SuccessEndpoint: successEndpoint,
	}
}

// isCancelledByBroadcast reports whether err is a context.Canceled directly
// caused by the broadcast's own cancel signal (i.e. the winning race). A
// context.Canceled that wasn't triggered by the broadcast cancel still counts
// as a real failure — e.g. the outer submitCtx timing out.
func isCancelledByBroadcast(err error, broadcastCtx context.Context) bool {
	if err == nil {
		return false
	}
	if broadcastCtx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled)
}

// recordEndpointOutcome routes a broadcast result into the teranode client's
// circuit-breaker based on whether the peer responded at all. statusCode == 0
// is the int zero value returned by Submit{Transaction,Transactions} when no
// HTTP response was received; any non-zero status means the peer is alive,
// regardless of whether it accepted this particular payload.
func recordEndpointOutcome(tc *teranode.Client, endpoint string, statusCode int) {
	if statusCode == 0 {
		tc.RecordFailure(endpoint)
		return
	}
	tc.RecordSuccess(endpoint)
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

	type endpointResult struct {
		endpoint   string
		statusCode int
		err        error
	}

	submitCtx, cancelSubmit := context.WithTimeout(ctx, 15*time.Second)
	defer cancelSubmit()
	broadcastCtx, cancelBroadcast := context.WithCancel(submitCtx)
	defer cancelBroadcast()

	resultCh := make(chan endpointResult, len(endpoints))
	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			statusCode, err := p.teranodeClient.SubmitTransactions(broadcastCtx, ep, rawTxs)
			resultCh <- endpointResult{endpoint: ep, statusCode: statusCode, err: err}
		}(endpoint)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	anySuccess := false
	for result := range resultCh {
		if isCancelledByBroadcast(result.err, broadcastCtx) {
			continue
		}
		// Reachability-based health: any HTTP response (even 5xx) means the
		// peer is alive. Only a missing response (statusCode == 0) counts
		// against the circuit-breaker.
		recordEndpointOutcome(p.teranodeClient, result.endpoint, result.statusCode)
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
