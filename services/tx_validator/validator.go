// Package tx_validator consumes the transaction Kafka topic, validates each
// submitted tx, deduplicates against the store, and forwards accepted txs to
// the propagation topic.
//
// The processing model is "buffer → drain → parallel-flush":
//
//   - handleMessage is cheap. It JSON-decodes the message and pushes the raw
//     bytes onto a per-validator pendingValidations slice under v.mu. No
//     parsing, no DB I/O, no validation happens here. This lets the Kafka
//     consumer drain its claim channel as fast as the broker can hand off
//     messages.
//
//   - flushValidations runs at the end of each drain window (the Kafka
//     consumer's drain-then-flush hook). It processes the buffered batch in
//     five phases, with parallelism bounded by tx_validator.parallelism:
//
//     1. Parse — concurrent BEEF/raw decode + txid computation. Parse failures
//        become per-tx rejects, not batch failures.
//     2. Dedup — concurrent GetOrInsertStatus per tx. Existing rows skip
//        validation entirely; new rows continue.
//     3. Validate — concurrent ValidateTransaction. Stateless on the validator
//        struct, safe for fan-out.
//     4. Persist rejects — UpdateStatus for each rejection, also concurrent.
//        Reject reasons are terminal; no Kafka retry is wanted.
//     5. Publish accepted — single SendBatch to the propagation topic.
//
// Per-message order within a flush window is intentionally NOT preserved.
// Independent transactions don't have inter-tx dependencies; if you need
// ordering for a specific stream, key those messages so they land on a single
// partition with parallelism=1.
//
// The pattern works in standalone mode (memory broker delivers all messages
// to a single consumer; parallelism happens inside the flush) and in
// horizontally scaled microservice mode (each pod's per-partition consumer
// runs the same pipeline; total throughput = partitions × parallelism).
package tx_validator

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/validator"
)

// pendingTx is one queued ingress message awaiting flush-time processing.
// Holding only the raw bytes plus the original Kafka offset (for log
// correlation) keeps handleMessage allocation-light.
type pendingTx struct {
	rawTx []byte
}

type Validator struct {
	cfg         *config.Config
	logger      *zap.Logger
	producer    *kafka.Producer
	store       store.Store
	txTracker   *store.TxTracker
	txValidator *validator.Validator
	consumer    *kafka.ConsumerGroup

	parallelism int

	mu sync.Mutex
	// pending holds raw tx bytes that haven't yet been through the validation
	// pipeline. Appended by handleMessage, drained at flush time.
	pending []pendingTx
	// publishCarry holds propagation-topic messages whose validation completed
	// but whose Kafka publish failed. The next flush prepends these to its
	// publish payload so we don't lose work across a transient publish error.
	publishCarry []kafka.KeyValue
}

func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, st store.Store, tracker *store.TxTracker, v *validator.Validator) *Validator {
	parallelism := cfg.TxValidator.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}
	return &Validator{
		cfg:         cfg,
		logger:      logger.Named("tx-validator"),
		producer:    producer,
		store:       st,
		txTracker:   tracker,
		txValidator: v,
		parallelism: parallelism,
	}
}

func (v *Validator) Name() string { return "tx-validator" }

func (v *Validator) Start(ctx context.Context) error {
	consumer, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:     v.producer.Broker(),
		GroupID:    v.cfg.Kafka.ConsumerGroup + "-tx-validator",
		Topics:     []string{kafka.TopicTransaction},
		Handler:    v.handleMessage,
		FlushFunc:  v.flushValidations,
		Producer:   v.producer,
		MaxRetries: v.cfg.Kafka.MaxRetries,
		Logger:     v.logger,
	})
	if err != nil {
		return fmt.Errorf("creating consumer group: %w", err)
	}
	v.consumer = consumer

	v.logger.Info("tx validator started", zap.Int("parallelism", v.parallelism))
	return consumer.Run(ctx)
}

func (v *Validator) Stop() error {
	v.logger.Info("stopping tx validator")
	if v.consumer != nil {
		return v.consumer.Close()
	}
	return nil
}

type txMessage struct {
	Action string `json:"action,omitempty"`
	// RawTx carries the serialized transaction as raw bytes. encoding/json
	// encodes []byte as base64 on the wire, so we avoid the hex round-trip
	// that used to happen at every pipeline hop.
	RawTx []byte `json:"raw_tx,omitempty"`
}

// handleMessage is the per-message hook. Its only job is to decode the
// envelope and queue the raw bytes for batch processing in flushValidations.
// Keep it cheap so the consumer drain loop runs as fast as the broker hands
// off messages.
func (v *Validator) handleMessage(_ context.Context, msg *kafka.Message) error {
	var txMsg txMessage
	if err := json.Unmarshal(msg.Value, &txMsg); err != nil {
		return fmt.Errorf("unmarshaling message: %w", err)
	}

	if txMsg.Action != "submit" || len(txMsg.RawTx) == 0 {
		// State updates and other action types are handled elsewhere.
		return nil
	}

	v.mu.Lock()
	v.pending = append(v.pending, pendingTx{rawTx: txMsg.RawTx})
	depth := len(v.pending)
	v.mu.Unlock()
	metrics.TxValidatorPendingDepth.Set(float64(depth))
	return nil
}

// parseResult carries the outcome of phase 1 for a single buffered tx.
// On parse failure, parsed is nil and err is non-nil; the entry is dropped
// from later phases (no DB write — the tx is malformed and we have no txid
// to write under).
type parseResult struct {
	rawTx  []byte
	parsed *sdkTx.Transaction
	txid   string
	err    error
}

// validatedTx is the per-tx state passed between phases 2-5 for tx that
// successfully parsed. status carries the dedup outcome from phase 2.
type validatedTx struct {
	rawTx        []byte
	parsed       *sdkTx.Transaction
	txid         string
	existing     *models.TransactionStatus // populated when !inserted
	inserted     bool
	rejected     bool   // set by phase 3
	rejectReason string // set by phase 3
}

// flushValidations runs the five-phase parallel pipeline over the buffered
// batch. The context comes from the current Kafka claim — a rebalance or
// shutdown cancels it so in-flight DB calls and the eventual Kafka publish
// abort cleanly rather than running on behalf of a partition this consumer
// no longer owns.
//
// On flush failure (Kafka publish error) the accepted tx slice is requeued so
// the next drain retries — same recovery semantics the previous serial
// implementation had.
func (v *Validator) flushValidations(ctx context.Context) error {
	v.mu.Lock()
	batch := v.pending
	v.pending = nil
	hasCarry := len(v.publishCarry) > 0
	v.mu.Unlock()
	metrics.TxValidatorPendingDepth.Set(0)

	// Nothing to do means: no fresh messages AND no carry from a prior failed
	// publish. Returning early here skips the publish phase, so we need to
	// stay in the function whenever there's a carry to retry.
	if len(batch) == 0 && !hasCarry {
		return nil
	}

	start := time.Now()
	metrics.TxValidatorFlushSize.Observe(float64(len(batch)))

	// Phase 1: parse + txid (parallel).
	parsed := v.phaseParse(ctx, batch)

	// Phase 2: dedup via per-tx GetOrInsertStatus (parallel).
	live, dups := v.phaseDedup(ctx, parsed)

	// Phase 3: validate (parallel). Mutates entries in `live` in-place.
	v.phaseValidate(ctx, live)

	// Phase 4: persist rejects (parallel).
	rejects := collectRejects(live)
	v.phasePersistRejects(ctx, rejects)

	// Build this flush's publish payload from the freshly accepted txs, then
	// merge with any prior carry-over from a previous flush whose publish
	// failed. Carry comes first so older messages aren't starved by a
	// continuous stream of new ones.
	accepted := collectAccepted(live)
	freshMsgs := make([]kafka.KeyValue, 0, len(accepted))
	for _, vt := range accepted {
		freshMsgs = append(freshMsgs, kafka.KeyValue{
			Key: vt.txid,
			Value: map[string]interface{}{
				"txid":   vt.txid,
				"raw_tx": vt.rawTx,
			},
		})
	}
	v.mu.Lock()
	publishMsgs := append(v.publishCarry, freshMsgs...)
	v.publishCarry = nil
	v.mu.Unlock()

	// Phase 5: single Kafka publish for the accepted set + any carry.
	if len(publishMsgs) > 0 {
		if err := v.producer.SendBatch(kafka.TopicPropagation, publishMsgs); err != nil {
			// Carry these messages to the next flush. Validation, dedup, and
			// reject persistence are already done in the store; we only need
			// the Kafka publish to succeed eventually.
			v.mu.Lock()
			v.publishCarry = append(publishMsgs, v.publishCarry...)
			carryDepth := len(v.publishCarry)
			v.mu.Unlock()
			metrics.TxValidatorPublishCarryDepth.Set(float64(carryDepth))
			metrics.TxValidatorFlushDuration.WithLabelValues("publish_failed").Observe(time.Since(start).Seconds())
			return fmt.Errorf("batch publishing to propagation: %w", err)
		}
		metrics.TxValidatorPublishCarryDepth.Set(0)
	}

	parseFails := 0
	for _, p := range parsed {
		if p.err != nil {
			parseFails++
		}
	}
	metrics.TxValidatorOutcomeTotal.WithLabelValues("accepted").Add(float64(len(accepted)))
	metrics.TxValidatorOutcomeTotal.WithLabelValues("rejected").Add(float64(len(rejects)))
	metrics.TxValidatorOutcomeTotal.WithLabelValues("duplicate").Add(float64(len(dups)))
	metrics.TxValidatorOutcomeTotal.WithLabelValues("parse_fail").Add(float64(parseFails))
	metrics.TxValidatorFlushDuration.WithLabelValues("success").Observe(time.Since(start).Seconds())
	v.logger.Info("validation flush complete",
		zap.Int("count", len(batch)),
		zap.Int("parsed", len(parsed)-parseFails),
		zap.Int("parse_fails", parseFails),
		zap.Int("duplicates", len(dups)),
		zap.Int("validated", len(accepted)),
		zap.Int("rejected", len(rejects)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// phaseParse runs BEEF/raw parsing concurrently. Output preserves input order
// so log lines and any future per-position error reporting line up. Parse
// failures are kept in the result with err != nil so phase metrics can count
// them.
func (v *Validator) phaseParse(ctx context.Context, batch []pendingTx) []parseResult {
	out := make([]parseResult, len(batch))
	sem := make(chan struct{}, v.parallelism)
	var wg sync.WaitGroup
	for i, p := range batch {
		i, p := i, p
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			out[i] = parseResult{rawTx: p.rawTx, err: ctx.Err()}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			out[i] = parseOne(p.rawTx)
		}()
	}
	wg.Wait()
	return out
}

// parseOne tries BEEF first, falls back to raw. Mirrors the previous serial
// code's behaviour exactly.
//
// Wrapped in a panic recover because go-sdk's BEEF parser is known to panic
// on truncated payloads (slice bounds out of range). A misshapen client body
// must not take the validator down — surface as a per-tx parse failure.
func parseOne(rawTx []byte) (res parseResult) {
	res.rawTx = rawTx
	defer func() {
		if r := recover(); r != nil {
			res.parsed = nil
			res.txid = ""
			res.err = fmt.Errorf("panic parsing transaction: %v", r)
		}
	}()
	beef, beefErr := sdkTx.NewBeefFromBytes(rawTx)
	if beefErr == nil && beef != nil {
		if tx := beef.FindTransactionForSigning(""); tx != nil {
			res.parsed = tx
			res.txid = tx.TxID().String()
			return res
		}
	}
	tx, err := sdkTx.NewTransactionFromBytes(rawTx)
	if err != nil {
		res.err = fmt.Errorf("failed to parse transaction: %w", err)
		return res
	}
	res.parsed = tx
	res.txid = tx.TxID().String()
	return res
}

// phaseDedup uses the store's batch CAS to insert (or read existing rows
// for) the parsed tx set in one round-trip on backends that support it
// (Postgres) and a bounded-concurrency loop on the others. Returns the
// slice of "new" txs that need validation and the duplicates that
// short-circuit the pipeline via the tx tracker.
//
// Errors here are logged and the offending batch is dropped — a transient
// store failure means we don't propagate but don't write a reject either,
// so clients can retry.
func (v *Validator) phaseDedup(ctx context.Context, parsed []parseResult) (live []*validatedTx, duplicates []*validatedTx) {
	live = make([]*validatedTx, 0, len(parsed))
	duplicates = make([]*validatedTx, 0, len(parsed))

	// Build the input slice for the batch CAS, keeping a parallel slice of
	// per-position validatedTx records so we can stitch the results back into
	// input order after the call returns. Parse failures don't go to the
	// store (no txid).
	insertInputs := make([]*models.TransactionStatus, 0, len(parsed))
	insertSlots := make([]int, 0, len(parsed))
	vts := make([]*validatedTx, len(parsed))
	now := time.Now()
	for i, p := range parsed {
		if p.err != nil || p.parsed == nil {
			v.logger.Debug("dropping unparseable tx", zap.Error(p.err))
			continue
		}
		insertInputs = append(insertInputs, &models.TransactionStatus{
			TxID:      p.txid,
			Timestamp: now,
		})
		insertSlots = append(insertSlots, i)
		vts[i] = &validatedTx{
			rawTx:  p.rawTx,
			parsed: p.parsed,
			txid:   p.txid,
		}
	}

	if len(insertInputs) == 0 {
		return live, duplicates
	}

	results, err := v.store.BatchGetOrInsertStatus(ctx, insertInputs)
	if err != nil {
		v.logger.Error("dedup store error", zap.Int("count", len(insertInputs)), zap.Error(err))
		// Per-row failures are reflected as zero-value entries in `results` —
		// process whatever came back so a single bad row doesn't kill the
		// whole batch on parallel-loop backends. True batched-SQL backends
		// fail atomically: results is nil/empty, so nothing graduates.
	}
	storeErrors := 0
	for k, slotIdx := range insertSlots {
		vt := vts[slotIdx]
		var res store.BatchInsertResult
		if k < len(results) {
			res = results[k]
		}
		// A zero-value result means: not inserted AND no existing row, i.e.
		// the per-row store call failed. Drop these and count as store_error.
		if !res.Inserted && res.Existing == nil {
			storeErrors++
			vts[slotIdx] = nil
			continue
		}
		vt.inserted = res.Inserted
		vt.existing = res.Existing
		if !res.Inserted {
			v.txTracker.Add(vt.txid, res.Existing.Status)
		}
	}
	if storeErrors > 0 {
		metrics.TxValidatorOutcomeTotal.WithLabelValues("store_error").Add(float64(storeErrors))
	}

	for _, vt := range vts {
		if vt == nil {
			continue
		}
		if vt.inserted {
			live = append(live, vt)
		} else {
			duplicates = append(duplicates, vt)
		}
	}
	return live, duplicates
}

// phaseValidate runs ValidateTransaction concurrently for each new tx.
// validator.ValidateTransaction reads only immutable struct fields so
// fan-out is safe. Mutates entries in-place: rejected=true and rejectReason
// for failures, untouched for accepts.
func (v *Validator) phaseValidate(ctx context.Context, live []*validatedTx) {
	if v.txValidator == nil {
		// Tests sometimes pass a nil validator; treat all as accepts.
		return
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(v.parallelism)
	for _, vt := range live {
		vt := vt
		g.Go(func() error {
			if err := v.txValidator.ValidateTransaction(gctx, vt.parsed, true, true); err != nil {
				vt.rejected = true
				vt.rejectReason = err.Error()
				v.logger.Info("transaction validation failed",
					zap.String("txid", vt.txid),
					zap.Error(err),
				)
				return nil
			}
			v.txTracker.Add(vt.txid, models.StatusReceived)
			return nil
		})
	}
	_ = g.Wait()
}

// phasePersistRejects writes REJECTED status + reject reason for every tx
// that failed validation via the store's batch update — single round-trip
// on Postgres, parallel-loop fallback on Aerospike/Pebble. The txid was
// already inserted in phase 2 so the rows exist.
func (v *Validator) phasePersistRejects(ctx context.Context, rejects []*validatedTx) {
	if len(rejects) == 0 {
		return
	}
	now := time.Now()
	updates := make([]*models.TransactionStatus, len(rejects))
	for i, vt := range rejects {
		updates[i] = &models.TransactionStatus{
			TxID:      vt.txid,
			Status:    models.StatusRejected,
			ExtraInfo: vt.rejectReason,
			Timestamp: now,
		}
	}
	if err := v.store.BatchUpdateStatus(ctx, updates); err != nil {
		v.logger.Error("failed to persist rejected statuses",
			zap.Int("count", len(rejects)),
			zap.Error(err),
		)
	}
}

func collectAccepted(live []*validatedTx) []*validatedTx {
	out := make([]*validatedTx, 0, len(live))
	for _, vt := range live {
		if !vt.rejected {
			out = append(out, vt)
		}
	}
	return out
}

func collectRejects(live []*validatedTx) []*validatedTx {
	out := make([]*validatedTx, 0)
	for _, vt := range live {
		if vt.rejected {
			out = append(out, vt)
		}
	}
	return out
}

