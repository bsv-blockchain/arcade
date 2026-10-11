package api_server

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/telemetry"
)

// publishOutcome is the intake decision after a propagation produce attempt.
type publishOutcome int

const (
	// publishAcked means the broker accepted the message on this call.
	publishAcked publishOutcome = iota
	// publishAcceptedForRecovery means a durable RECEIVED row exists and
	// this process will republish it. The HTTP response is 202, same body
	// as a broker ack. A client retry is not required.
	publishAcceptedForRecovery
	// publishBackpressure is ErrBrokerBackpressure. The caller sheds with
	// 503 + Retry-After and does not enqueue recovery: the client retry is
	// the load-shed path, and shouldRepublishExisting republishes the row.
	publishBackpressure
	// publishFailed means the produce failed and Arcade has not taken
	// responsibility (no durable RECEIVED row, or recovery could not be
	// scheduled). The caller returns 500.
	publishFailed
)

// receivedRecoveryTiming bounds the prompt republish loop. The zero value
// is not used; defaultReceivedRecoveryTiming is the production budget.
//
// First attempt target: within stagger (25ms) plus one tick (50ms), so a
// durable RECEIVED row is retried in under 100ms. Later attempts double
// from initial up to maxDelay. workers caps how many produces run at once
// so a broker coming back does not get a thundering herd. maxAttempts at
// a 2s cap is about ten minutes; after that the row stays RECEIVED for the
// existing one-hour propagation reaper. That reaper is the last-resort
// backstop, not the primary produce-failure path.
//
// lookback, scanEvery, and scanBatch bound the restart scan. The in-memory
// queue dies with the process, so each start walks RECEIVED rows younger
// than lookback (the one-hour reaper threshold) and enqueues at most
// scanBatch new txids per pass. A row this process already published,
// shed for backpressure, or exhausted is not rediscovered until restart.
type receivedRecoveryTiming struct {
	workers      int
	maxAttempts  int
	initial      time.Duration
	maxDelay     time.Duration
	tick         time.Duration
	stagger      time.Duration
	publishedTTL time.Duration
	lookback     time.Duration
	scanEvery    time.Duration
	scanBatch    int
}

func defaultReceivedRecoveryTiming() receivedRecoveryTiming {
	return receivedRecoveryTiming{
		workers:      4,
		maxAttempts:  300,
		initial:      100 * time.Millisecond,
		maxDelay:     2 * time.Second,
		tick:         50 * time.Millisecond,
		stagger:      25 * time.Millisecond,
		publishedTTL: 2 * time.Minute,
		lookback:     time.Hour,
		scanEvery:    time.Second,
		scanBatch:    64,
	}
}

// recoveryItem is one durable RECEIVED row waiting for a broker ack.
// value is the propagation envelope (txid, raw_tx, input_txids). It is
// never logged.
type recoveryItem struct {
	txid     string
	topic    string
	key      string
	value    any
	attempts int
	next     time.Time
	inFlight bool
}

// receivedRecovery republishes propagation envelopes after the synchronous
// produce failed. One item per txid. A successful produce is remembered
// for publishedTTL so a client retry of the still-RECEIVED row does not
// publish a second copy. Before each attempt the store is re-read: a row
// that has left RECEIVED is dropped, so a late retry cannot broadcast
// again after the propagator has advanced it.
type receivedRecovery struct {
	producer *kafka.Producer
	store    store.Store
	logger   *zap.Logger
	timing   receivedRecoveryTiming

	wake chan struct{}

	mu        sync.Mutex
	items     map[string]*recoveryItem
	published map[string]time.Time
	// submitting is the txid set whose POST is between the durable insert
	// and the produce result. The restart scan skips these so it does not
	// publish a second copy of a request this process is still sending.
	submitting map[string]struct{}
	// shed is a backpressure txid. The client owns the retry; the scan
	// must not also enqueue it. Cleared on restart with the rest of this
	// map, so a 503 whose client never came back is still rediscovered.
	shed map[string]struct{}
	// exhausted is a txid that spent maxAttempts in this process. The
	// one-hour reaper is the backstop until restart, which starts a fresh
	// budget by virtue of this map being empty.
	exhausted map[string]struct{}

	wg sync.WaitGroup
}

func newReceivedRecovery(producer *kafka.Producer, st store.Store, logger *zap.Logger, timing receivedRecoveryTiming) *receivedRecovery {
	if logger == nil {
		logger = zap.NewNop()
	}
	if timing.workers <= 0 {
		timing = defaultReceivedRecoveryTiming()
	}
	if timing.lookback <= 0 {
		timing.lookback = time.Hour
	}
	if timing.scanEvery <= 0 {
		timing.scanEvery = time.Second
	}
	if timing.scanBatch <= 0 {
		timing.scanBatch = 64
	}
	return &receivedRecovery{
		producer:   producer,
		store:      st,
		logger:     logger,
		timing:     timing,
		wake:       make(chan struct{}, 1),
		items:      make(map[string]*recoveryItem),
		published:  make(map[string]time.Time),
		submitting: make(map[string]struct{}),
		shed:       make(map[string]struct{}),
		exhausted:  make(map[string]struct{}),
	}
}

func (r *receivedRecovery) run(ctx context.Context) {
	defer r.wg.Done()
	// First pass is immediate so a restart does not wait for scanEvery.
	r.rediscover(ctx)
	ticker := time.NewTicker(r.timing.tick)
	defer ticker.Stop()
	scan := time.NewTicker(r.timing.scanEvery)
	defer scan.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
			r.pump(ctx)
		case <-ticker.C:
			r.pump(ctx)
		case <-scan.C:
			r.rediscover(ctx)
		}
	}
}

func (r *receivedRecovery) wakeSoon() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *receivedRecovery) pump(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	due := r.claimDue(time.Now())
	for _, item := range due {
		r.wg.Add(1)
		go func(item *recoveryItem) {
			defer r.wg.Done()
			r.attempt(ctx, item)
		}(item)
	}
}

// claimDue marks up to the free worker slots of due items in flight.
// Oldest next-time wins, then txid, so a slow item cannot starve the rest
// and a full queue cannot start more produces than workers.
func (r *receivedRecovery) claimDue(now time.Time) []*recoveryItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	inflight := 0
	for _, item := range r.items {
		if item.inFlight {
			inflight++
		}
	}
	slots := r.timing.workers - inflight
	if slots <= 0 {
		return nil
	}
	due := make([]*recoveryItem, 0, slots)
	for _, item := range r.items {
		if item.inFlight || item.next.After(now) {
			continue
		}
		due = append(due, item)
	}
	slices.SortFunc(due, func(a, b *recoveryItem) int {
		if a.next.Before(b.next) {
			return -1
		}
		if a.next.After(b.next) {
			return 1
		}
		return strings.Compare(a.txid, b.txid)
	})
	if len(due) > slots {
		due = due[:slots]
	}
	for _, item := range due {
		item.inFlight = true
	}
	return due
}

func (r *receivedRecovery) attempt(ctx context.Context, item *recoveryItem) {
	if ctx.Err() != nil {
		r.releaseInFlight(item.txid)
		return
	}
	row, err := r.store.GetStatus(ctx, item.txid)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		r.failAttempt(item, err)
		return
	}
	if err != nil || row == nil || row.Status != models.StatusReceived {
		status := ""
		if row != nil {
			status = string(row.Status)
		}
		r.logger.Debug(
			"received publish recovery skipped",
			logfields.TxID(item.txid),
			logfields.Status(status),
		)
		r.drop(item.txid)
		return
	}

	metrics.ReceivedRecoveryAttemptsTotal.Inc()
	sendErr := r.producer.Send(ctx, item.topic, item.key, item.value)
	if sendErr == nil {
		r.completeSuccess(item.txid)
		return
	}
	r.failAttempt(item, sendErr)
}

func (r *receivedRecovery) completeSuccess(txid string) {
	r.mu.Lock()
	delete(r.items, txid)
	r.published[txid] = time.Now()
	metrics.ReceivedRecoverySuccessTotal.Inc()
	metrics.ReceivedRecoveryPendingDepth.Set(float64(len(r.items)))
	r.mu.Unlock()
	r.logger.Info("received publish recovery succeeded", logfields.TxID(txid))
}

func (r *receivedRecovery) drop(txid string) {
	r.mu.Lock()
	delete(r.items, txid)
	metrics.ReceivedRecoveryPendingDepth.Set(float64(len(r.items)))
	r.mu.Unlock()
}

func (r *receivedRecovery) releaseInFlight(txid string) {
	r.mu.Lock()
	if item, ok := r.items[txid]; ok {
		item.inFlight = false
	}
	r.mu.Unlock()
}

func (r *receivedRecovery) failAttempt(item *recoveryItem, cause error) {
	r.mu.Lock()
	current, ok := r.items[item.txid]
	if !ok {
		r.mu.Unlock()
		return
	}
	current.attempts++
	if current.attempts >= r.timing.maxAttempts {
		delete(r.items, item.txid)
		r.exhausted[item.txid] = struct{}{}
		metrics.ReceivedRecoveryExhaustedTotal.Inc()
		metrics.ReceivedRecoveryPendingDepth.Set(float64(len(r.items)))
		r.mu.Unlock()
		r.logger.Warn(
			"received publish recovery exhausted; one-hour reaper remains the backstop",
			logfields.TxID(item.txid),
			zap.Int("attempts", current.attempts),
			zap.Error(cause),
		)
		return
	}
	current.inFlight = false
	current.next = time.Now().Add(r.backoff(current.attempts))
	r.mu.Unlock()
	r.wakeSoon()
}

func (r *receivedRecovery) backoff(attempt int) time.Duration {
	d := r.timing.initial
	if d <= 0 {
		d = time.Millisecond
	}
	maxDelay := r.timing.maxDelay
	if maxDelay <= 0 {
		maxDelay = d
	}
	for i := 1; i < attempt; i++ {
		if d >= maxDelay/2 {
			d = maxDelay
			break
		}
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}
	return jitterDuration(d)
}

func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	span := int64(d) / 5
	if span == 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(2*span+1)-span)
}

// schedule queues txid if it is not already queued or recently published.
// A true result means the caller should acknowledge the submit (202):
// either recovery will republish, or a republish already succeeded.
func (r *receivedRecovery) schedule(txid, topic, key string, value any) bool {
	now := time.Now()
	r.mu.Lock()
	r.prunePublishedLocked(now)
	delete(r.shed, txid)
	delete(r.exhausted, txid)
	if _, ok := r.published[txid]; ok {
		r.mu.Unlock()
		return true
	}
	if _, ok := r.items[txid]; ok {
		r.mu.Unlock()
		return true
	}
	stagger := r.timing.stagger
	var wait time.Duration
	if stagger > 0 {
		wait = time.Duration(rand.Int64N(int64(stagger) + 1))
	}
	r.items[txid] = &recoveryItem{
		txid:  txid,
		topic: topic,
		key:   key,
		value: value,
		next:  now.Add(wait),
	}
	metrics.ReceivedPublishFailuresTotal.Inc()
	metrics.ReceivedRecoveryPendingDepth.Set(float64(len(r.items)))
	r.mu.Unlock()
	r.logger.Warn("scheduling received publish recovery", logfields.TxID(txid))
	r.wakeSoon()
	return true
}

func (r *receivedRecovery) owns(txid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[txid]; ok {
		return true
	}
	publishedAt, ok := r.published[txid]
	return ok && time.Since(publishedAt) < r.timing.publishedTTL
}

func (r *receivedRecovery) prunePublishedLocked(now time.Time) {
	ttl := r.timing.publishedTTL
	for txid, at := range r.published {
		if now.Sub(at) >= ttl {
			delete(r.published, txid)
		}
	}
}

// errStopReceivedScan stops IterateRecentReceived once this pass has
// enqueued scanBatch new txids. The next pass continues with the rows
// this process does not already own.
var errStopReceivedScan = errors.New("received rediscovery batch full")

func (r *receivedRecovery) rediscover(ctx context.Context) {
	if r.store == nil || ctx.Err() != nil {
		return
	}
	since := time.Now().Add(-r.timing.lookback)
	added := 0
	err := r.store.IterateRecentReceived(ctx, since, func(st *models.TransactionStatus) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if st == nil || st.Status != models.StatusReceived || st.TxID == "" || len(st.RawTx) == 0 {
			return nil
		}
		if r.skipRediscover(st.TxID) {
			return nil
		}
		env, envErr := receivedPublishEnvelope(st.TxID, st.RawTx)
		if envErr != nil {
			r.logger.Warn(
				"received publish rediscovery skipped undecodable row",
				logfields.TxID(st.TxID),
				zap.Error(envErr),
			)
			return nil
		}
		if !r.enqueueRediscovered(st.TxID, kafka.TopicPropagation, st.TxID, env) {
			return nil
		}
		added++
		if added >= r.timing.scanBatch {
			return errStopReceivedScan
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopReceivedScan) && !errors.Is(err, context.Canceled) {
		r.logger.Warn("received publish rediscovery scan failed", zap.Error(err))
	}
}

func receivedPublishEnvelope(txid string, raw []byte) (map[string]interface{}, error) {
	tx, _, err := sdkTx.NewTransactionFromStream(raw)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"txid":        txid,
		"raw_tx":      raw,
		"input_txids": collectInputTXIDs(tx),
	}, nil
}

func (r *receivedRecovery) skipRediscover(txid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.submitting[txid]; ok {
		return true
	}
	if _, ok := r.shed[txid]; ok {
		return true
	}
	if _, ok := r.exhausted[txid]; ok {
		return true
	}
	if _, ok := r.items[txid]; ok {
		return true
	}
	at, ok := r.published[txid]
	return ok && time.Since(at) < r.timing.publishedTTL
}

// enqueueRediscovered queues a store row. Unlike schedule, it does not
// count a publish failure: the row was found after a restart, not after
// a produce error in this process. False means the txid was not added.
func (r *receivedRecovery) enqueueRediscovered(txid, topic, key string, value any) bool {
	now := time.Now()
	r.mu.Lock()
	r.prunePublishedLocked(now)
	if _, ok := r.submitting[txid]; ok {
		r.mu.Unlock()
		return false
	}
	if _, ok := r.shed[txid]; ok {
		r.mu.Unlock()
		return false
	}
	if _, ok := r.exhausted[txid]; ok {
		r.mu.Unlock()
		return false
	}
	if _, ok := r.published[txid]; ok {
		r.mu.Unlock()
		return false
	}
	if _, ok := r.items[txid]; ok {
		r.mu.Unlock()
		return false
	}
	stagger := r.timing.stagger
	var wait time.Duration
	if stagger > 0 {
		wait = time.Duration(rand.Int64N(int64(stagger) + 1))
	}
	r.items[txid] = &recoveryItem{
		txid:  txid,
		topic: topic,
		key:   key,
		value: value,
		next:  now.Add(wait),
	}
	metrics.ReceivedRecoveryPendingDepth.Set(float64(len(r.items)))
	r.mu.Unlock()
	r.logger.Info("rediscovered received row for publish", logfields.TxID(txid))
	r.wakeSoon()
	return true
}

func (r *receivedRecovery) hold(txid string) {
	r.mu.Lock()
	r.submitting[txid] = struct{}{}
	r.mu.Unlock()
}

func (r *receivedRecovery) release(txid string) {
	r.mu.Lock()
	delete(r.submitting, txid)
	r.mu.Unlock()
}

func (r *receivedRecovery) notePublished(txid string) {
	r.mu.Lock()
	r.published[txid] = time.Now()
	delete(r.submitting, txid)
	delete(r.shed, txid)
	r.mu.Unlock()
}

func (r *receivedRecovery) markShed(txid string) {
	r.mu.Lock()
	r.shed[txid] = struct{}{}
	r.mu.Unlock()
}

func (s *Server) ensureReceivedRecovery() *receivedRecovery {
	if s.producer == nil || s.store == nil {
		return nil
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.recoveryStopped {
		return nil
	}
	if s.recovery != nil {
		return s.recovery
	}
	timing := s.recoveryTiming
	if timing.workers <= 0 {
		timing = defaultReceivedRecoveryTiming()
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := newReceivedRecovery(s.producer, s.store, s.logger, timing)
	s.recovery = r
	s.recoveryCancel = cancel
	r.wg.Add(1)
	go r.run(ctx)
	return r
}

func (s *Server) stopReceivedRecovery() {
	s.recoveryMu.Lock()
	s.recoveryStopped = true
	cancel := s.recoveryCancel
	r := s.recovery
	s.recoveryMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if r != nil {
		r.wg.Wait()
	}
}

func (s *Server) receivedPublishRecoveryOwns(txid string) bool {
	s.recoveryMu.Lock()
	r := s.recovery
	s.recoveryMu.Unlock()
	if r == nil {
		return false
	}
	return r.owns(txid)
}

func (s *Server) holdReceivedSubmit(txid string) {
	r := s.ensureReceivedRecovery()
	if r == nil || txid == "" {
		return
	}
	r.hold(txid)
}

func (s *Server) releaseReceivedSubmit(txid string) {
	s.recoveryMu.Lock()
	r := s.recovery
	s.recoveryMu.Unlock()
	if r == nil || txid == "" {
		return
	}
	r.release(txid)
}

func (s *Server) noteReceivedPublished(txid string) {
	s.recoveryMu.Lock()
	r := s.recovery
	s.recoveryMu.Unlock()
	if r == nil || txid == "" {
		return
	}
	r.notePublished(txid)
}

func (s *Server) shedReceivedSubmit(txid string) {
	s.recoveryMu.Lock()
	r := s.recovery
	s.recoveryMu.Unlock()
	if r == nil || txid == "" {
		return
	}
	r.markShed(txid)
}

func (s *Server) scheduleReceivedRecovery(txid, topic, key string, value any) bool {
	if txid == "" || value == nil || s.producer == nil || s.store == nil {
		return false
	}
	r := s.ensureReceivedRecovery()
	if r == nil {
		return false
	}
	return r.schedule(txid, topic, key, value)
}

// publishSingleOrRecover produces one POST /tx envelope. See publishOutcome
// for the HTTP mapping. The envelope is not logged.
func (s *Server) publishSingleOrRecover(ctx context.Context, txid string, msg map[string]interface{}, persistedReceived bool) publishOutcome {
	if persistedReceived && s.receivedPublishRecoveryOwns(txid) {
		return publishAcceptedForRecovery
	}
	err := s.producer.Send(ctx, kafka.TopicPropagation, txid, msg)
	if err == nil {
		s.noteReceivedPublished(txid)
		return publishAcked
	}
	if errors.Is(err, kafka.ErrBrokerBackpressure) {
		s.shedReceivedSubmit(txid)
		s.logger.Warn("submit rejected: kafka backpressure", logfields.TxID(txid))
		return publishBackpressure
	}
	s.logger.Error("failed to publish transaction", logfields.TxID(txid), zap.Error(err))
	if !persistedReceived {
		return publishFailed
	}
	if !s.scheduleReceivedRecovery(txid, kafka.TopicPropagation, txid, msg) {
		return publishFailed
	}
	s.logger.Warn(
		"transaction accepted for received-publish recovery",
		logfields.TxID(txid),
		logfields.Status(string(models.StatusReceived)),
	)
	return publishAcceptedForRecovery
}

// publishBatchOrRecover is the POST /txs form of publishSingleOrRecover.
// Owned txids are left to the recovery loop. A failed batch is recovered
// per tx with the same family key the batch used, and only when every
// member of this produce was a durable RECEIVED row does the caller
// acknowledge 202. Otherwise the caller returns 500 so the client retries
// the members that were not durable. Backpressure is not recovered here.
func (s *Server) publishBatchOrRecover(ctx context.Context, toPublish []parsedItem, keyByTxid map[string]string, inputsByTxid map[string][]string) publishOutcome {
	need := make([]parsedItem, 0, len(toPublish))
	for _, p := range toPublish {
		if p.durableReceived && s.receivedPublishRecoveryOwns(p.txid) {
			continue
		}
		need = append(need, p)
	}
	if len(need) == 0 {
		return publishAcceptedForRecovery
	}
	msgs := propagationMessages(need, keyByTxid, inputsByTxid)
	err := s.producer.SendBatch(ctx, kafka.TopicPropagation, msgs)
	if err == nil {
		ids := make([]string, len(need))
		for i, p := range need {
			ids[i] = p.txid
		}
		// Same line the synchronous ack used to emit from the handler.
		// TxIDBatch is capped; raw transaction bytes are not logged.
		fields := append([]zap.Field{logfields.Status(string(models.StatusReceived))}, logfields.TxIDBatch(ids)...)
		telemetry.LoggerWith(ctx, s.logger).Info("transactions received", fields...)
		for _, id := range ids {
			s.noteReceivedPublished(id)
		}
		return publishAcked
	}
	if errors.Is(err, kafka.ErrBrokerBackpressure) {
		for _, p := range need {
			if p.durableReceived {
				s.shedReceivedSubmit(p.txid)
			}
		}
		s.logger.Warn("batch submit rejected: kafka backpressure", zap.Int("count", len(msgs)))
		return publishBackpressure
	}
	s.logger.Error("failed to publish transaction batch", zap.Error(err), zap.Int("count", len(need)))
	allDurable := true
	recovering := false
	recoveredIDs := make([]string, 0, len(need))
	for i, p := range need {
		if !p.durableReceived {
			allDurable = false
			continue
		}
		if !s.scheduleReceivedRecovery(p.txid, kafka.TopicPropagation, msgs[i].Key, msgs[i].Value) {
			allDurable = false
			continue
		}
		recovering = true
		recoveredIDs = append(recoveredIDs, p.txid)
	}
	if !recovering || !allDurable {
		return publishFailed
	}
	fields := append([]zap.Field{logfields.Status(string(models.StatusReceived))}, logfields.TxIDBatch(recoveredIDs)...)
	s.logger.Warn("transactions accepted for received-publish recovery", fields...)
	return publishAcceptedForRecovery
}
