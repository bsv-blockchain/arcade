package propagation

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/models"
)

// propagationMsg is the propagation-topic payload. JSON-encoded onto
// kafka.TopicDispatch by intake and decoded by dispatcherConsumer.
//
// InputTXIDs and KafkaOffset support the dep-aware pipeline: the
// dispatcher uses InputTXIDs to decide eligibility without re-parsing
// the raw tx, and KafkaOffset is populated by the consumer (not via
// JSON) so offset commit can be deferred until the tx terminalizes.
type propagationMsg struct {
	TXID string `json:"txid"`
	// RawTx is the serialized transaction as raw bytes. encoding/json
	// encodes []byte as base64.
	RawTx []byte `json:"raw_tx"`
	// InputTXIDs is populated by intake so the dispatcher can decide
	// eligibility without re-parsing. Empty/absent is treated as "no
	// in-flight parents".
	InputTXIDs []string `json:"input_txids,omitempty"`
	// KafkaOffset is set by the consumer before handing the message to
	// the dispatcher. Not serialized.
	KafkaOffset int64 `json:"-"`
}

// inFlightEntry is the dispatcher's per-tx record. Owned exclusively by
// the dispatcher goroutine — no locks because no other goroutine touches
// it. The struct holds everything the dispatcher needs to make decisions
// (broadcast, hold as waiter, retry, terminalize) without going back to
// Kafka or the store.
type inFlightEntry struct {
	txid          string
	rawTx         []byte
	inputTXIDs    []string
	kafkaOffset   int64
	receivedAt    time.Time
	retryAttempts int
}

// offsetTracker exposes the dispatcher's Kafka-offset bookkeeping behind
// an interface so the actual data structure (min-heap with lazy delete,
// btree, etc.) can be swapped without touching the dispatcher's main
// loop. The contract is:
//
//   - Add records an offset that's now in flight.
//   - Done marks an offset as terminalized so it can be removed from the
//     "lowest unfinished" calculation.
//   - LowestUnfinished returns the smallest offset that hasn't yet been
//     Done. The caller commits one less than this (Kafka commit means
//     "advance past offsets [0, commit) — N is the offset of the next
//     message to read").
//   - Empty reports whether every Add'd offset has been Done.
//
// Concrete implementation lands in a follow-up commit.
type offsetTracker interface {
	Add(offset int64)
	Done(offset int64)
	LowestUnfinished() (offset int64, ok bool)
	Empty() bool
}

// Dispatcher is the single-goroutine engine that owns the dependency
// index, the in-flight set, and the retry queue. All state lives in its
// fields and is touched only by the goroutine running Run. External
// components communicate via the three channels:
//
//   - incomingMsgs: new transactions to consider (from the Kafka consumer)
//   - outgoingBatch: batches of eligible transactions to broadcast
//   - statusFlips: notifications of status changes (from broadcast
//     workers and the merkle-service callback handler)
//
// The single-goroutine model means every map lookup, mutation, and
// recursive cascade walk happens without locks. Throughput is bounded
// by JSON decode cost on the input side and the broadcast workers'
// capacity to drain the output side. Map operations on 32-byte-ish
// string keys are not the bottleneck.
type Dispatcher struct {
	// Channels — interface boundaries.
	incomingMsgs  <-chan propagationMsg
	outgoingBatch chan<- []*inFlightEntry
	statusFlips   <-chan *models.TransactionStatus

	// Dep index state. Only mutated by the goroutine running Run.
	inFlight       map[string]*inFlightEntry
	waiters        map[string]map[string]struct{} // parent txid → set of waiting child txids
	pendingParents map[string]map[string]struct{} // child txid → set of parents still in flight
	retryQueue     map[string]time.Time           // txid → next retry time

	// Pending batch accumulator. Flushed when len >= batchMaxSize or
	// batchFlushDeadline elapses, whichever comes first.
	pendingBatch       []*inFlightEntry
	batchFlushDeadline time.Time

	// Kafka offset bookkeeping — set on every consumed message, popped on
	// terminalization.
	offsetTracker offsetTracker

	// Config — set once at construction, never mutated.
	maxInFlight       int
	batchMaxSize      int
	batchFlushTimeout time.Duration
	retryMaxAttempts  int
	retryBackoffMs    int

	logger *zap.Logger

	// rejectedSink, when non-nil, receives terminal-REJECTED txids that
	// the dispatcher emits (via cascade or retry exhaustion). Wired by
	// the propagator so terminal status writes still land in the store
	// without coupling the dispatcher directly to the store layer.
	rejectedSink func(txid string, reason string)
}

// NewDispatcher constructs a Dispatcher with empty state. The caller is
// responsible for wiring the channels and starting Run on its own
// goroutine. Returns a ready-to-run instance — no background work has
// started yet.
func NewDispatcher(
	logger *zap.Logger,
	incomingMsgs <-chan propagationMsg,
	outgoingBatch chan<- []*inFlightEntry,
	statusFlips <-chan *models.TransactionStatus,
	offsets offsetTracker,
	maxInFlight, batchMaxSize, retryMaxAttempts, retryBackoffMs int,
	batchFlushTimeout time.Duration,
) *Dispatcher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Dispatcher{
		incomingMsgs:      incomingMsgs,
		outgoingBatch:     outgoingBatch,
		statusFlips:       statusFlips,
		inFlight:          make(map[string]*inFlightEntry),
		waiters:           make(map[string]map[string]struct{}),
		pendingParents:    make(map[string]map[string]struct{}),
		retryQueue:        make(map[string]time.Time),
		pendingBatch:      nil,
		offsetTracker:     offsets,
		maxInFlight:       maxInFlight,
		batchMaxSize:      batchMaxSize,
		batchFlushTimeout: batchFlushTimeout,
		retryMaxAttempts:  retryMaxAttempts,
		retryBackoffMs:    retryBackoffMs,
		logger:            logger,
	}
}

// SetRejectedSink wires a callback invoked when the dispatcher
// terminally rejects a tx (via cascade or retry exhaustion). The
// callback runs synchronously in the dispatcher goroutine — keep it
// cheap or hand off the work asynchronously inside the callback.
func (d *Dispatcher) SetRejectedSink(sink func(txid, reason string)) {
	d.rejectedSink = sink
}

// Run drives the dispatcher's main loop. Returns when ctx is canceled or
// either input channel closes. Caller invokes Run on its own goroutine;
// the function blocks for the dispatcher's lifetime. All dep-index state
// is mutated exclusively from inside this goroutine.
func (d *Dispatcher) Run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		d.armTimer(timer)

		select {
		case <-ctx.Done():
			d.logger.Info("dispatcher: context canceled, exiting")
			return

		case msg, ok := <-d.incomingMsgs:
			if !ok {
				d.logger.Info("dispatcher: incoming channel closed, exiting")
				return
			}
			d.handleIncoming(msg)

		case flip, ok := <-d.statusFlips:
			if !ok {
				d.logger.Info("dispatcher: status channel closed, exiting")
				return
			}
			d.handleStatusFlip(flip)

		case <-timer.C:
			d.handleTimerTick()
		}
	}
}

// armTimer programs the wake-up timer to fire at the earliest of: the
// batch flush deadline (if set) or the soonest retry-due time in the
// retry queue. If neither applies, the timer is set far in the future
// — an incoming message or status flip will preempt it anyway.
//
// Drains the timer channel before resetting per the standard time.Timer
// pattern, so a stale tick can't fire after we've moved past it.
func (d *Dispatcher) armTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	wakeAt := d.nextWakeUp()
	if wakeAt.IsZero() {
		timer.Reset(time.Hour)
		return
	}
	delay := time.Until(wakeAt)
	if delay < 0 {
		delay = 0
	}
	timer.Reset(delay)
}

// nextWakeUp returns the earliest time the dispatcher needs to do
// timer-driven work, or the zero time if no work is pending.
func (d *Dispatcher) nextWakeUp() time.Time {
	var earliest time.Time
	if !d.batchFlushDeadline.IsZero() {
		earliest = d.batchFlushDeadline
	}
	for _, due := range d.retryQueue {
		if earliest.IsZero() || due.Before(earliest) {
			earliest = due
		}
	}
	return earliest
}

// handleIncoming processes a freshly consumed Kafka message. The tx
// enters the in-flight set, gets its Kafka offset recorded, then either
// joins the pending batch (no unmet parents) or is registered as a
// waiter on its in-flight parents.
//
// Duplicate txids (already in the in-flight set) are ignored: this can
// happen on Kafka replay after restart, and the dispatcher's existing
// entry already reflects whatever decisions were made. We still record
// the offset so commit-tracking advances.
func (d *Dispatcher) handleIncoming(msg propagationMsg) {
	d.offsetTracker.Add(msg.KafkaOffset)

	if _, exists := d.inFlight[msg.TXID]; exists {
		d.logger.Debug("dispatcher: duplicate incoming tx, ignoring",
			zap.String("txid", msg.TXID),
			zap.Int64("offset", msg.KafkaOffset),
		)
		return
	}

	entry := &inFlightEntry{
		txid:        msg.TXID,
		rawTx:       msg.RawTx,
		inputTXIDs:  msg.InputTXIDs,
		kafkaOffset: msg.KafkaOffset,
		receivedAt:  time.Now(),
	}
	d.inFlight[msg.TXID] = entry

	// Determine which input parents are currently in flight. Only
	// in-flight parents create waits — parents already terminalized (or
	// never seen by Arcade) don't.
	var pending map[string]struct{}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == msg.TXID {
			continue
		}
		if _, parentInFlight := d.inFlight[parent]; !parentInFlight {
			continue
		}
		if pending == nil {
			pending = make(map[string]struct{})
		}
		pending[parent] = struct{}{}
	}

	if len(pending) == 0 {
		d.admitToBatch(entry)
		return
	}

	// Register as a waiter on every in-flight parent.
	d.pendingParents[msg.TXID] = pending
	for parent := range pending {
		set, ok := d.waiters[parent]
		if !ok {
			set = make(map[string]struct{})
			d.waiters[parent] = set
		}
		set[msg.TXID] = struct{}{}
	}
}

// handleStatusFlip processes a status update for an in-flight tx.
//
//   - ACCEPTED_BY_NETWORK: terminalize; release waiters whose
//     pendingParents becomes empty.
//   - REJECTED: terminalize; cascade-reject every waiter recursively.
//   - Anything else (intermediate states like SEEN_ON_NETWORK,
//     SENT_TO_NETWORK, etc.): logged but not acted on. Those don't
//     change the dispatcher's eligibility decisions; only ACCEPTED and
//     REJECTED release/cascade waiters.
//
// A flip for a txid the dispatcher doesn't know about is a no-op. That
// can legitimately happen for txs the dispatcher previously terminalized
// or for status flips from the merkle-service callback path firing on
// txs not in our current in-flight window.
func (d *Dispatcher) handleStatusFlip(flip *models.TransactionStatus) {
	if flip == nil {
		return
	}
	entry, ok := d.inFlight[flip.TxID]
	if !ok {
		return
	}

	switch flip.Status {
	case models.StatusAcceptedByNetwork:
		d.terminalize(entry)
		d.releaseWaiters(flip.TxID)

	case models.StatusRejected:
		// Differentiate retryable from terminal by HTTP status code.
		// 422 (ErrTxMissingParent) and connection-level failures
		// (statusCode == 0, captured separately upstream) are
		// retryable; everything else is terminal.
		if isRetryableStatusCode(flip.StatusCode) && entry.retryAttempts < d.retryMaxAttempts {
			entry.retryAttempts++
			d.retryQueue[flip.TxID] = computeRetryDeadline(d.retryBackoffMs, entry.retryAttempts)
			return
		}
		reason := flip.ExtraInfo
		if isRetryableStatusCode(flip.StatusCode) {
			reason = "broadcast retries exhausted"
		}
		d.terminalize(entry)
		d.cascadeReject(flip.TxID, reason)

	default:
		// Intermediate statuses don't change dep-index decisions.
	}
}

// terminalize removes the txid from the in-flight set, the retry queue,
// and marks its Kafka offset done so the commit pointer can advance.
// The caller is responsible for emitting the terminal status to any
// external sinks (cascade emits REJECTED via rejectedSink; ACCEPTED is
// expected to already have been written by the broadcast worker).
func (d *Dispatcher) terminalize(entry *inFlightEntry) {
	delete(d.inFlight, entry.txid)
	delete(d.retryQueue, entry.txid)
	d.offsetTracker.Done(entry.kafkaOffset)
}

// releaseWaiters processes a parent's ACCEPTED_BY_NETWORK flip by
// promoting any waiters whose pendingParents set becomes empty as a
// result. A waiter with multiple parents stays held until the last
// parent terminalizes.
func (d *Dispatcher) releaseWaiters(parentTxID string) {
	children, ok := d.waiters[parentTxID]
	if !ok {
		return
	}
	delete(d.waiters, parentTxID)

	for childTxID := range children {
		parents, hasPending := d.pendingParents[childTxID]
		if !hasPending {
			continue
		}
		delete(parents, parentTxID)
		if len(parents) > 0 {
			continue
		}
		delete(d.pendingParents, childTxID)

		// Child's last parent just terminalized — admit to batch.
		entry, stillInFlight := d.inFlight[childTxID]
		if !stillInFlight {
			continue
		}
		d.admitToBatch(entry)
	}
}

// cascadeReject processes a parent's REJECTED flip by recursively
// rejecting every waiter that depended on it. Reasons are tagged as
// parent-rejected so downstream consumers can distinguish them from
// direct rejections. Recursion is bounded by the dep graph's depth — in
// practice tx chains rarely exceed a few hops, and the in-flight set
// caps the total reachable set anyway.
func (d *Dispatcher) cascadeReject(parentTxID, reason string) {
	children, ok := d.waiters[parentTxID]
	if !ok {
		return
	}
	delete(d.waiters, parentTxID)

	cascadeReason := "parent rejected: " + parentTxID
	if reason != "" {
		cascadeReason = "parent rejected: " + parentTxID + " (" + reason + ")"
	}

	for childTxID := range children {
		delete(d.pendingParents, childTxID)
		entry, stillInFlight := d.inFlight[childTxID]
		if !stillInFlight {
			continue
		}
		d.terminalize(entry)
		if d.rejectedSink != nil {
			d.rejectedSink(childTxID, cascadeReason)
		}
		d.cascadeReject(childTxID, cascadeReason)
	}
}

// admitToBatch adds an eligible tx to the pending batch, flushing
// immediately if the batch is now full, otherwise arming the flush
// deadline if this is the first entry in the batch.
func (d *Dispatcher) admitToBatch(entry *inFlightEntry) {
	d.pendingBatch = append(d.pendingBatch, entry)
	if len(d.pendingBatch) == 1 {
		d.batchFlushDeadline = time.Now().Add(d.batchFlushTimeout)
	}
	if d.batchMaxSize > 0 && len(d.pendingBatch) >= d.batchMaxSize {
		d.flushBatch()
	}
}

// flushBatch emits the pending batch to the broadcast workers and
// clears the accumulator. Resets the flush deadline so the next admit
// starts a new window.
func (d *Dispatcher) flushBatch() {
	if len(d.pendingBatch) == 0 {
		d.batchFlushDeadline = time.Time{}
		return
	}
	batch := d.pendingBatch
	d.pendingBatch = nil
	d.batchFlushDeadline = time.Time{}
	d.outgoingBatch <- batch
}

// handleTimerTick processes timer-driven events: an elapsed batch flush
// deadline and any retries that have come due. Both are processed in one
// tick — they're independent and inexpensive.
func (d *Dispatcher) handleTimerTick() {
	now := time.Now()

	if !d.batchFlushDeadline.IsZero() && !now.Before(d.batchFlushDeadline) {
		d.flushBatch()
	}

	for txid, due := range d.retryQueue {
		if now.Before(due) {
			continue
		}
		delete(d.retryQueue, txid)
		entry, stillInFlight := d.inFlight[txid]
		if !stillInFlight {
			continue
		}
		d.admitToBatch(entry)
	}
}

// isRetryableStatusCode reports whether a broadcast outcome with the
// given HTTP status code should be retried. Per the plan:
//
//   - 422 (Unprocessable Entity, ErrTxMissingParent) → retry
//   - 0 (no response: timeout, network error) → retry
//   - everything else from a broadcast outcome → terminal
//
// Mempool-conflict is terminal — wallets resolve double-spends, not
// Arcade. The 500 case is also terminal in this scheme; the Teranode
// audit ensures genuine infra-failure-vs-validation is distinguished at
// the status code level.
func isRetryableStatusCode(statusCode int) bool {
	switch statusCode {
	case 0, 422:
		return true
	default:
		return false
	}
}

// computeRetryDeadline returns the next-retry time for the given
// attempt number using exponential backoff. Mirrors the existing
// ComputeBackoff math but operates inline so the dispatcher doesn't
// depend on the legacy reaper-flow helper that's slated for removal.
func computeRetryDeadline(baseBackoffMs, attempt int) time.Time {
	delay := time.Duration(baseBackoffMs) * time.Millisecond
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay > 30*time.Second {
			delay = 30 * time.Second
			break
		}
	}
	return time.Now().Add(delay)
}
