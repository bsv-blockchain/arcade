package propagation

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// The dispatcher is a single-goroutine engine that owns all dep-aware
// state: inFlight, waiters, heldMsgs, and pendingMsgs. Every read and
// write to that state happens on this one goroutine — no locks
// anywhere.
//
// External components communicate via three channels:
//
//   - admitCh: handleMessage sends a propagationMsg here and waits for
//     a synchronous reply describing what happened (admitted to the
//     pending batch, or held as a waiter on in-flight parents).
//
//   - terminalCh: applyTerminalStatuses sends a terminalEvent here per
//     terminalized txid and waits for a terminalResult naming the
//     cascaded descendants the caller has to write REJECTED rows for.
//     Released waiters (parent ACCEPTED) are re-entered into
//     pendingMsgs by the dispatcher itself — the caller never touches
//     that slice.
//
//   - drainCh: flushBatch sends a drainRequest here to pull the
//     current pendingMsgs as a batch; the dispatcher hands over the
//     slice and clears its local state.
//
// Backpressure: when pendingMsgs is at maxPending the dispatcher
// excludes admitCh from its select, so handleMessage's send blocks
// and the Kafka consumer goroutine pauses pulling — backpressure
// flows back to the broker naturally, no DLQ.

// admitResult tells handleMessage what the dispatcher did with the
// admitted tx. Exactly one of admitted / held / duplicate is true.
// duplicate means the same txid was already in flight: the original
// admission still owns the broadcast and waiter graph, and the new
// message's Kafka offset has been added-and-immediately-marked-done
// on the tracker so it does not pin the commit watermark.
type admitResult struct {
	admitted  bool // true: tx was added to pendingMsgs, broadcast pending
	held      bool // true: registered as a waiter on in-flight parents
	duplicate bool // true: txid already in flight; offset bookkept and dropped
}

// admitRequest is the protocol between handleMessage and the
// dispatcher goroutine. offset is the Kafka offset the message came
// from; the dispatcher tracks it on the offsetTracker so the Kafka
// commit watermark cannot advance past unfinished work.
type admitRequest struct {
	msg    propagationMsg
	offset int64
	reply  chan admitResult
}

// terminalEvent is the protocol between applyTerminalStatuses and the
// dispatcher goroutine.
type terminalEvent struct {
	txid   string
	status models.Status
	reply  chan terminalResult
}

// terminalResult names cascaded descendants the caller has to write
// terminal REJECTED rows for. Released waiters (those whose blocking
// parents have all cleared) are NOT returned — the dispatcher
// re-enters them into pendingMsgs directly, so the next flushBatch
// picks them up without any caller action.
type terminalResult struct {
	cascaded []string
}

// drainRequest is the protocol between flushBatch and the dispatcher.
// The dispatcher replies with the current pendingMsgs and clears its
// local pendingMsgs state.
type drainRequest struct {
	reply chan []propagationMsg
}

// requeueRequest sends an already-in-flight tx back through admission
// after a transient infra failure (Teranode infra slot, merkle /watch
// error, etc.). The tx is already in inFlight and its offset is already
// on the tracker — the dispatcher just re-checks parents and routes to
// either heldMsgs or pendingMsgs. No reply: the caller doesn't need to
// know which bucket the tx landed in.
type requeueRequest struct {
	msg propagationMsg
}

// dispatcherConfig is the small subset of Propagator config the
// dispatcher needs at runtime.
type dispatcherConfig struct {
	maxPending int
}

// runDispatcher is the single state-owning loop. ALL dep-aware state
// (inFlight, waiters, heldMsgs, pendingMsgs, the offsetTracker, and the
// per-offset *kafka.Message references used for marking) lives in this
// function's local variables. Nothing else touches any of it — every
// mutation arrives via the channels read in the select.
//
// Two modes:
//
//   - Production: invoked as the kafka.ClaimHandler with a non-nil claim.
//     The loop runs on Sarama's per-claim goroutine; claim.Messages() is
//     the message source and claim.MarkMessage is called inline when an
//     offset terminalizes. Returns when the claim ends.
//   - Test: invoked with a nil claim from a goroutine started by tests.
//     admitCh is the message source; no MarkMessage calls.
//
// One goroutine, no locks, no atomics.
func (p *Propagator) runDispatcher(ctx context.Context, claim kafka.Claim, cfg dispatcherConfig) error {
	inFlight := make(map[string]int64)
	waiters := make(map[string]map[string]struct{})
	heldMsgs := make(map[string]propagationMsg)
	var pendingMsgs []propagationMsg
	tracker := newOffsetTracker()
	// pendingMarks is the per-offset Kafka-message reference we hand back
	// to claim.MarkMessage when its offset terminalizes. Only populated
	// when claim != nil; in test mode admitRequests carry no Kafka
	// message and pendingMarks stays empty.
	pendingMarks := make(map[int64]*kafka.Message)

	var claimMsgCh <-chan *kafka.Message
	if claim != nil {
		claimMsgCh = claim.Messages()
	}

	// In production the ticker drives periodic flushes (50ms). In test
	// mode (nil claim) tests drive flushes explicitly via flushBatch /
	// drainCh, so a live ticker would race with the explicit drain.
	// flushTickC is nil in test mode, which (per the nil-channel-in-
	// select trick) means the ticker case never fires.
	var flushTickC <-chan time.Time
	if claim != nil {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		flushTickC = t.C
	}

	for {
		// Keep the pending-depth gauge in sync at the top of every
		// iteration. Every branch below that mutates pendingMsgs
		// (handleAdmit, handleRequeue, handleTerminal, drain, flush)
		// is observed exactly once before the next select fires.
		// One atomic write per iteration is negligible vs the throughput
		// the dispatcher handles.
		metrics.PropagationPendingDepth.Set(float64(len(pendingMsgs)))

		// Backpressure: nil-channel trick excludes incoming-message
		// sources from the select when pendingMsgs is at cap. Both
		// claim.Messages() and admitCh participate, so the same cap
		// applies in either mode.
		var admitChIfRoom <-chan admitRequest
		var claimChIfRoom <-chan *kafka.Message
		if cfg.maxPending <= 0 || len(pendingMsgs) < cfg.maxPending {
			admitChIfRoom = p.admitCh
			claimChIfRoom = claimMsgCh
		}

		select {
		case <-ctx.Done():
			return nil

		case msg, ok := <-claimChIfRoom:
			if !ok {
				// Claim channel closed → claim ended; exit cleanly so
				// Sarama can move on.
				return nil
			}
			var propMsg propagationMsg
			if err := json.Unmarshal(msg.Value, &propMsg); err != nil {
				p.logger.Warn(
					"decoding propagation message",
					zap.Int64("offset", msg.Offset),
					zap.Error(err),
				)
				// Mark malformed messages so the consumer doesn't
				// redeliver them forever. Drop on the floor.
				claim.MarkMessage(msg)
				continue
			}
			if len(propMsg.RawTx) == 0 {
				p.logger.Warn("propagation message has empty raw_tx", zap.Int64("offset", msg.Offset))
				claim.MarkMessage(msg)
				continue
			}
			handleAdmit(propMsg, msg.Offset, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
			pendingMarks[msg.Offset] = msg

		case req := <-admitChIfRoom:
			res := handleAdmit(req.msg, req.offset, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
			req.reply <- res

		case req := <-p.requeueCh:
			handleRequeue(req.msg, inFlight, waiters, heldMsgs, &pendingMsgs)

		case ev := <-p.terminalCh:
			result := handleTerminal(ev, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
			ev.reply <- result
			advanceMarks(claim, tracker, pendingMarks)

		case req := <-p.drainCh:
			batch := pendingMsgs
			pendingMsgs = nil
			req.reply <- batch

		case <-flushTickC:
			if len(pendingMsgs) == 0 {
				continue
			}
			// Non-blocking semaphore acquire: if all processBatch
			// slots are busy, leave pendingMsgs alone and try next
			// tick. The reads from claim/admit pause naturally
			// because the cap path keeps appending; backpressure
			// flows the right direction.
			select {
			case p.processBatchSem <- struct{}{}:
			default:
				continue
			}
			batch := pendingMsgs
			pendingMsgs = nil
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
		}
	}
}

// advanceMarks walks pendingMarks and calls claim.MarkMessage on every
// offset that is strictly below the dispatcher's current lowest in-flight
// offset. Idempotent and cheap when nothing has advanced. No-op when
// claim is nil (test mode).
func advanceMarks(claim kafka.Claim, tracker *offsetTracker, pendingMarks map[int64]*kafka.Message) {
	if claim == nil {
		return
	}
	lowest, hasUnfinished := tracker.LowestUnfinished()
	for offset, msg := range pendingMarks {
		if hasUnfinished && offset >= lowest {
			continue
		}
		claim.MarkMessage(msg)
		delete(pendingMarks, offset)
	}
}

// handleAdmit decides what to do with a new tx based on whether any
// of its inputs are currently in flight:
//
//   - Parent in inFlight → block this child. Teranode processes bulk
//     submissions in parallel so we can't trust ordering across
//     concurrent /txs calls; the child has to wait until every parent
//     has terminalized.
//   - Parent not in inFlight → already mined, never seen by Arcade,
//     or otherwise out of scope. Doesn't block this admit.
//
// If ANY input requires holding, the whole tx is held as a waiter on
// every blocking parent. Otherwise the tx is admitted to inFlight
// and appended to pendingMsgs.
//
// The maxPending cap is enforced upstream by the select loop's
// nil-channel pattern; handleAdmit never sees a full-queue admit.
//
// The Kafka offset is recorded on tracker so the commit watermark
// can never advance past this in-flight tx.
func handleAdmit(
	msg propagationMsg,
	offset int64,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
	tracker *offsetTracker,
) admitResult {
	// Duplicate admission: the same txid is already in flight (Kafka
	// redelivery, or a publisher that slipped past the intake dedup
	// CAS). The original entry still owns the broadcast and waiter
	// graph — overwriting inFlight[msg.TXID] would strand its offset
	// on the tracker forever, freezing the commit watermark. Add the
	// new offset and immediately mark it done so advanceMarks can
	// flush it once the original terminalizes, and return without
	// touching pendingMsgs.
	if _, exists := inFlight[msg.TXID]; exists {
		tracker.Add(offset)
		tracker.Done(offset)
		return admitResult{duplicate: true}
	}

	// Identify blocking parents — any input that's currently in
	// flight, regardless of whether it's in pendingMsgs or already
	// broadcasting. Teranode's parallel bulk processing means we
	// can't trust same-batch ordering.
	var blocking map[string]struct{}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == msg.TXID {
			continue
		}
		if _, inFlt := inFlight[parent]; !inFlt {
			continue
		}
		if blocking == nil {
			blocking = make(map[string]struct{})
		}
		blocking[parent] = struct{}{}
	}

	// Record the offset on the tracker for either branch — both held
	// and admitted txs are in-flight from the consumer's perspective
	// and must pin the commit watermark below this offset.
	inFlight[msg.TXID] = offset
	tracker.Add(offset)

	if len(blocking) > 0 {
		// Hold as a waiter. Held txs DO go into inFlight (so
		// descendants can register on them) but NOT into pendingMsgs
		// (they're not on the broadcast path yet).
		for parent := range blocking {
			set, ok := waiters[parent]
			if !ok {
				set = make(map[string]struct{})
				waiters[parent] = set
			}
			set[msg.TXID] = struct{}{}
		}
		heldMsgs[msg.TXID] = msg
		return admitResult{held: true}
	}

	// Eligible for broadcast. Add to pendingMsgs.
	*pendingMsgs = append(*pendingMsgs, msg)
	return admitResult{admitted: true}
}

// handleRequeue re-routes an already-in-flight tx after a transient
// infra failure. The tx is already in inFlight (from the original
// handleAdmit) and its offset is already on the tracker, so this only
// has to re-check parents and place the msg into either heldMsgs or
// pendingMsgs.
//
// A tx whose own status flipped terminal (e.g. via a sibling's cascade)
// won't be in inFlight anymore; in that case the requeue is a no-op.
func handleRequeue(
	msg propagationMsg,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) {
	if _, ok := inFlight[msg.TXID]; !ok {
		// Terminalized between the failed broadcast and the requeue
		// (e.g. cascade-rejected by a parent). Drop on the floor.
		return
	}

	var blocking map[string]struct{}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == msg.TXID {
			continue
		}
		if _, inFlt := inFlight[parent]; !inFlt {
			continue
		}
		if blocking == nil {
			blocking = make(map[string]struct{})
		}
		blocking[parent] = struct{}{}
	}

	if len(blocking) > 0 {
		for parent := range blocking {
			set, ok := waiters[parent]
			if !ok {
				set = make(map[string]struct{})
				waiters[parent] = set
			}
			set[msg.TXID] = struct{}{}
		}
		heldMsgs[msg.TXID] = msg
		return
	}

	*pendingMsgs = append(*pendingMsgs, msg)
}

// handleTerminal processes a terminal status flip for txid. ACCEPTED
// releases direct waiters whose other-parent set has also cleared —
// each released waiter goes into pendingMsgs (and its own waiters
// stay held until IT terminalizes; no recursive cascade). REJECTED
// recursively cascade-rejects every descendant.
//
// Both branches mark the txid's offset Done on the tracker (and every
// cascaded descendant's offset too) so the Kafka commit watermark can
// advance past them.
func handleTerminal(
	ev terminalEvent,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
	tracker *offsetTracker,
) terminalResult {
	switch ev.status {
	case models.StatusAcceptedByNetwork:
		if offset, ok := inFlight[ev.txid]; ok {
			tracker.Done(offset)
		}
		delete(inFlight, ev.txid)
		releaseWaiters(ev.txid, inFlight, waiters, heldMsgs, pendingMsgs)
		return terminalResult{}

	case models.StatusRejected:
		if offset, ok := inFlight[ev.txid]; ok {
			tracker.Done(offset)
		}
		delete(inFlight, ev.txid)
		return terminalResult{
			cascaded: cascadeReject(ev.txid, inFlight, waiters, heldMsgs, tracker),
		}

	default:
		// Intermediate statuses don't change dispatcher state and
		// don't advance the offset watermark.
		return terminalResult{}
	}
}

// releaseWaiters processes a parent that just terminalized ACCEPTED.
// Walks waiters[parent] one level deep. For each child, canRelease
// checks whether the child has any OTHER in-flight parents — if all
// are cleared, the child moves into pendingMsgs. No recursion: a
// released child's own waiters stay held until the child itself
// terminalizes (Teranode processes batches in parallel, so child and
// grandchild can't share a batch).
func releaseWaiters(
	parentTxID string,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) {
	children, ok := waiters[parentTxID]
	if !ok {
		return
	}
	delete(waiters, parentTxID)
	for child := range children {
		msg, ready := canRelease(child, inFlight, heldMsgs)
		if !ready {
			continue
		}
		cleanupWaiterEntries(child, parentTxID, waiters, heldMsgs)
		delete(heldMsgs, child)
		*pendingMsgs = append(*pendingMsgs, msg)
	}
}

// canRelease asks: are all of child's blocking parents resolved?
// A parent is "blocking" if it's still in inFlight. Recomputes the
// answer from heldMsgs[child].InputTXIDs at call time — we don't
// maintain a per-child pending-parent count.
func canRelease(
	child string,
	inFlight map[string]int64,
	heldMsgs map[string]propagationMsg,
) (propagationMsg, bool) {
	msg, ok := heldMsgs[child]
	if !ok {
		return propagationMsg{}, false
	}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == child {
			continue
		}
		if _, inFlt := inFlight[parent]; inFlt {
			return propagationMsg{}, false
		}
	}
	return msg, true
}

// cascadeReject walks the dep graph from rejectedTxID forward and
// returns every descendant that should be terminally rejected. The
// caller writes a REJECTED row for each, with "parent rejected" as
// the ExtraInfo — the descendants didn't fail for any reason of
// their own, only because an ancestor did. Every cascaded descendant's
// Kafka offset is marked Done on the tracker so the commit watermark
// can advance past them once the caller writes the REJECTED rows.
func cascadeReject(
	rejectedTxID string,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	tracker *offsetTracker,
) []string {
	var cascaded []string
	queue := []string{rejectedTxID}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		children, ok := waiters[parent]
		if !ok {
			continue
		}
		delete(waiters, parent)
		for child := range children {
			cleanupWaiterEntries(child, parent, waiters, heldMsgs)
			delete(heldMsgs, child)
			if offset, ok := inFlight[child]; ok {
				tracker.Done(offset)
			}
			delete(inFlight, child)
			cascaded = append(cascaded, child)
			queue = append(queue, child)
		}
	}
	return cascaded
}

// cleanupWaiterEntries removes child from every OTHER parent's
// waiters set, where "other" means parents in heldMsgs[child].InputTXIDs
// other than skipParent (the one being processed by the calling
// release / cascade walk). Prevents dangling waiter entries that
// would otherwise show up when those other parents terminalize
// later.
func cleanupWaiterEntries(
	child, skipParent string,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
) {
	msg, ok := heldMsgs[child]
	if !ok {
		return
	}
	for _, otherParent := range msg.InputTXIDs {
		if otherParent == "" || otherParent == child || otherParent == skipParent {
			continue
		}
		set, ok := waiters[otherParent]
		if !ok {
			continue
		}
		delete(set, child)
		if len(set) == 0 {
			delete(waiters, otherParent)
		}
	}
}

// dispatcherChannelBuffer sizes the dispatcher's input channels. The
// reply protocol means handleMessage and applyTerminalStatuses block
// on a goroutine round-trip per call, but the buffer absorbs short
// bursts so a momentarily slow dispatcher (during a large cascade
// walk, say) doesn't immediately stall the consumer or broadcast
// workers.
const dispatcherChannelBuffer = 256

// admitToDispatcher is the consumer-side helper. Sends the tx and its
// Kafka offset, waits for the dispatcher's verdict, returns it.
func (p *Propagator) admitToDispatcher(msg propagationMsg, offset int64) admitResult {
	reply := make(chan admitResult, 1)
	p.admitCh <- admitRequest{msg: msg, offset: offset, reply: reply}
	return <-reply
}

// notifyTerminalToDispatcher is the post-broadcast helper. Tells the
// dispatcher the txid reached terminal status, returns the cascaded
// descendants (caller writes REJECTED rows for them).
func (p *Propagator) notifyTerminalToDispatcher(txid string, status models.Status) terminalResult {
	reply := make(chan terminalResult, 1)
	p.terminalCh <- terminalEvent{txid: txid, status: status, reply: reply}
	return <-reply
}

// drainPending asks the dispatcher for the current pendingMsgs as a
// batch. The dispatcher clears its slice and hands the snapshot to
// the caller; the caller owns it fully and processBatch can mutate
// it as needed.
func (p *Propagator) drainPending() []propagationMsg {
	reply := make(chan []propagationMsg, 1)
	p.drainCh <- drainRequest{reply: reply}
	return <-reply
}

// requeueToDispatcher sends a tx back through admission after a
// transient infra failure. Fire-and-forget — the dispatcher's reply
// (held vs pending) doesn't change anything the caller can act on.
// The send is buffered (dispatcherChannelBuffer); a momentarily
// saturated buffer applies natural backpressure to the caller.
func (p *Propagator) requeueToDispatcher(msg propagationMsg) {
	p.requeueCh <- requeueRequest{msg: msg}
}
