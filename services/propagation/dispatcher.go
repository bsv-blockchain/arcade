package propagation

import (
	"context"

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
// admitted tx. Exactly one of admitted / held is true.
type admitResult struct {
	admitted bool // true: tx was added to pendingMsgs, broadcast pending
	held     bool // true: registered as a waiter on in-flight parents
}

// admitRequest is the protocol between handleMessage and the
// dispatcher goroutine.
type admitRequest struct {
	msg   propagationMsg
	reply chan admitResult
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

// dispatcherConfig is the small subset of Propagator config the
// dispatcher needs at runtime.
type dispatcherConfig struct {
	maxPending int
}

// runDispatcher is the dispatcher goroutine's main loop. Started by
// New (so existing tests that don't call Start still have a running
// dispatcher) and runs for the lifetime of the propagation service.
// All dep-index + pendingMsgs state declared inside the function
// body — nothing leaks out, so nothing else can mutate it without
// going through the channels.
func (p *Propagator) runDispatcher(ctx context.Context, cfg dispatcherConfig) {
	// inFlight is the set of txids the dispatcher is aware of and has
	// not yet seen a terminal status for. Anything in pendingMsgs,
	// already broadcasting, or held as a waiter is in this set.
	// A child of any in-flight parent gets held — Teranode processes
	// bulk submissions in parallel, so parent and child must be in
	// SEPARATE batches with the parent terminalized before the child
	// is admitted.
	inFlight := make(map[string]struct{})

	// waiters maps a parent txid to the set of children currently
	// waiting on it. Populated by admit (when a child has any
	// in-flight parent) and drained by terminal events:
	// ACCEPTED releases direct waiters whose other parents have also
	// cleared; REJECTED cascade-rejects every descendant in the
	// subtree.
	waiters := make(map[string]map[string]struct{})

	// heldMsgs stores the held child's raw message so release can
	// re-enter it into pendingMsgs without going back to Kafka. We
	// don't keep a separate "pending parent count" — at release time
	// we recompute it by walking heldMsgs[child].InputTXIDs against
	// inFlight.
	heldMsgs := make(map[string]propagationMsg)

	// pendingMsgs is the broadcast-ready queue. drainCh pulls from
	// here; admit and release append to here. All on this one
	// goroutine — no locks.
	var pendingMsgs []propagationMsg

	for {
		// Nil-channel trick: when pendingMsgs is at the configured cap,
		// exclude admitCh from the select so handleMessage's send
		// blocks. Kafka consumer goroutine sits blocked → no more
		// pulls → broker holds messages. Terminal and drain events
		// keep firing; the instant they shrink pendingMsgs back under
		// the cap, admitCh is back in the select and the blocked
		// send unblocks.
		var admitChIfRoom <-chan admitRequest
		if cfg.maxPending <= 0 || len(pendingMsgs) < cfg.maxPending {
			admitChIfRoom = p.admitCh
		}

		select {
		case <-ctx.Done():
			return

		case req := <-admitChIfRoom:
			res := handleAdmit(req.msg, inFlight, waiters, heldMsgs, &pendingMsgs)
			req.reply <- res

		case ev := <-p.terminalCh:
			result := handleTerminal(ev, inFlight, waiters, heldMsgs, &pendingMsgs)
			ev.reply <- result

		case req := <-p.drainCh:
			batch := pendingMsgs
			pendingMsgs = nil
			req.reply <- batch
		}
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
func handleAdmit(
	msg propagationMsg,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) admitResult {
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

	if len(blocking) > 0 {
		// Hold as a waiter. Held txs DO go into inFlight (so
		// descendants can register on them) but NOT into pendingMsgs
		// (they're not on the broadcast path yet).
		inFlight[msg.TXID] = struct{}{}
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

	// Eligible for broadcast. Add to inFlight and pendingMsgs.
	inFlight[msg.TXID] = struct{}{}
	*pendingMsgs = append(*pendingMsgs, msg)
	return admitResult{admitted: true}
}

// handleTerminal processes a terminal status flip for txid. ACCEPTED
// releases direct waiters whose other-parent set has also cleared —
// each released waiter goes into pendingMsgs (and its own waiters
// stay held until IT terminalizes; no recursive cascade). REJECTED
// recursively cascade-rejects every descendant.
func handleTerminal(
	ev terminalEvent,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) terminalResult {
	delete(inFlight, ev.txid)

	switch ev.status {
	case models.StatusAcceptedByNetwork:
		releaseWaiters(ev.txid, inFlight, waiters, heldMsgs, pendingMsgs)
		return terminalResult{}

	case models.StatusRejected:
		return terminalResult{
			cascaded: cascadeReject(ev.txid, inFlight, waiters, heldMsgs),
		}

	default:
		// Intermediate statuses don't change dispatcher state.
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
	inFlight map[string]struct{},
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
	inFlight map[string]struct{},
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
// their own, only because an ancestor did.
func cascadeReject(
	rejectedTxID string,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
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

// admitToDispatcher is the consumer-side helper. Sends the tx, waits
// for the dispatcher's verdict, returns it.
func (p *Propagator) admitToDispatcher(msg propagationMsg) admitResult {
	reply := make(chan admitResult, 1)
	p.admitCh <- admitRequest{msg: msg, reply: reply}
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
