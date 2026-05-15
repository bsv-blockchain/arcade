package propagation

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// The dispatcher is a single-goroutine engine that owns ALL dep-aware
// state: inFlight, inPending, waiters, heldMsgs, and pendingMsgs.
// Every read and write to that state happens on this one goroutine —
// no locks anywhere.
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
//     Released waiters are re-entered into pendingMsgs by the
//     dispatcher itself — the caller never touches that slice.
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
// local pendingMsgs + inPending state.
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
	// Descendants check inFlight to decide whether they need to wait.
	inFlight := make(map[string]struct{})

	// inPending is the subset of inFlight currently in pendingMsgs —
	// i.e., the txids that will go in the NEXT broadcast batch.
	// Children whose only in-flight parents are also in inPending can
	// join the same batch (Teranode handles intra-batch ordering).
	// Children with a parent that's in inFlight but NOT in inPending
	// must hold — the parent is in a different in-flight batch and
	// Teranode can't coordinate across batches.
	inPending := make(map[string]struct{})

	// waiters maps a parent txid to the set of children currently
	// waiting on it. Populated by admit (when a child has an
	// inFlight-but-not-inPending parent) and drained by terminal
	// (ACCEPTED releases the parent's waiters, REJECTED cascade-
	// rejects them).
	waiters := make(map[string]map[string]struct{})

	// heldMsgs stores the held child's raw message so release can
	// re-enter it into pendingMsgs without going back to Kafka. We
	// don't keep a separate "pending parent count" — at release time
	// we recompute it by walking heldMsgs[child].InputTXIDs against
	// inFlight/inPending. One less map to maintain.
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
			res := handleAdmit(req.msg, inFlight, inPending, waiters, heldMsgs, &pendingMsgs)
			req.reply <- res

		case ev := <-p.terminalCh:
			result := handleTerminal(ev, inFlight, inPending, waiters, heldMsgs, &pendingMsgs)
			ev.reply <- result

		case req := <-p.drainCh:
			batch := pendingMsgs
			pendingMsgs = nil
			// Drain semantics: every txid currently in inPending is
			// moving from "queued for next batch" to "in-flight on
			// the broadcast pipeline." inPending clears; the txids
			// stay in inFlight (still tracked, still blocking
			// descendants from joining a future batch they can't
			// reach) until they terminalize.
			for txid := range inPending {
				delete(inPending, txid)
			}
			req.reply <- batch
		}
	}
}

// handleAdmit decides what to do with a new tx based on whether its
// inputs are currently in flight, and if so, whether they're in the
// same pending batch:
//
//   - Parent in inPending → fine, will go in the same batch
//     (Teranode handles intra-batch ordering internally).
//   - Parent in inFlight but NOT in inPending → parent is in a
//     different in-flight batch or is itself held; we must hold this
//     child until the parent is reachable.
//   - Parent not in inFlight → already mined, never seen by Arcade,
//     or otherwise out of scope. Doesn't block this admit.
//
// If ANY input requires holding, the whole tx is held as a waiter on
// every such blocking parent. Otherwise the tx is admitted to both
// inFlight and inPending and appended to pendingMsgs.
//
// The maxPending cap is enforced upstream by the select loop's
// nil-channel pattern; handleAdmit never sees a full-queue admit.
func handleAdmit(
	msg propagationMsg,
	inFlight map[string]struct{},
	inPending map[string]struct{},
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) admitResult {
	// Identify blocking parents — in flight but not in this batch.
	var blocking map[string]struct{}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == msg.TXID {
			continue
		}
		if _, inFlt := inFlight[parent]; !inFlt {
			continue
		}
		if _, inPnd := inPending[parent]; inPnd {
			// Parent is queued for the same batch — fine, Teranode
			// handles ordering inside the /txs POST.
			continue
		}
		// Parent is in a different in-flight batch or is itself
		// held — block this child on it.
		if blocking == nil {
			blocking = make(map[string]struct{})
		}
		blocking[parent] = struct{}{}
	}

	if len(blocking) > 0 {
		// Hold as a waiter. Held txs DO go into inFlight (so
		// descendants can register on them) but NOT into inPending
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

	// Eligible for broadcast. Add to both inFlight and inPending so
	// subsequent children of this tx can co-batch with it.
	inFlight[msg.TXID] = struct{}{}
	inPending[msg.TXID] = struct{}{}
	*pendingMsgs = append(*pendingMsgs, msg)
	return admitResult{admitted: true}
}

// handleTerminal processes a terminal status flip for txid. ACCEPTED
// releases waiters whose blocking-parent set becomes empty (including
// recursive cascade — a released waiter's own waiters re-check and
// may release, since the just-released tx is now in inPending and
// no longer blocking). REJECTED recursively cascade-rejects every
// descendant.
func handleTerminal(
	ev terminalEvent,
	inFlight map[string]struct{},
	inPending map[string]struct{},
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) terminalResult {
	delete(inFlight, ev.txid)
	delete(inPending, ev.txid)

	switch ev.status {
	case models.StatusAcceptedByNetwork:
		releaseWaiters(ev.txid, inFlight, inPending, waiters, heldMsgs, pendingMsgs)
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

// releaseWaiters processes a parent that just left inFlight or moved
// into inPending. Walks waiters[parent], evaluating each child via
// canRelease — children whose remaining blockers are all gone (or in
// inPending) move to pendingMsgs + inPending themselves, and the
// process recurses on their waiters. This propagates a release
// through a held chain in one call, so a chain whose root just
// terminalized ACCEPTED all queues into the next batch together.
func releaseWaiters(
	parentTxID string,
	inFlight map[string]struct{},
	inPending map[string]struct{},
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) {
	queue := []string{parentTxID}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		children, ok := waiters[p]
		if !ok {
			continue
		}
		delete(waiters, p)
		for child := range children {
			msg, ready := canRelease(child, inFlight, inPending, heldMsgs)
			if !ready {
				continue
			}
			cleanupWaiterEntries(child, p, waiters, heldMsgs)
			delete(heldMsgs, child)
			inPending[child] = struct{}{}
			*pendingMsgs = append(*pendingMsgs, msg)
			// The just-released child is now in inPending; its own
			// waiters may release as a result. Cascade.
			queue = append(queue, child)
		}
	}
}

// canRelease asks: are all of child's blocking parents resolved?
// A parent is "blocking" if it's in inFlight but not in inPending
// (i.e., in a different in-flight batch or held). Recomputes the
// answer from heldMsgs[child].InputTXIDs at call time — we don't
// maintain a per-child pending-parent count.
func canRelease(
	child string,
	inFlight map[string]struct{},
	inPending map[string]struct{},
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
		if _, inFlt := inFlight[parent]; !inFlt {
			continue
		}
		if _, inPnd := inPending[parent]; inPnd {
			continue
		}
		return propagationMsg{}, false
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
// batch. The dispatcher clears its slice and inPending set and hands
// the snapshot to the caller; the caller owns it fully and
// processBatch can mutate it as needed.
func (p *Propagator) drainPending() []propagationMsg {
	reply := make(chan []propagationMsg, 1)
	p.drainCh <- drainRequest{reply: reply}
	return <-reply
}
