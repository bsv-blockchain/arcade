package propagation

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// The dispatcher is a single-goroutine engine that owns ALL dep-aware
// state: the in-flight set, waiters map, pendingParents map, held
// messages, AND the pending-broadcast slice. Every read and write to
// that state happens on this one goroutine — no locks anywhere.
//
// External components communicate via three channels:
//
//   - admitCh: handleMessage sends a propagationMsg here and waits
//     for a synchronous reply describing what happened (admitted to
//     pending batch, held as a waiter on in-flight parents, or
//     rejected because pendingMsgs is at its max-pending cap).
//
//   - terminalCh: applyTerminalStatuses sends a terminalEvent here
//     per terminalized txid and waits for a terminalResult that
//     names the cascaded descendants the caller has to write
//     REJECTED rows for. Released waiters are re-entered into
//     pendingMsgs by the dispatcher itself — the caller never
//     touches that slice.
//
//   - drainCh: flushBatch sends a drainRequest here to pull the
//     current pendingMsgs as a batch; the dispatcher replies with
//     the slice and clears its local state.
//
// Since all dep-aware state plus pendingMsgs lives behind the select
// loop, there is no shared state and no mutex anywhere in the
// propagation pipeline's dep-aware path.

// admitResult tells handleMessage what the dispatcher did with the
// admitted tx. Exactly one of admitted / held / full is true.
type admitResult struct {
	admitted bool // true: tx was added to pendingMsgs, broadcast pending
	held     bool // true: registered as a waiter on in-flight parents
	full     bool // true: pendingMsgs is at its cap; caller signals backpressure
}

// admitRequest is the protocol between handleMessage and the
// dispatcher goroutine.
type admitRequest struct {
	msg   propagationMsg
	reply chan admitResult
}

// terminalEvent is the protocol between applyTerminalStatuses and the
// dispatcher goroutine. status is the just-applied terminal status of
// txid; reason carries the rejection reason for the REJECTED case so
// cascaded descendants can attribute their own rejection back to the
// original cause.
type terminalEvent struct {
	txid   string
	status models.Status
	reason string
	reply  chan terminalResult
}

// terminalResult names cascaded descendants the caller has to write
// terminal REJECTED rows for. Released waiters (those whose final
// parent just terminalized ACCEPTED) are NOT returned — the
// dispatcher re-enters them into pendingMsgs directly, so the
// next flushBatch picks them up without any caller action.
type terminalResult struct {
	cascaded []cascadedRejection
}

// cascadedRejection pairs a cascaded txid with the reason from the
// ancestor that originally caused the rejection. The caller writes
// these as REJECTED rows with ExtraInfo populated.
type cascadedRejection struct {
	txid   string
	reason string
}

// drainRequest is the protocol between flushBatch and the dispatcher.
// The dispatcher replies with the current pendingMsgs (as a slice the
// caller owns) and clears its own slice.
type drainRequest struct {
	reply chan []propagationMsg
}

// dispatcherConfig is the small subset of Propagator config the
// dispatcher needs at runtime. Captured into local variables when
// runDispatcher starts so the goroutine doesn't have to reach back
// into the Propagator's shared struct fields after that point.
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
	// inFlight is the set of txids the dispatcher knows about and has
	// not yet seen a terminal status for. A tx is in inFlight whether
	// it's pending broadcast OR held as a waiter — both states count
	// as "Arcade is aware of this tx, work is pending." Descendants
	// check inFlight to decide whether to wait.
	inFlight := make(map[string]struct{})

	// waiters maps a parent txid to the set of children currently
	// waiting on it. Populated by admit (when a child has in-flight
	// parents) and drained by terminal (ACCEPTED releases, REJECTED
	// cascade-rejects).
	waiters := make(map[string]map[string]struct{})

	// pendingParents is the inverse of waiters: child txid → set of
	// parents the child is still waiting on. Used so a child with
	// multiple parents only releases when the LAST parent terminalizes.
	pendingParents := make(map[string]map[string]struct{})

	// heldMsgs stores the held child's raw message so release can
	// re-enter it into pendingMsgs without going back to Kafka.
	heldMsgs := make(map[string]propagationMsg)

	// pendingMsgs is the broadcast-ready queue. flushBatch drains it
	// via a drainCh request; admit appends to it; release (from
	// terminal events) also appends to it. All three on this one
	// goroutine — no locks.
	var pendingMsgs []propagationMsg

	for {
		select {
		case <-ctx.Done():
			return

		case req := <-p.admitCh:
			res := handleAdmit(req.msg, inFlight, waiters, pendingParents, heldMsgs, &pendingMsgs, cfg.maxPending)
			req.reply <- res

		case ev := <-p.terminalCh:
			result := handleTerminal(ev, inFlight, waiters, pendingParents, heldMsgs, &pendingMsgs)
			ev.reply <- result

		case req := <-p.drainCh:
			batch := pendingMsgs
			pendingMsgs = nil
			req.reply <- batch
		}
	}
}

// handleAdmit decides what to do with a new tx:
//   - If any input txid is in inFlight, register the tx as a waiter
//     on each such parent and return held (no append to pendingMsgs).
//   - Otherwise, if pendingMsgs is already at maxPending capacity,
//     return full WITHOUT adding to inFlight — the caller will
//     surface backpressure to the Kafka consumer and the message
//     won't be processed at all. (No leak into inFlight.)
//   - Otherwise, add to inFlight, append to pendingMsgs, return
//     admitted.
//
// Order of these checks matters: parent check first so children of
// in-flight parents never count against the maxPending cap (they're
// not on the broadcast path yet).
func handleAdmit(
	msg propagationMsg,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	pendingParents map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
	maxPending int,
) admitResult {
	// Identify in-flight parents.
	var pending map[string]struct{}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == msg.TXID {
			continue
		}
		if _, ok := inFlight[parent]; !ok {
			continue
		}
		if pending == nil {
			pending = make(map[string]struct{})
		}
		pending[parent] = struct{}{}
	}

	if len(pending) > 0 {
		// Hold as a waiter. Held txs DO go into inFlight so
		// descendants can register on them; they DON'T count against
		// maxPending since they aren't on the broadcast path.
		inFlight[msg.TXID] = struct{}{}
		pendingParents[msg.TXID] = pending
		for parent := range pending {
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

	// No in-flight parents — eligible for broadcast. Cap check
	// happens BEFORE we add to inFlight so a rejection never leaks
	// state.
	if maxPending > 0 && len(*pendingMsgs) >= maxPending {
		return admitResult{full: true}
	}

	inFlight[msg.TXID] = struct{}{}
	*pendingMsgs = append(*pendingMsgs, msg)
	return admitResult{admitted: true}
}

// handleTerminal processes a terminal status flip for txid. ACCEPTED
// releases waiters whose parent set becomes empty as a result;
// REJECTED recursively cascades rejection through every descendant.
// In both cases txid is removed from inFlight. Released waiters get
// appended directly to pendingMsgs by the dispatcher (no caller
// action needed); cascaded descendants are returned via the result
// so the caller can write REJECTED rows for them.
func handleTerminal(
	ev terminalEvent,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	pendingParents map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) terminalResult {
	delete(inFlight, ev.txid)

	switch ev.status {
	case models.StatusAcceptedByNetwork:
		released := releaseWaiters(ev.txid, waiters, pendingParents, heldMsgs)
		*pendingMsgs = append(*pendingMsgs, released...)
		return terminalResult{}

	case models.StatusRejected:
		return terminalResult{
			cascaded: cascadeReject(ev.txid, ev.reason, inFlight, waiters, pendingParents, heldMsgs),
		}

	default:
		// Intermediate statuses (SEEN_ON_NETWORK, etc.) aren't part
		// of the terminal contract; the caller shouldn't be sending
		// them here. No state change, empty result.
		return terminalResult{}
	}
}

// releaseWaiters scans the waiters of parentTxID, decrementing each
// child's pendingParents set. Children whose set becomes empty are
// returned as released msgs (caller appends to pendingMsgs).
func releaseWaiters(
	parentTxID string,
	waiters map[string]map[string]struct{},
	pendingParents map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
) []propagationMsg {
	children, ok := waiters[parentTxID]
	if !ok {
		return nil
	}
	delete(waiters, parentTxID)

	var released []propagationMsg
	for child := range children {
		parents := pendingParents[child]
		delete(parents, parentTxID)
		if len(parents) > 0 {
			continue
		}
		delete(pendingParents, child)
		if msg, ok := heldMsgs[child]; ok {
			delete(heldMsgs, child)
			released = append(released, msg)
		}
	}
	return released
}

// cascadeReject walks the dep graph from rejectedTxID forward and
// returns every descendant that should be terminally rejected. The
// reason carried in from the originally-rejected ancestor is propagated
// to every cascaded descendant so the eventual REJECTED row's
// ExtraInfo records the actual cause, not a generic "parent rejected"
// string.
func cascadeReject(
	rejectedTxID string,
	reason string,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	pendingParents map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
) []cascadedRejection {
	var cascaded []cascadedRejection
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
			delete(pendingParents, child)
			delete(heldMsgs, child)
			delete(inFlight, child)
			cascaded = append(cascaded, cascadedRejection{txid: child, reason: reason})
			queue = append(queue, child)
		}
	}
	return cascaded
}

// dispatcherChannelBuffer sizes the dispatcher's input channels. The
// reply protocol means handleMessage and applyTerminalStatuses block
// on a goroutine round-trip per call, but the buffer absorbs short
// bursts so a momentarily slow dispatcher (during a large cascade
// walk, say) doesn't immediately stall the consumer or broadcast
// workers.
const dispatcherChannelBuffer = 256

// admitToDispatcher is the consumer-side helper. Sends the tx,
// waits for the dispatcher's verdict, returns it.
func (p *Propagator) admitToDispatcher(msg propagationMsg) admitResult {
	reply := make(chan admitResult, 1)
	p.admitCh <- admitRequest{msg: msg, reply: reply}
	return <-reply
}

// notifyTerminalToDispatcher is the post-broadcast helper. Tells the
// dispatcher the txid reached terminal status, returns the cascaded
// descendants (caller writes REJECTED rows for them).
func (p *Propagator) notifyTerminalToDispatcher(txid string, status models.Status, reason string) terminalResult {
	reply := make(chan terminalResult, 1)
	p.terminalCh <- terminalEvent{txid: txid, status: status, reason: reason, reply: reply}
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
