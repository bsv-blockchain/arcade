package propagation

import (
	"context"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/models"
)

// The dispatcher is a single-goroutine engine that owns the dep-index
// state (in-flight set, waiters map, pendingParents map, held messages).
// All dep-aware decisions happen here, on this one goroutine, with no
// locks: state mutations are serialized by the select loop.
//
// External components communicate via two channels:
//
//   - admitCh: handleMessage sends a propagationMsg here and waits for
//     a boolean reply telling it whether to admit (append to
//     pendingMsgs) or hold (the dispatcher has registered the tx as a
//     waiter on its in-flight parents).
//
//   - terminalCh: applyTerminalStatuses sends a terminalEvent here per
//     terminalized txid and waits for a terminalResult that describes
//     which waiters were released and which descendants were cascade-
//     rejected. The caller is responsible for taking action on those
//     lists (re-entering released msgs into pendingMsgs, writing
//     REJECTED rows for cascaded descendants).
//
// The Kafka consumer is single-threaded by partition assignment, so
// admitCh is driven from one goroutine. processBatch goroutines drive
// terminalCh in parallel (one per concurrent batch), but each waits
// for its own reply before moving on. The dispatcher serializes them
// via the select loop — no lock needed.

// admitRequest is the protocol between handleMessage and the
// dispatcher goroutine. The dispatcher replies true on admit (caller
// appends to pendingMsgs) or false on hold (caller does nothing — the
// dispatcher has registered the tx in heldMsgs/waiters/pendingParents).
type admitRequest struct {
	msg   propagationMsg
	reply chan bool
}

// terminalEvent is the protocol between applyTerminalStatuses and the
// dispatcher goroutine. status is the just-applied terminal status of
// txid; reason carries the rejection reason for the REJECTED case
// (passed through to cascade-rejected descendants' ExtraInfo).
type terminalEvent struct {
	txid   string
	status models.Status
	reason string
	reply  chan terminalResult
}

// terminalResult is what the dispatcher returns to the caller of
// applyTerminalStatuses. released lists waiters whose parent set is
// now empty (caller appends them to pendingMsgs). cascaded lists
// descendants that have been transitively rejected because of a
// REJECTED ancestor (caller writes terminal REJECTED rows for them
// and emits a bulk publish event).
type terminalResult struct {
	released []propagationMsg
	cascaded []string
}

// runDispatcher is the dispatcher goroutine's main loop. Started by
// Propagator.Start and runs for the lifetime of the propagation
// service. All dep-index state declared inside the function body —
// nothing leaks out, so nothing else can mutate it without going
// through the channels.
func (p *Propagator) runDispatcher(ctx context.Context) {
	// inFlight is the set of txids the dispatcher knows about and has
	// not yet seen a terminal status for. A tx is in inFlight whether
	// it's been admitted to pendingMsgs OR held as a waiter — both
	// states count as "Arcade is aware of this tx, work is pending."
	// Descendants check inFlight to decide whether to wait.
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

	for {
		select {
		case <-ctx.Done():
			return

		case req := <-p.admitCh:
			handleAdmit(req, inFlight, waiters, pendingParents, heldMsgs)

		case ev := <-p.terminalCh:
			handleTerminal(ev, inFlight, waiters, pendingParents, heldMsgs)
		}
	}
}

// handleAdmit decides whether a new tx is admitted to pendingMsgs or
// held as a waiter on in-flight parents. The tx is added to inFlight
// either way — descendants need to see it as a parent regardless.
func handleAdmit(
	req admitRequest,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	pendingParents map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
) {
	msg := req.msg
	inFlight[msg.TXID] = struct{}{}

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

	if len(pending) == 0 {
		req.reply <- true // admit
		return
	}

	// Hold the tx as a waiter on every in-flight parent.
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
	req.reply <- false // hold
}

// handleTerminal processes a terminal status flip for txid. ACCEPTED
// releases waiters whose parent set becomes empty as a result;
// REJECTED recursively cascades rejection through every descendant.
// In both cases txid is removed from inFlight. Returns the release/
// cascade outcome via the event's reply channel so the caller can
// take downstream action (pendingMsgs append for releases, store
// write + publish for cascades).
func handleTerminal(
	ev terminalEvent,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	pendingParents map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
) {
	delete(inFlight, ev.txid)

	var result terminalResult

	switch ev.status {
	case models.StatusAcceptedByNetwork:
		result.released = releaseWaiters(ev.txid, waiters, pendingParents, heldMsgs)

	case models.StatusRejected:
		result.cascaded = cascadeReject(ev.txid, inFlight, waiters, pendingParents, heldMsgs)

	default:
		// Intermediate statuses (SEEN_ON_NETWORK, etc.) aren't part
		// of the terminal contract; the caller shouldn't be sending
		// them here. No state change, empty result.
	}

	ev.reply <- result
}

// releaseWaiters scans the waiters of parentTxID, decrementing each
// child's pendingParents set. Children whose set becomes empty are
// returned as released msgs (caller appends them to pendingMsgs).
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
// returns every descendant txid that should be terminally rejected
// as a result. Drops cascaded entries from all dep-index maps
// (inFlight, heldMsgs, pendingParents, waiters). Caller writes
// REJECTED rows + emits a bulk publish for the returned list.
func cascadeReject(
	rejectedTxID string,
	inFlight map[string]struct{},
	waiters map[string]map[string]struct{},
	pendingParents map[string]map[string]struct{},
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
			delete(pendingParents, child)
			delete(heldMsgs, child)
			delete(inFlight, child)
			cascaded = append(cascaded, child)
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

// admitToDispatcher is the consumer-side helper: send the tx, wait
// for the dispatcher's admit/hold decision, return it. Keeps the
// reply-channel protocol out of handleMessage's body.
func (p *Propagator) admitToDispatcher(msg propagationMsg) bool {
	reply := make(chan bool, 1)
	p.admitCh <- admitRequest{msg: msg, reply: reply}
	return <-reply
}

// notifyTerminalToDispatcher is the post-broadcast helper: tell the
// dispatcher the txid reached terminal status, get back the lists of
// released waiters and cascaded descendants. Caller acts on both.
func (p *Propagator) notifyTerminalToDispatcher(txid string, status models.Status, reason string) terminalResult {
	reply := make(chan terminalResult, 1)
	p.terminalCh <- terminalEvent{txid: txid, status: status, reason: reason, reply: reply}
	return <-reply
}

// Suppress unused-warning placeholder. The logger field is reserved
// for diagnostics added in a follow-up; keeping the reference here
// avoids churn when those land.
var _ = zap.NewNop
