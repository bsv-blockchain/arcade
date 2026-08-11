package propagation

import (
	"github.com/bsv-blockchain/arcade/kafka"
)

// markQueue holds consumed-but-unmarked Kafka messages awaiting the
// commit watermark. It replaces the pendingMarks map whose advanceMarks
// walked EVERY entry once per terminal event — O(terminals × in-flight
// offsets) on the dispatcher's serial path, and the prime suspect for
// the ~10x gap between the dependency-aware-dispatch plan's throughput
// estimate and the ~1.6-1.9k TPS measured in issue #295.
//
// The dispatcher pushes messages in claim-delivery order, and Sarama
// delivers a partition claim in offset order, so the queue is
// monotonically increasing: advancing pops from the front while the
// head is strictly below LowestUnfinished, touching only newly markable
// entries — amortized O(1) per message no matter how often it runs.
// A monotonicity violation (which claim delivery can't produce) could
// only DELAY marks behind the out-of-order entry, never over-commit:
// every marked offset is individually checked against the watermark.
//
// Owned exclusively by the runDispatcher goroutine; no locking.
type markQueue struct {
	msgs []*kafka.Message
	head int
}

// push appends a consumed message awaiting its offset's release.
func (q *markQueue) push(m *kafka.Message) {
	q.msgs = append(q.msgs, m)
}

// pending returns the number of messages still awaiting marking.
func (q *markQueue) pending() int {
	return len(q.msgs) - q.head
}

// advance marks every queued message strictly below the tracker's
// lowest unfinished offset (all of them when nothing is unfinished).
// No-op with a nil claim (dispatcher test mode). Safe under claim
// revocation: a still-in-flight offset is never marked, and marking an
// already-dead session is broker-side bookkeeping only.
func (q *markQueue) advance(claim kafka.Claim, tracker *offsetTracker) {
	if claim == nil {
		return
	}
	lowest, hasUnfinished := tracker.LowestUnfinished()
	for q.head < len(q.msgs) {
		msg := q.msgs[q.head]
		if hasUnfinished && msg.Offset >= lowest {
			break
		}
		claim.MarkMessage(msg)
		q.msgs[q.head] = nil // release for GC
		q.head++
	}
	// Compact once the consumed prefix dominates so the backing slice
	// doesn't grow without bound across the claim's lifetime.
	if q.head > 0 && q.head >= len(q.msgs)/2 {
		q.msgs = append(q.msgs[:0], q.msgs[q.head:]...)
		q.head = 0
	}
}
