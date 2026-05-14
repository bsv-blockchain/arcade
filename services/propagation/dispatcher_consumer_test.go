package propagation

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/kafka"
)

// staticOffsetReader returns whatever the test sets, ignoring any heap
// state. Lets the consumer's commit logic be exercised independently of
// the dispatcher.
type staticOffsetReader struct {
	mu     sync.Mutex
	cursor int64
	hasVal bool
	empty  bool
}

func (s *staticOffsetReader) set(cursor int64, hasVal bool, empty bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor = cursor
	s.hasVal = hasVal
	s.empty = empty
}

func (s *staticOffsetReader) LowestUnfinished() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, s.hasVal
}

func (s *staticOffsetReader) Empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.empty
}

// TestDispatcherConsumer_DeferredCommit verifies that messages are NOT
// MarkMessage'd until the offset tracker reports them as terminalized.
// Without that, a crash would lose in-flight txs whose offsets had been
// committed but whose work hadn't completed.
func TestDispatcherConsumer_DeferredCommit(t *testing.T) {
	broker := kafka.NewMemoryBroker(0)
	t.Cleanup(func() { _ = broker.Close() })

	const topic = "test-propagation-defer"
	in := make(chan dispatcherMsg, 8)
	flips := make(chan statusFlip)
	out := make(chan []*inFlightEntry, 4)

	// Real dispatcher so the Dispatcher reference can be wired, but we
	// don't actually start it for this test — only the consumer's
	// commit semantics matter here.
	d := NewDispatcher(zap.NewNop(), in, out, flips, newOffsetTracker(),
		100, 100, 3, 100, 50*time.Millisecond)
	offsets := &staticOffsetReader{cursor: 0, hasVal: true} // nothing committed yet

	c, err := newDispatcherConsumer(dispatcherConsumerConfig{
		Broker:       broker,
		GroupID:      "test-defer",
		Topic:        topic,
		Dispatcher:   d,
		Offsets:      offsets,
		IncomingMsgs: in,
		CommitTicker: 20 * time.Millisecond,
		Logger:       zap.NewNop(),
	})
	if err != nil {
		t.Fatalf("newDispatcherConsumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runErr := make(chan error, 1)
	go func() {
		runErr <- c.Run(ctx)
	}()

	select {
	case <-c.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("consumer never became ready")
	}

	// Publish 3 messages.
	producer := kafka.NewProducer(broker)
	for i := 0; i < 3; i++ {
		envelope := dispatcherMsg{TXID: txidForIndex(i), RawTx: []byte{byte(i)}}
		if err := producer.Send(topic, envelope.TXID, envelope); err != nil {
			t.Fatalf("producer.Send: %v", err)
		}
	}

	// Drain incoming channel — confirms the consumer is delivering.
	for i := 0; i < 3; i++ {
		select {
		case got := <-in:
			if got.TXID != txidForIndex(i) {
				t.Errorf("position %d: got txid %q, want %q", i, got.TXID, txidForIndex(i))
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("position %d: timed out waiting for dispatcher msg", i)
		}
	}

	// With LowestUnfinished=0, none of the offsets (0,1,2) should be
	// below the cursor — so nothing should be marked. Give the commit
	// loop several ticks to be sure.
	time.Sleep(80 * time.Millisecond)
	c.pendingMu.Lock()
	pendingBefore := len(c.pending)
	c.pendingMu.Unlock()
	if pendingBefore != 3 {
		t.Errorf("with cursor=0, expected 3 pending, got %d", pendingBefore)
	}

	// Advance cursor to 2 — offsets 0 and 1 should now be committed,
	// offset 2 remains.
	offsets.set(2, true, false)
	waitForPendingSize(t, c, 1, 500*time.Millisecond)

	// Advance to past everything by signaling empty.
	offsets.set(0, false, true)
	waitForPendingSize(t, c, 0, 500*time.Millisecond)

	cancel()
	select {
	case err := <-runErr:
		// Memory-broker subscriptions return nil on graceful close.
		if err != nil {
			t.Logf("consumer Run returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not exit after cancel")
	}
}

func waitForPendingSize(t *testing.T, c *dispatcherConsumer, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.pendingMu.Lock()
		got := len(c.pending)
		c.pendingMu.Unlock()
		if got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.pendingMu.Lock()
	got := len(c.pending)
	c.pendingMu.Unlock()
	t.Fatalf("pending size never reached %d (got %d) within %v", want, got, timeout)
}

func txidForIndex(i int) string {
	// 64 hex chars to mimic a real txid length, varying by index. Caller
	// passes small non-negative i (0-9 in tests), so the rune conversion
	// is in-range — gosec G115 doesn't see that statically.
	digit := rune('0') + rune(i) //nolint:gosec // bounded test input
	return "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbe0" + string(digit)
}
