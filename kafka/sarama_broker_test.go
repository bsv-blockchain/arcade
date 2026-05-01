package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// fakeSyncProducer satisfies sarama.SyncProducer so the saramaBroker can be
// constructed without a real Kafka cluster. The async-channel-drainer fix
// only exercises the async producer side, but Close() drives both producers.
type fakeSyncProducer struct {
	closed atomic.Bool
}

func (f *fakeSyncProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, nil
}

func (f *fakeSyncProducer) SendMessages([]*sarama.ProducerMessage) error { return nil }

func (f *fakeSyncProducer) Close() error { f.closed.Store(true); return nil }

func (f *fakeSyncProducer) AbortTxn() error { return nil }

func (f *fakeSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func (f *fakeSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (f *fakeSyncProducer) BeginTxn() error { return nil }

func (f *fakeSyncProducer) CommitTxn() error { return nil }

func (f *fakeSyncProducer) IsTransactional() bool { return false }

func (f *fakeSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }

func (f *fakeSyncProducer) TxnAddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

// newAsyncProducerMock builds a Sarama mock async producer with the same
// Return.Successes/Return.Errors flags the production broker uses, so the
// drainer goroutines see the same channel behavior they would in a real
// deployment.
func newAsyncProducerMock(t *testing.T) *mocks.AsyncProducer {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return mocks.NewAsyncProducer(t, cfg)
}

// TestSaramaBroker_AsyncDrainer_DoesNotBlockOnSuccesses sends N messages
// through SendAsync. Without a Successes drainer the mock's success channel
// (capacity = ChannelBufferSize, default 256) would fill and the next send
// would block on Input(). N is chosen well above that buffer to make the
// regression mode unambiguous.
func TestSaramaBroker_AsyncDrainer_DoesNotBlockOnSuccesses(t *testing.T) {
	mp := newAsyncProducerMock(t)
	const n = 1024
	for range n {
		mp.ExpectInputAndSucceed()
	}

	b := newSaramaBrokerFromProducers(&fakeSyncProducer{}, mp, nil, "", zaptest.NewLogger(t))

	done := make(chan error, 1)
	go func() {
		ctx := context.Background()
		for i := 0; i < n; i++ {
			if err := b.SendAsync(ctx, "tx.validated", "k", []byte("v")); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendAsync returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendAsync blocked — Successes channel was not drained")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

// TestSaramaBroker_AsyncDrainer_HandlesErrors verifies that messages routed
// to the Errors channel are drained (not blocking the producer) and logged
// without panicking. The mock fails every input, so without an Errors
// drainer the channel would fill and SendAsync would deadlock.
func TestSaramaBroker_AsyncDrainer_HandlesErrors(t *testing.T) {
	mp := newAsyncProducerMock(t)
	const n = 512
	produceErr := errors.New("simulated produce failure")
	for range n {
		mp.ExpectInputAndFail(produceErr)
	}

	b := newSaramaBrokerFromProducers(&fakeSyncProducer{}, mp, nil, "", zaptest.NewLogger(t))

	done := make(chan error, 1)
	go func() {
		ctx := context.Background()
		for i := 0; i < n; i++ {
			if err := b.SendAsync(ctx, "tx.validated", "k", []byte("v")); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendAsync returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendAsync blocked — Errors channel was not drained")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

// TestSaramaBroker_Close_WaitsForDrainers asserts that Close() blocks until
// the drainer goroutines exit. After Close returns, no goroutine should
// still be reading from Successes/Errors, which we approximate by checking
// that the broker's WaitGroup has zero counter (drainersWG.Wait() returns
// immediately on a second call).
func TestSaramaBroker_Close_WaitsForDrainers(t *testing.T) {
	mp := newAsyncProducerMock(t)
	mp.ExpectInputAndSucceed()

	b := newSaramaBrokerFromProducers(&fakeSyncProducer{}, mp, nil, "", zap.NewNop())

	if err := b.SendAsync(context.Background(), "t", "k", []byte("v")); err != nil {
		t.Fatalf("SendAsync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A second Wait must be a no-op; if it blocks the drainers leaked.
	waitDone := make(chan struct{})
	go func() {
		b.drainersWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("drainer goroutines leaked past Close()")
	}
}
