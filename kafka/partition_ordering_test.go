package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// The entire #295 ordering model rests on two properties that nothing in the
// repo currently asserts:
//
//  1. messages on one partition are delivered to their claim in publish
//     order, and
//  2. the producer routes by hashing the message key.
//
// Family partition keys are only meaningful if both hold. If (2) regressed to
// round-robin or random, every family would scatter and the dep-aware
// dispatcher would never see a parent before its child — with no test going
// red. If (1) regressed, per-partition order would not imply parent-precedes-
// child even within a co-located family.
//
// TestMemoryBrokerPartitions_OneClaimPerPartition's comment says "per-key
// order must survive", but its recorder keeps only string(m.Key) and drops
// the payload, so ordering is never actually checked.

// TestMemoryBrokerPartitions_PreservesPerPartitionValueOrder pins property
// (1) for the in-memory broker that smoke tests run against: for each key,
// the claim must observe that key's payloads in the order they were sent.
func TestMemoryBrokerPartitions_PreservesPerPartitionValueOrder(t *testing.T) {
	const topic = "part-order"
	const partitions = 3
	const keys = 10
	const perKey = 8

	b := NewMemoryBrokerWithPartitions(1000, 0, map[string]int{topic: partitions})
	defer func() { _ = b.Close() }()

	sub, err := b.Subscribe("g", []string{topic}, StartOldest)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var mu sync.Mutex
	seen := make(map[string][]byte) // key → payload sequence in receive order
	var total atomic.Int32

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sub.Consume(ctx, func(c Claim) error {
			for {
				select {
				case m := <-c.Messages():
					mu.Lock()
					seen[string(m.Key)] = append(seen[string(m.Key)], m.Value[0])
					mu.Unlock()
					total.Add(1)
				case <-c.Context().Done():
					return nil
				}
			}
		})
	}()

	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		for j := 0; j < perKey; j++ {
			if err := b.Send(context.Background(), topic, key, []byte{byte(j)}); err != nil {
				t.Fatalf("send: %v", err)
			}
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && total.Load() < int32(keys*perKey) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if got := int(total.Load()); got != keys*perKey {
		t.Fatalf("received %d messages, want %d", got, keys*perKey)
	}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		got := seen[key]
		if len(got) != perKey {
			t.Errorf("key %q: got %d messages, want %d", key, len(got), perKey)
			continue
		}
		for j := 0; j < perKey; j++ {
			if got[j] != byte(j) {
				t.Fatalf("key %q delivered out of order: got %v, want ascending 0..%d.\n"+
					"Per-partition order is the foundation of family keying — without it, "+
					"co-locating a family on one partition does not imply a parent's admit "+
					"reaches the dispatcher before its child's.", key, got, perKey-1)
			}
		}
	}
}

// TestNewSyncProducerConfig_RoutesByKeyHash pins property (2), and does it
// behaviorally rather than by asserting a type name.
//
// It builds the partitioner the shipped producer config actually uses and
// checks it agrees with the memory broker's partitionForKey across a key
// corpus and every partition count that matters. That covers three
// regressions at once:
//
//   - swapping Producer.Partitioner for round-robin/random (family keying
//     silently stops working; TestNewSyncProducerConfig_OrderingInvariants
//     does not look at this field)
//   - a sarama upgrade changing the default partitioner or its arithmetic
//     (referenceAbs / hashUnsigned flip mod-then-abs to abs-then-mod)
//   - the memory broker drifting from production placement, which would make
//     every smoke-test family observation meaningless
//
// The existing memory-broker test compares partitionForKey against a local
// re-implementation of the same arithmetic, so it cannot catch any of these.
func TestNewSyncProducerConfig_RoutesByKeyHash(t *testing.T) {
	const topic = "arcade.propagation"

	cfg := newSyncProducerConfig()
	if cfg.Producer.Partitioner == nil {
		t.Fatal("Producer.Partitioner is nil")
	}
	partitioner := cfg.Producer.Partitioner(topic)

	keys := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"0000000000000000000000000000000000000000000000000000000000000000",
		"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		"21e64e24829013f04db6614fc3c865b92ada9e31d58dcb8a6a169023fa7d27bd",
	}
	for i := 0; i < 64; i++ {
		keys = append(keys, fmt.Sprintf("family-%03d", i))
	}

	for _, n := range []int32{1, 2, 3, 4, 8, 16} {
		for _, key := range keys {
			msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key)}
			want, err := partitioner.Partition(msg, n)
			if err != nil {
				t.Fatalf("sarama Partition(%q, %d): %v", key, n, err)
			}
			if want < 0 || want >= n {
				t.Fatalf("sarama returned partition %d for %q with n=%d — out of range; "+
					"the configured partitioner is not a keyed hash partitioner", want, key, n)
			}
			if got := partitionForKey(key, int(n)); got != want {
				t.Fatalf("partition mismatch for key %q with %d partitions: memory broker = %d, "+
					"sarama = %d.\nA family placed by the memory broker in smoke tests would land "+
					"on a different partition in production.", key, n, got, want)
			}
		}
	}

	// A keyed hash partitioner must also be deterministic across instances —
	// the same family key produced by two different pods has to agree.
	other := cfg.Producer.Partitioner(topic)
	msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder("alpha")}
	a, err := partitioner.Partition(msg, 8)
	if err != nil {
		t.Fatalf("Partition: %v", err)
	}
	b, err := other.Partition(msg, 8)
	if err != nil {
		t.Fatalf("Partition: %v", err)
	}
	if a != b {
		t.Fatalf("partitioner is not deterministic across instances: %d vs %d — "+
			"two arcade pods would place the same family on different partitions", a, b)
	}
}
