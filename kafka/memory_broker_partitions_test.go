package kafka

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Partitioned memory broker (#295): standalone/smoke deployments need the
// same key→partition→per-claim-dispatcher topology as production Kafka so
// the multi-partition propagation path is testable without a real broker.
// Routing must match Sarama's default hash partitioner (FNV-1a 32-bit,
// mod partition count, abs) so a smoke test observes the same family
// placement a production topic would produce.

// saramaHashPartition mirrors sarama.NewHashPartitioner's arithmetic.
func saramaHashPartition(key string, n int32) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	p := int32(h.Sum32()) % n
	if p < 0 {
		p = -p
	}
	return p
}

// TestMemoryBrokerPartitions_KeyRoutingMatchesSaramaHash pins that a keyed
// publish lands on the Sarama-compatible partition, so same-key messages
// (a #295 dependency family) always share a partition.
func TestMemoryBrokerPartitions_KeyRoutingMatchesSaramaHash(t *testing.T) {
	const topic = "part-routing"
	const n = 4
	b := NewMemoryBrokerWithPartitions(100, 0, map[string]int{topic: n})
	defer b.Close()

	sub, err := b.Subscribe("g", []string{topic}, StartOldest)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var mu sync.Mutex
	got := map[string]int32{} // key → partition observed
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sub.Consume(ctx, func(c Claim) error {
			for {
				select {
				case m := <-c.Messages():
					mu.Lock()
					got[string(m.Key)] = m.Partition
					mu.Unlock()
				case <-c.Context().Done():
					return nil
				}
			}
		})
	}()

	keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "alpha"}
	for _, k := range keys {
		if err := b.Send(context.Background(), topic, k, []byte("v")); err != nil {
			t.Fatalf("send %s: %v", k, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	for k, p := range got {
		if want := saramaHashPartition(k, n); p != want {
			t.Errorf("key %q landed on partition %d, want sarama-compatible %d", k, p, want)
		}
	}
}

// TestMemoryBrokerPartitions_OneClaimPerPartition pins the consumer shape:
// Consume invokes the handler once per partition, each claim seeing only
// its own partition's messages in publish order — exactly what production
// Sarama does with per-partition ConsumeClaim goroutines, and what gives
// each partition its own dep-aware dispatcher.
func TestMemoryBrokerPartitions_OneClaimPerPartition(t *testing.T) {
	const topic = "part-claims"
	const n = 3
	b := NewMemoryBrokerWithPartitions(100, 0, map[string]int{topic: n})
	defer b.Close()

	sub, err := b.Subscribe("g", []string{topic}, StartOldest)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var claims atomic.Int32
	var mu sync.Mutex
	perClaim := make(map[int][]string) // claim serial → keys in receive order

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sub.Consume(ctx, func(c Claim) error {
			serial := int(claims.Add(1))
			for {
				select {
				case m := <-c.Messages():
					mu.Lock()
					perClaim[serial] = append(perClaim[serial], string(m.Key))
					mu.Unlock()
				case <-c.Context().Done():
					return nil
				}
			}
		})
	}()

	// 30 sends across 10 distinct keys; per-key order must survive.
	var wantTotal int
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		for j := 0; j < 3; j++ {
			if err := b.Send(context.Background(), topic, key, []byte{byte(j)}); err != nil {
				t.Fatalf("send: %v", err)
			}
			wantTotal++
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		total := 0
		for _, ks := range perClaim {
			total += len(ks)
		}
		mu.Unlock()
		if total == wantTotal {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if got := claims.Load(); got != n {
		t.Fatalf("handler invoked for %d claims, want one per partition (%d)", got, n)
	}
	// Every key's messages must be confined to a single claim.
	mu.Lock()
	defer mu.Unlock()
	keyToClaim := map[string]int{}
	for serial, ks := range perClaim {
		for _, k := range ks {
			if prev, ok := keyToClaim[k]; ok && prev != serial {
				t.Fatalf("key %q seen on claims %d and %d — same key must map to one partition", k, prev, serial)
			}
			keyToClaim[k] = serial
		}
	}
}

// TestMemoryBrokerPartitions_PartitionCount pins the startup check's view:
// configured topics report their count, everything else reports 1.
func TestMemoryBrokerPartitions_PartitionCount(t *testing.T) {
	b := NewMemoryBrokerWithPartitions(10, 0, map[string]int{"wide": 8})
	defer b.Close()
	if n, err := b.PartitionCount("wide"); err != nil || n != 8 {
		t.Errorf("PartitionCount(wide) = %d, %v; want 8, nil", n, err)
	}
	if n, err := b.PartitionCount("other"); err != nil || n != 1 {
		t.Errorf("PartitionCount(other) = %d, %v; want 1, nil", n, err)
	}
}

// TestMemoryBrokerPartitions_DefaultIsSingleClaim pins backward
// compatibility: a broker built without a partition map behaves exactly as
// before — one claim per subscribed topic.
func TestMemoryBrokerPartitions_DefaultIsSingleClaim(t *testing.T) {
	b := NewMemoryBroker(10)
	defer b.Close()

	sub, err := b.Subscribe("g", []string{"plain"}, StartOldest)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	var claims atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sub.Consume(ctx, func(c Claim) error {
			claims.Add(1)
			<-c.Context().Done()
			return nil
		})
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	if got := claims.Load(); got != 1 {
		t.Fatalf("expected exactly 1 claim for an unpartitioned topic, got %d", got)
	}
}
