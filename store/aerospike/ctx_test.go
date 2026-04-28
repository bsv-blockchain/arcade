//go:build integration

package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/config"
)

// These tests require a running Aerospike instance on localhost:3200
// (matches the docker-compose.yml port mapping). Run with:
//
//	go test -tags=integration ./store/...

func integrationStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(config.Aero{
		Hosts:           []string{"localhost:3200"},
		Namespace:       "arcade",
		BatchSize:       100,
		PoolSize:        64,
		QueryTimeoutMs:  8000,
		OpTimeoutMs:     3000,
		SocketTimeoutMs: 5000,
	})
	if err != nil {
		t.Skipf("aerospike unavailable on localhost:3200: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGetStumpsByBlockHash_CancelledCtx(t *testing.T) {
	s := integrationStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := s.GetStumpsByBlockHash(ctx, "any-block-hash")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("want error for cancelled ctx, got nil")
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("cancelled ctx should short-circuit; took %v", elapsed)
	}
}

func TestGetStumpsByBlockHash_ShortDeadline(t *testing.T) {
	s := integrationStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _ = s.GetStumpsByBlockHash(ctx, "any-block-hash")
	elapsed := time.Since(start)

	// Should return within ~100ms even if the query would naturally take longer.
	// We don't assert on err value because a zero-result query is not an error
	// and the ctx may or may not fire before the query naturally completes.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("short deadline should bound query time; took %v", elapsed)
	}
}

func TestGetStumpsByBlockHash_HappyPath(t *testing.T) {
	s := integrationStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Empty block hash should just return empty slice, no error.
	stumps, err := s.GetStumpsByBlockHash(ctx, "nonexistent-block-for-integration-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stumps) != 0 {
		t.Fatalf("expected 0 stumps, got %d", len(stumps))
	}
}
