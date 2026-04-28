package aerospike

import (
	"context"
	"testing"
	"time"
)

func TestRemaining(t *testing.T) {
	const def = 5 * time.Second

	t.Run("no deadline returns default", func(t *testing.T) {
		if got := remaining(context.Background(), def); got != def {
			t.Fatalf("want %v, got %v", def, got)
		}
	})

	t.Run("cancelled ctx returns zero", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := remaining(ctx, def); got != 0 {
			t.Fatalf("want 0 for cancelled ctx, got %v", got)
		}
	})

	t.Run("expired deadline returns zero", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if got := remaining(ctx, def); got != 0 {
			t.Fatalf("want 0 for expired deadline, got %v", got)
		}
	})

	t.Run("deadline shorter than default is used", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		got := remaining(ctx, def)
		if got <= 0 || got > 100*time.Millisecond {
			t.Fatalf("want (0, 100ms], got %v", got)
		}
	})

	t.Run("deadline longer than default is capped", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		if got := remaining(ctx, def); got != def {
			t.Fatalf("want %v, got %v", def, got)
		}
	})
}

func TestPolicyBuilders(t *testing.T) {
	s := &Store{
		queryTimeout:  8 * time.Second,
		opTimeout:     3 * time.Second,
		socketTimeout: 5 * time.Second,
	}

	t.Run("queryPolicy cancelled ctx sets zero TotalTimeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p := s.queryPolicy(ctx)
		if p.TotalTimeout != 0 {
			t.Fatalf("want 0 TotalTimeout for cancelled ctx, got %v", p.TotalTimeout)
		}
		if p.SocketTimeout != s.socketTimeout {
			t.Fatalf("want SocketTimeout=%v, got %v", s.socketTimeout, p.SocketTimeout)
		}
		if p.MaxRetries != 1 {
			t.Fatalf("want MaxRetries=1, got %d", p.MaxRetries)
		}
	})

	t.Run("readPolicy applies op timeout", func(t *testing.T) {
		p := s.readPolicy(context.Background())
		if p.TotalTimeout != s.opTimeout {
			t.Fatalf("want %v, got %v", s.opTimeout, p.TotalTimeout)
		}
	})

	t.Run("writePolicy applies op timeout and is mutable", func(t *testing.T) {
		p := s.writePolicy(context.Background())
		if p.TotalTimeout != s.opTimeout {
			t.Fatalf("want %v, got %v", s.opTimeout, p.TotalTimeout)
		}
		// Callers mutate this (e.g. CREATE_ONLY, generation) — verify it's writable.
		p.MaxRetries = 0
		if p.MaxRetries != 0 {
			t.Fatal("policy struct should be mutable by caller")
		}
	})

	t.Run("batchPolicy uses op timeout and low retries", func(t *testing.T) {
		p := s.batchPolicy(context.Background())
		if p.TotalTimeout != s.opTimeout {
			t.Fatalf("want %v, got %v", s.opTimeout, p.TotalTimeout)
		}
		if p.MaxRetries != 1 {
			t.Fatalf("want MaxRetries=1, got %d", p.MaxRetries)
		}
	})
}
