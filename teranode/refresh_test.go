package teranode

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// fakeSource is a minimal EndpointSource that returns whatever set the test
// configures, with an atomic call counter so tests can wait for the refresh
// goroutine to fire without sleeping speculatively.
type fakeSource struct {
	urls  atomic.Value // []string
	calls atomic.Int32
	err   error
}

func (f *fakeSource) ListEndpointURLs(_ context.Context) ([]string, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	v, _ := f.urls.Load().([]string)
	return v, nil
}

func TestStart_SyncFirstRefreshSeedsEndpoints(t *testing.T) {
	src := &fakeSource{}
	src.urls.Store([]string{"https://a.example", "https://b.example"})

	c := NewClient(nil, "", HealthConfig{
		Source:          src,
		RefreshInterval: time.Hour, // we only care about the synchronous first refresh
		Logger:          zap.NewNop(),
	})
	defer c.Close()

	// Before Start, the in-memory list is empty.
	if eps := c.GetEndpoints(); len(eps) != 0 {
		t.Fatalf("expected no endpoints pre-Start, got %v", eps)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	eps := c.GetEndpoints()
	if len(eps) != 2 {
		t.Fatalf("expected 2 endpoints after Start sync refresh, got %v", eps)
	}
}

func TestRefreshLoop_AddsLaterDiscoveries(t *testing.T) {
	src := &fakeSource{}
	src.urls.Store([]string{"https://a.example"})

	c := NewClient(nil, "", HealthConfig{
		Source:          src,
		RefreshInterval: 10 * time.Millisecond,
		Logger:          zap.NewNop(),
	})
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	if eps := c.GetEndpoints(); len(eps) != 1 {
		t.Fatalf("expected 1 endpoint after first refresh, got %v", eps)
	}

	// Mutate the source — the refresh loop should pick up the new URL.
	src.urls.Store([]string{"https://a.example", "https://b.example"})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(c.GetEndpoints()) == 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("refresh loop did not pick up new URL within deadline; eps=%v", c.GetEndpoints())
}

func TestRefreshLoop_SourceErrorDoesNotPanic(t *testing.T) {
	src := &fakeSource{err: errors.New("store down")}

	c := NewClient([]string{"https://seed.example"}, "", HealthConfig{
		Source:          src,
		RefreshInterval: 5 * time.Millisecond,
		Logger:          zap.NewNop(),
	})
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// Wait for at least a couple of refresh ticks beyond the synchronous first one.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if src.calls.Load() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The seed URL should still be there — the failed refresh must not have
	// disrupted the in-memory list.
	eps := c.GetEndpoints()
	if len(eps) != 1 || eps[0] != "https://seed.example" {
		t.Fatalf("seed endpoint disrupted by failing refresh: %v", eps)
	}
}

func TestRefreshLoop_NoSourceMeansNoLoop(t *testing.T) {
	c := NewClient([]string{"https://a.example"}, "", HealthConfig{
		RefreshInterval: time.Millisecond,
		Logger:          zap.NewNop(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	if c.refreshDone != nil {
		t.Fatal("refresh loop started despite Source=nil")
	}
	c.Close() // must not block on a nil refreshDone
}
