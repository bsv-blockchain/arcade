package teranode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecordFailure_TripsAfterThreshold(t *testing.T) {
	c := NewClient([]string{"https://a.example", "https://b.example"}, "", HealthConfig{FailureThreshold: 3})

	// Two sub-threshold failures keep the endpoint healthy.
	c.RecordFailure("https://a.example")
	c.RecordFailure("https://a.example")
	if healthy := c.GetHealthyEndpoints(); len(healthy) != 2 {
		t.Fatalf("expected both endpoints healthy after 2 failures, got %v", healthy)
	}

	// Third failure trips.
	c.RecordFailure("https://a.example")
	healthy := c.GetHealthyEndpoints()
	if !reflect.DeepEqual(healthy, []string{"https://b.example"}) {
		t.Fatalf("expected only b after trip, got %v", healthy)
	}
	// GetEndpoints still returns everything.
	if len(c.GetEndpoints()) != 2 {
		t.Fatalf("GetEndpoints should still return both, got %v", c.GetEndpoints())
	}
}

func TestRecordSuccess_ResetsCounter(t *testing.T) {
	c := NewClient([]string{"https://a.example"}, "", HealthConfig{FailureThreshold: 3})

	c.RecordFailure("https://a.example")
	c.RecordFailure("https://a.example")
	c.RecordSuccess("https://a.example")
	// After reset, three more failures should be required to trip.
	c.RecordFailure("https://a.example")
	c.RecordFailure("https://a.example")
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatalf("endpoint should still be healthy after reset + 2 failures")
	}
	c.RecordFailure("https://a.example")
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatalf("endpoint should trip after 3 post-reset failures")
	}
}

func TestRecordSuccess_RecoversUnhealthy(t *testing.T) {
	c := NewClient([]string{"https://a.example"}, "", HealthConfig{FailureThreshold: 2})
	c.RecordFailure("https://a.example")
	c.RecordFailure("https://a.example")
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("expected endpoint to be unhealthy")
	}
	c.RecordSuccess("https://a.example")
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatal("expected endpoint to recover to healthy")
	}
}

func TestRecordFailure_UnknownURL_NoOp(t *testing.T) {
	c := NewClient([]string{"https://a.example"}, "", HealthConfig{FailureThreshold: 1})
	// Repeatedly call RecordFailure for an unknown URL — should not create a
	// health entry or affect the registered endpoint.
	for i := 0; i < 10; i++ {
		c.RecordFailure("https://nonexistent.example")
	}
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatalf("unknown-URL failures must not affect registered endpoints")
	}
	if len(c.GetEndpoints()) != 1 {
		t.Fatalf("unknown-URL failures must not create new endpoints")
	}
}

func TestGetHealthyEndpoints_SnapshotIndependence(t *testing.T) {
	c := NewClient([]string{"https://a.example", "https://b.example"}, "", HealthConfig{FailureThreshold: 1})
	snap := c.GetHealthyEndpoints()
	c.RecordFailure("https://a.example") // trips a
	if len(snap) != 2 {
		t.Fatalf("previously-returned snapshot was mutated: %v", snap)
	}
}

func TestGetHealthyEndpoints_PreservesOrder(t *testing.T) {
	c := NewClient([]string{"https://a.example", "https://b.example", "https://c.example"}, "", HealthConfig{FailureThreshold: 2})
	// Trip b, leaving a and c healthy.
	c.RecordFailure("https://b.example")
	c.RecordFailure("https://b.example")
	got := c.GetHealthyEndpoints()
	want := []string{"https://a.example", "https://c.example"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestAddEndpoints_SeedsHealthyState(t *testing.T) {
	c := NewClient(nil, "", HealthConfig{FailureThreshold: 1})
	c.AddEndpoints([]string{"https://new.example"})
	if !reflect.DeepEqual(c.GetHealthyEndpoints(), []string{"https://new.example"}) {
		t.Fatalf("newly added endpoint should be healthy immediately")
	}
}

func TestAddEndpoints_Rediscover_PreservesHealthState(t *testing.T) {
	c := NewClient([]string{"https://a.example"}, "", HealthConfig{FailureThreshold: 2})
	c.RecordFailure("https://a.example")
	c.RecordFailure("https://a.example") // trip
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("expected endpoint to be unhealthy")
	}
	// Re-announcement is deduplicated — must NOT reset health state.
	added := c.AddEndpoints([]string{"https://a.example"})
	if added != 0 {
		t.Fatalf("expected 0 new endpoints, got %d", added)
	}
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("rediscovered unhealthy endpoint must stay unhealthy")
	}
}

// Concurrent RecordSuccess / RecordFailure + readers — -race must stay silent.
func TestHealthTracker_Concurrent(t *testing.T) {
	c := NewClient([]string{"https://a.example", "https://b.example"}, "", HealthConfig{FailureThreshold: 1000})

	const workers = 8
	const perWorker = 200
	var wg sync.WaitGroup
	wg.Add(workers * 3)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				c.RecordFailure("https://a.example")
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				c.RecordSuccess("https://a.example")
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				_ = c.GetHealthyEndpoints()
			}
		}()
	}
	wg.Wait()
	// No assertion on state — success is -race not tripping and no panic.
}

func TestProbe_Recovers_AfterReachable(t *testing.T) {
	var healthHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			healthHits.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewClient([]string{srv.URL}, "", HealthConfig{
		FailureThreshold: 1,
		ProbeInterval:    10 * time.Millisecond,
		ProbeTimeout:     500 * time.Millisecond,
	})
	// Trip the endpoint.
	c.RecordFailure(srv.URL)
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("endpoint should be unhealthy before probe")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	// Wait up to 2s for the probe to recover the endpoint.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.GetHealthyEndpoints()) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("probe did not recover endpoint within 2s (health hits=%d)", healthHits.Load())
}

func TestProbe_4xxTreatedAsReachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Any path returns 404. The probe should still treat the peer as reachable.
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewClient([]string{srv.URL}, "", HealthConfig{
		FailureThreshold: 1,
		ProbeInterval:    10 * time.Millisecond,
		ProbeTimeout:     500 * time.Millisecond,
	})
	c.RecordFailure(srv.URL) // trip

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.GetHealthyEndpoints()) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("probe did not mark 4xx peer as healthy within 2s")
}

func TestGetEndpointStatuses_SourceAndHealth(t *testing.T) {
	c := NewClient([]string{"https://a.example", "https://b.example"}, "", HealthConfig{FailureThreshold: 2})
	c.AddEndpoints([]string{"https://c.example"})

	// Trip b to unhealthy.
	c.RecordFailure("https://b.example")
	c.RecordFailure("https://b.example")

	got := c.GetEndpointStatuses()
	want := []EndpointStatus{
		{URL: "https://a.example", Source: "configured", Healthy: true},
		{URL: "https://b.example", Source: "configured", Healthy: false},
		{URL: "https://c.example", Source: "discovered", Healthy: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected statuses:\n got  %+v\n want %+v", got, want)
	}
}

func TestGetEndpointStatuses_EmptyReturnsEmptySlice(t *testing.T) {
	c := NewClient(nil, "", HealthConfig{})
	got := c.GetEndpointStatuses()
	if got == nil {
		t.Fatal("expected empty slice, got nil — callers may json-encode and need an array, not null")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %+v", got)
	}
}

func TestProbe_StopsOnClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient([]string{srv.URL}, "", HealthConfig{
		ProbeInterval: 10 * time.Millisecond,
		ProbeTimeout:  500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// Close should return promptly even though the probe goroutine would
	// otherwise run forever on its ticker.
	done := make(chan struct{})
	go func() {
		c.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s")
	}
}
