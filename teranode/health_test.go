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
	c := NewClient([]string{testEndpointA, testEndpointB}, "", HealthConfig{FailureThreshold: 3})

	// Two sub-threshold failures keep the endpoint healthy.
	c.RecordFailure(testEndpointA)
	c.RecordFailure(testEndpointA)
	if healthy := c.GetHealthyEndpoints(); len(healthy) != 2 {
		t.Fatalf("expected both endpoints healthy after 2 failures, got %v", healthy)
	}

	// Third failure trips.
	c.RecordFailure(testEndpointA)
	healthy := c.GetHealthyEndpoints()
	if !reflect.DeepEqual(healthy, []string{testEndpointB}) {
		t.Fatalf("expected only b after trip, got %v", healthy)
	}
	// GetEndpoints still returns everything.
	if len(c.GetEndpoints()) != 2 {
		t.Fatalf("GetEndpoints should still return both, got %v", c.GetEndpoints())
	}
}

func TestRecordSuccess_ResetsCounter(t *testing.T) {
	c := NewClient([]string{testEndpointA}, "", HealthConfig{FailureThreshold: 3})

	c.RecordFailure(testEndpointA)
	c.RecordFailure(testEndpointA)
	c.RecordSuccess(testEndpointA)
	// After reset, three more failures should be required to trip.
	c.RecordFailure(testEndpointA)
	c.RecordFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatalf("endpoint should still be healthy after reset + 2 failures")
	}
	c.RecordFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatalf("endpoint should trip after 3 post-reset failures")
	}
}

// TestRecordBroadcastFailure_TripsAfterThreshold covers the slow-track
// breaker: persistent non-2xx broadcast responses sideline an endpoint even
// though it is still responding (reachability counter never advances).
// Without this, ngrok-proxied datahubs serving "endpoint offline" 404s and
// peers with permanently disagreeing validation rules kept being included
// in every broadcast, wasting worker-pool slots.
func TestRecordBroadcastFailure_TripsAfterThreshold(t *testing.T) {
	c := NewClient([]string{testEndpointA, testEndpointB}, "", HealthConfig{
		FailureThreshold:          3,
		BroadcastFailureThreshold: 5,
	})

	// Sub-threshold broadcast failures keep the endpoint healthy.
	for i := 0; i < 4; i++ {
		c.RecordBroadcastFailure(testEndpointA)
	}
	if healthy := c.GetHealthyEndpoints(); len(healthy) != 2 {
		t.Fatalf("expected both endpoints healthy after 4 broadcast failures, got %v", healthy)
	}

	// Fifth broadcast failure trips the slow-track breaker.
	c.RecordBroadcastFailure(testEndpointA)
	healthy := c.GetHealthyEndpoints()
	if !reflect.DeepEqual(healthy, []string{testEndpointB}) {
		t.Fatalf("expected only b after broadcast-trip, got %v", healthy)
	}
}

// TestRecordBroadcastFailure_IndependentFromReachability proves the two
// counters are independent: an endpoint with 2 reachability failures and 4
// broadcast failures is still healthy (neither threshold reached), even
// though their sum exceeds either threshold.
func TestRecordBroadcastFailure_IndependentFromReachability(t *testing.T) {
	c := NewClient([]string{testEndpointA}, "", HealthConfig{
		FailureThreshold:          3,
		BroadcastFailureThreshold: 5,
	})

	c.RecordFailure(testEndpointA)
	c.RecordFailure(testEndpointA)
	c.RecordBroadcastFailure(testEndpointA)
	c.RecordBroadcastFailure(testEndpointA)
	c.RecordBroadcastFailure(testEndpointA)
	c.RecordBroadcastFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatalf("endpoint should still be healthy (neither counter tripped)")
	}
	// One more of either kind trips. Take the reachability path.
	c.RecordFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatalf("endpoint should trip on the 3rd reachability failure")
	}
}

// TestRecordSuccess_ResetsBothCounters confirms a 2xx response is the
// single recovery signal: it clears the reachability counter AND the
// broadcast counter, regardless of which one is closer to tripping. Without
// this, a flapping endpoint with intermittent successes would accumulate
// broadcast failures forever and eventually trip even though it's mostly
// working.
func TestRecordSuccess_ResetsBothCounters(t *testing.T) {
	c := NewClient([]string{testEndpointA}, "", HealthConfig{
		FailureThreshold:          3,
		BroadcastFailureThreshold: 5,
	})

	// Accumulate 2 reachability failures + 4 broadcast failures.
	c.RecordFailure(testEndpointA)
	c.RecordFailure(testEndpointA)
	for i := 0; i < 4; i++ {
		c.RecordBroadcastFailure(testEndpointA)
	}

	// One success wipes both slates.
	c.RecordSuccess(testEndpointA)

	// Now we should need full thresholds again on either track.
	for i := 0; i < 4; i++ {
		c.RecordBroadcastFailure(testEndpointA)
	}
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatalf("broadcast counter did not reset after success")
	}
	c.RecordBroadcastFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatalf("expected broadcast trip on 5th post-reset failure")
	}
}

func TestRecordSuccess_RecoversUnhealthy(t *testing.T) {
	c := NewClient([]string{testEndpointA}, "", HealthConfig{FailureThreshold: 2})
	c.RecordFailure(testEndpointA)
	c.RecordFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("expected endpoint to be unhealthy")
	}
	c.RecordSuccess(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatal("expected endpoint to recover to healthy")
	}
}

func TestRecordFailure_UnknownURL_NoOp(t *testing.T) {
	c := NewClient([]string{testEndpointA}, "", HealthConfig{FailureThreshold: 1})
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
	c := NewClient([]string{testEndpointA, testEndpointB}, "", HealthConfig{FailureThreshold: 1})
	snap := c.GetHealthyEndpoints()
	c.RecordFailure(testEndpointA) // trips a
	if len(snap) != 2 {
		t.Fatalf("previously-returned snapshot was mutated: %v", snap)
	}
}

func TestGetHealthyEndpoints_PreservesOrder(t *testing.T) {
	c := NewClient([]string{testEndpointA, testEndpointB, "https://c.example"}, "", HealthConfig{FailureThreshold: 2})
	// Trip b, leaving a and c healthy.
	c.RecordFailure(testEndpointB)
	c.RecordFailure(testEndpointB)
	got := c.GetHealthyEndpoints()
	want := []string{testEndpointA, "https://c.example"}
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
	c := NewClient([]string{testEndpointA}, "", HealthConfig{FailureThreshold: 2})
	c.RecordFailure(testEndpointA)
	c.RecordFailure(testEndpointA) // trip
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("expected endpoint to be unhealthy")
	}
	// Re-announcement is deduplicated — must NOT reset health state.
	added := c.AddEndpoints([]string{testEndpointA})
	if added != 0 {
		t.Fatalf("expected 0 new endpoints, got %d", added)
	}
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("rediscovered unhealthy endpoint must stay unhealthy")
	}
}

// Concurrent RecordSuccess / RecordFailure + readers — -race must stay silent.
func TestHealthTracker_Concurrent(_ *testing.T) {
	c := NewClient([]string{testEndpointA, testEndpointB}, "", HealthConfig{FailureThreshold: 1000})

	const workers = 8
	const perWorker = 200
	var wg sync.WaitGroup
	wg.Add(workers * 3)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				c.RecordFailure(testEndpointA)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				c.RecordSuccess(testEndpointA)
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	c := NewClient([]string{testEndpointA, testEndpointB}, "", HealthConfig{FailureThreshold: 2})
	c.AddEndpoints([]string{"https://c.example"})

	// Trip b to unhealthy.
	c.RecordFailure(testEndpointB)
	c.RecordFailure(testEndpointB)

	got := c.GetEndpointStatuses()
	want := []EndpointStatus{
		{URL: testEndpointA, Source: "configured", Healthy: true},
		{URL: testEndpointB, Source: "configured", Healthy: false},
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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

// TestProbe_TripsUnreachableHealthyEndpoint is the truthful-health
// regression test: a registered-but-unreachable endpoint must be demoted by
// the probe loop alone — no broadcast traffic — so pods that never
// broadcast (api-server serving /health) stop reporting dead endpoints as
// healthy.
func TestProbe_TripsUnreachableHealthyEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadURL := srv.URL
	srv.Close() // port now refuses connections

	c := NewClient([]string{deadURL}, "", HealthConfig{
		FailureThreshold: 3,
		ProbeInterval:    10 * time.Millisecond,
		ProbeTimeout:     200 * time.Millisecond,
	})
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatal("endpoint should start healthy")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		statuses := c.GetEndpointStatuses()
		if len(statuses) == 1 && !statuses[0].Healthy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("probe loop did not demote an unreachable healthy endpoint within 2s")
}

// TestProbe_HealthySuccess_DoesNotResetBroadcastCounter guards the reason
// probe outcomes use a dedicated counter: an endpoint whose /health answers
// 200 but whose broadcasts persistently fail non-2xx must still trip the
// slow track, even with fast probes succeeding between every failure.
func TestProbe_HealthySuccess_DoesNotResetBroadcastCounter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient([]string{srv.URL}, "", HealthConfig{
		FailureThreshold:          3,
		BroadcastFailureThreshold: 5,
		ProbeInterval:             5 * time.Millisecond,
		ProbeTimeout:              200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	for i := 0; i < 5; i++ {
		// Several probe intervals pass between failures; each probe succeeds
		// against the live server. If probe success reset the broadcast
		// counter, the threshold could never be reached.
		time.Sleep(15 * time.Millisecond)
		c.RecordBroadcastFailure(srv.URL)
	}
	if got := len(c.GetHealthyEndpoints()); got != 0 {
		t.Fatal("slow-track breaker did not trip: probe successes must not reset consecutiveBroadcastFailures")
	}
}

// TestProbe_HealthySuccess_DoesNotResetReachabilityCounter is the fast-track
// sibling: broadcast-path transport failures must accumulate to the
// threshold regardless of interleaved probe successes.
func TestProbe_HealthySuccess_DoesNotResetReachabilityCounter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient([]string{srv.URL}, "", HealthConfig{
		FailureThreshold: 3,
		ProbeInterval:    5 * time.Millisecond,
		ProbeTimeout:     200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	for i := 0; i < 3; i++ {
		time.Sleep(15 * time.Millisecond)
		c.RecordFailure(srv.URL)
	}
	if got := len(c.GetHealthyEndpoints()); got != 0 {
		t.Fatal("fast-track breaker did not trip: probe successes must not reset consecutiveFailures")
	}
}

// TestRecordProbeCounters_UnitSemantics pins the probe counter's consecutive
// semantics and the full-recovery contract without any HTTP traffic.
func TestRecordProbeCounters_UnitSemantics(t *testing.T) {
	c := NewClient([]string{testEndpointA}, "", HealthConfig{FailureThreshold: 3})

	// Two failures + a success: counter resets, still healthy.
	c.recordProbeFailure(testEndpointA)
	c.recordProbeFailure(testEndpointA)
	c.recordProbeSuccess(testEndpointA)
	c.recordProbeFailure(testEndpointA)
	c.recordProbeFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatal("non-consecutive probe failures must not trip the breaker")
	}

	// Third consecutive failure trips.
	c.recordProbeFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("3rd consecutive probe failure should trip the breaker")
	}

	// Probe success on an unhealthy endpoint = full recovery: all counters
	// reset, so the slow track needs its full threshold again afterwards.
	c.RecordBroadcastFailure(testEndpointA) // accumulate some pre-recovery state
	c.recordProbeSuccess(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatal("probe success should recover an unhealthy endpoint")
	}
	// 9 more broadcast failures shouldn't trip a threshold of 10 if the
	// pre-recovery failure was wiped.
	for i := 0; i < 9; i++ {
		c.RecordBroadcastFailure(testEndpointA)
	}
	if len(c.GetHealthyEndpoints()) != 1 {
		t.Fatal("recovery must reset consecutiveBroadcastFailures (9 < threshold 10)")
	}
	c.RecordBroadcastFailure(testEndpointA)
	if len(c.GetHealthyEndpoints()) != 0 {
		t.Fatal("10th consecutive broadcast failure should trip")
	}
}
