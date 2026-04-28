package propagation

import (
	"testing"
	"time"
)

func TestComputeBackoff_ProgressesExponentially(t *testing.T) {
	// base=100ms, attempt=0..4. Expected midpoints (pre-jitter): 100, 200, 400,
	// 800, 1600. With ±25% jitter, each actual delay lands in [0.75x, 1.25x]
	// of the midpoint. Assert each attempt's delay is strictly greater than the
	// previous lower bound — i.e. backoff monotonically progresses.
	const base = 100
	var prevLower time.Duration
	now := time.Now()
	for attempt := 0; attempt < 5; attempt++ {
		got := ComputeBackoff(base, attempt)
		delay := got.Sub(now)
		if delay < prevLower {
			t.Errorf("attempt %d: delay %v regressed below prev lower bound %v", attempt, delay, prevLower)
		}
		// Track this attempt's lower bound (0.75x midpoint) for the next check.
		midpoint := time.Duration(base) * time.Millisecond
		for i := 0; i < attempt; i++ {
			midpoint *= 2
		}
		prevLower = midpoint * 3 / 4
	}
}

func TestComputeBackoff_CapsAtMax(t *testing.T) {
	// attempt=20 with base=500ms would be 500ms * 2^20 ≈ 524288s pre-cap.
	// Post-cap is 30s, post-jitter is [22.5s, 37.5s].
	got := ComputeBackoff(500, 20)
	delay := got.Sub(time.Now())
	const jitteredMax = maxBackoffCap + (maxBackoffCap / 4) + 100*time.Millisecond // +slack for wall-clock drift
	if delay > jitteredMax {
		t.Errorf("expected capped delay ≤ %v, got %v", jitteredMax, delay)
	}
	if delay < maxBackoffCap*3/4-100*time.Millisecond {
		t.Errorf("expected delay ≥ 75%% of cap, got %v", delay)
	}
}

func TestComputeBackoff_NonNegative(t *testing.T) {
	// attempt=0 with tiny base — jitter can in theory subtract; implementation
	// clamps to zero. Verify by sampling multiple times (jitter is random).
	for i := 0; i < 50; i++ {
		got := ComputeBackoff(1, 0)
		if got.Before(time.Now().Add(-10 * time.Millisecond)) {
			t.Fatalf("iteration %d: backoff went backward: %v vs now %v", i, got, time.Now())
		}
	}
}
