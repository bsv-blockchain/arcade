package propagation

import (
	"math"
	"math/rand/v2"
	"time"
)

const maxBackoffCap = 30 * time.Second

// ComputeBackoff returns the next retry time for a given attempt number using
// exponential backoff (baseBackoffMs * 2^attempt) with ±25% jitter, capped at
// 30 seconds.
func ComputeBackoff(baseBackoffMs int, attempt int) time.Time {
	base := time.Duration(baseBackoffMs) * time.Millisecond
	delay := base * time.Duration(math.Pow(2, float64(attempt)))
	if delay > maxBackoffCap {
		delay = maxBackoffCap
	}

	jitter := float64(delay) * 0.25
	delay = time.Duration(float64(delay) + (rand.Float64()*2-1)*jitter)
	if delay < 0 {
		delay = 0
	}

	return time.Now().Add(delay)
}
