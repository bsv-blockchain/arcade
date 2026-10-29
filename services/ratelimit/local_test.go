package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalLimiter(t *testing.T) {
	l := newLocalLimiter(16)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	d := l.take("ip:1.2.3.4", 2, 1, now)
	require.True(t, d.Allowed)
	require.Equal(t, int64(1), d.Remaining)
	require.True(t, l.take("ip:1.2.3.4", 2, 1, now).Allowed)

	d = l.take("ip:1.2.3.4", 2, 1, now)
	require.False(t, d.Allowed, "burst is one minute of quota")
	require.InDelta(t, 30.0, d.Reset.Seconds(), 0.001, "2/min refills one unit every 30s")

	require.True(t, l.take("ip:5.6.7.8", 2, 1, now).Allowed, "subjects are independent")
	require.True(t, l.take("ip:1.2.3.4", 2, 1, now.Add(30*time.Second)).Allowed, "refilled")
	require.False(t, l.take("ip:9.9.9.9", 2, 3, now).Allowed, "a cost above the burst never fits")
}

func TestLocalLimiterResetWhenAllowed(t *testing.T) {
	l := newLocalLimiter(16)
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	d := l.take("ip:10.0.0.1", 60, 30, now)
	require.True(t, d.Allowed)
	require.Equal(t, int64(30), d.Remaining)
	require.InDelta(t, 30.0, d.Reset.Seconds(), 0.001, "60/min refills to full one second per unit")
}

func TestInflight(t *testing.T) {
	f := newInflight()
	require.True(t, f.acquire("key:a", 2))
	require.True(t, f.acquire("key:a", 2))
	require.False(t, f.acquire("key:a", 2))
	require.True(t, f.acquire("key:b", 2))
	f.release("key:a")
	require.True(t, f.acquire("key:a", 2))
	f.release("key:a")
	f.release("key:a")
	f.release("key:b")
	require.Empty(t, f.n, "released subjects are dropped")
}

func TestInflightLimitOne(t *testing.T) {
	f := newInflight()
	require.True(t, f.acquire("key:solo", 1))
	require.False(t, f.acquire("key:solo", 1))
	f.release("key:solo")
	require.Empty(t, f.n, "released subjects are dropped")
}
