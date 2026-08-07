package api_server

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/validator"
)

func newFeeServer(ms *mockStore) *Server {
	return &Server{
		cfg:       &config.Config{Network: "mainnet"},
		logger:    zap.NewNop(),
		store:     ms,
		validator: validator.NewValidator(nil), // starts at DefaultMinFeePerKB (100)
	}
}

func TestLowestObservedFeePerKB(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	ttl := 15 * time.Minute

	t.Run("min over fresh peers, stale excluded", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "p1", MiningFeeSatoshis: 500, MiningFeeBytes: 1000, LastSeen: now},
			{PeerID: "p2", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, LastSeen: now.Add(-time.Minute)},
			// stale cheapest — must be excluded
			{PeerID: "p3", MiningFeeSatoshis: 1, MiningFeeBytes: 1000, LastSeen: now.Add(-24 * time.Hour)},
		}
		got, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 100 {
			t.Fatalf("got (%d,%v), want (100,true)", got, ok)
		}
	})

	t.Run("no fresh peers", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "p1", MiningFeeSatoshis: 1, MiningFeeBytes: 1000, LastSeen: now.Add(-24 * time.Hour)},
		}
		if _, ok := lowestObservedFeePerKB(peers, ttl, now); ok {
			t.Fatal("expected ok=false when all peers stale")
		}
	})

	t.Run("empty", func(t *testing.T) {
		if _, ok := lowestObservedFeePerKB(nil, ttl, now); ok {
			t.Fatal("expected ok=false for empty input")
		}
	})

	t.Run("non-1000 byte basis normalized to sat/kB", func(t *testing.T) {
		// 250 sat per 500 bytes == 500 sat/kB; 100 sat per 1000 bytes == 100 sat/kB.
		peers := []store.PeerPolicy{
			{PeerID: "p1", MiningFeeSatoshis: 250, MiningFeeBytes: 500, LastSeen: now},
			{PeerID: "p2", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, LastSeen: now},
		}
		got, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 100 {
			t.Fatalf("got (%d,%v), want (100,true)", got, ok)
		}
	})

	t.Run("rounds up, never below the peer rate", func(t *testing.T) {
		// 1 sat / 1001 bytes is ~0.999 sat/kB; must round UP to 1, not down to
		// 0 (which would let arcade accept fee=0 that the peer would reject).
		peers := []store.PeerPolicy{
			{PeerID: "p1", MiningFeeSatoshis: 1, MiningFeeBytes: 1001, LastSeen: now},
		}
		got, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 1 {
			t.Fatalf("got (%d,%v), want (1,true) — ceil, not floor", got, ok)
		}
	})

	t.Run("absurd fee saturates and is never the minimum", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "huge", MiningFeeSatoshis: 1 << 60, MiningFeeBytes: 1, LastSeen: now},
			{PeerID: "sane", MiningFeeSatoshis: 20, MiningFeeBytes: 1000, LastSeen: now},
		}
		got, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 20 {
			t.Fatalf("got (%d,%v), want (20,true) — huge fee must not wrap into the minimum", got, ok)
		}
	})

	t.Run("zero-byte row skipped", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "bad", MiningFeeSatoshis: 5, MiningFeeBytes: 0, LastSeen: now},
			{PeerID: "ok", MiningFeeSatoshis: 80, MiningFeeBytes: 1000, LastSeen: now},
		}
		got, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 80 {
			t.Fatalf("got (%d,%v), want (80,true)", got, ok)
		}
	})
}

func TestRefreshFeeOnce_UpdatesValidatorToObservedMin(t *testing.T) {
	ms := &mockStore{peerPolicies: []store.PeerPolicy{
		{PeerID: "p1", MiningFeeSatoshis: 500, MiningFeeBytes: 1000, LastSeen: time.Now()},
		{PeerID: "p2", MiningFeeSatoshis: 50, MiningFeeBytes: 1000, LastSeen: time.Now()},
	}}
	s := newFeeServer(ms)

	s.refreshFeeOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != 50 {
		t.Errorf("validator floor = %d, want 50 (network minimum)", got)
	}
}

func TestRefreshFeeOnce_NoObservationsUsesDefault(t *testing.T) {
	s := newFeeServer(&mockStore{})
	s.validator.SetMinFeePerKB(7) // pretend a prior observation had set it low

	s.refreshFeeOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != config.DefaultValidatorMinFeePerKB {
		t.Errorf("validator floor = %d, want %d (default when nothing observed)", got, config.DefaultValidatorMinFeePerKB)
	}
}

func TestRefreshFeeOnce_StoreErrorKeepsCurrentFloor(t *testing.T) {
	s := newFeeServer(&mockStore{listPeerPoliciesErr: errTest})
	s.validator.SetMinFeePerKB(42)

	s.refreshFeeOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != 42 {
		t.Errorf("validator floor = %d, want 42 (unchanged on store error)", got)
	}
}

func TestFeePinned(t *testing.T) {
	cases := []struct {
		name string
		vcfg config.ValidatorConfig
		want bool
	}{
		{"unset", config.ValidatorConfig{}, false},
		{"accept zero fee", config.ValidatorConfig{AcceptZeroFee: true}, true},
		{"explicit min fee", config.ValidatorConfig{MinFeePerKB: 100}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{cfg: &config.Config{Validator: tc.vcfg}}
			if got := s.feePinned(); got != tc.want {
				t.Errorf("feePinned() = %v, want %v", got, tc.want)
			}
		})
	}
}
