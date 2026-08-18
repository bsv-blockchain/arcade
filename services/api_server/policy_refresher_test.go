package api_server

import (
	"context"
	"math"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/validator"
)

func newPolicyServer(ms *mockStore) *Server {
	return newPolicyServerWithConfig(ms, config.ValidatorConfig{})
}

func newPolicyServerWithConfig(ms *mockStore, vcfg config.ValidatorConfig) *Server {
	return &Server{
		cfg:       &config.Config{Network: config.NetworkMainnet, Validator: vcfg},
		logger:    zap.NewNop(),
		store:     ms,
		validator: validator.NewValidator(nil), // starts at the built-in defaults
	}
}

// freshPolicy is a peer row seen just now, with both size limits advertised.
func freshPolicy(id string, sats, maxTx, maxScript uint64) store.PeerPolicy {
	return store.PeerPolicy{
		PeerID: id, MiningFeeSatoshis: sats, MiningFeeBytes: 1000,
		MaxTxSizePolicy: maxTx, MaxScriptSizePolicy: maxScript, LastSeen: time.Now(),
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
		got, _, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 100 {
			t.Fatalf("got (%d,%v), want (100,true)", got, ok)
		}
	})

	t.Run("no fresh peers", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "p1", MiningFeeSatoshis: 1, MiningFeeBytes: 1000, LastSeen: now.Add(-24 * time.Hour)},
		}
		if _, _, ok := lowestObservedFeePerKB(peers, ttl, now); ok {
			t.Fatal("expected ok=false when all peers stale")
		}
	})

	t.Run("empty", func(t *testing.T) {
		if _, _, ok := lowestObservedFeePerKB(nil, ttl, now); ok {
			t.Fatal("expected ok=false for empty input")
		}
	})

	t.Run("non-1000 byte basis normalized to sat/kB", func(t *testing.T) {
		// 250 sat per 500 bytes == 500 sat/kB; 100 sat per 1000 bytes == 100 sat/kB.
		peers := []store.PeerPolicy{
			{PeerID: "p1", MiningFeeSatoshis: 250, MiningFeeBytes: 500, LastSeen: now},
			{PeerID: "p2", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, LastSeen: now},
		}
		got, _, ok := lowestObservedFeePerKB(peers, ttl, now)
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
		got, _, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 1 {
			t.Fatalf("got (%d,%v), want (1,true) — ceil, not floor", got, ok)
		}
	})

	t.Run("absurd fee saturates and is never the minimum", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "huge", MiningFeeSatoshis: 1 << 60, MiningFeeBytes: 1, LastSeen: now},
			{PeerID: "sane", MiningFeeSatoshis: 20, MiningFeeBytes: 1000, LastSeen: now},
		}
		got, _, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 20 {
			t.Fatalf("got (%d,%v), want (20,true) — huge fee must not wrap into the minimum", got, ok)
		}
	})

	t.Run("zero-byte row skipped", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "bad", MiningFeeSatoshis: 5, MiningFeeBytes: 0, LastSeen: now},
			{PeerID: "ok", MiningFeeSatoshis: 80, MiningFeeBytes: 1000, LastSeen: now},
		}
		got, _, ok := lowestObservedFeePerKB(peers, ttl, now)
		if !ok || got != 80 {
			t.Fatalf("got (%d,%v), want (80,true)", got, ok)
		}
	})
}

// TestHighestObservedLimit covers the size-limit counterpart of
// lowestObservedFeePerKB. It takes the maximum rather than the minimum: a
// transaction is pre-rejected only when NO peer on the network would accept it,
// which is the same "at least one node will take it" rule the fee minimum
// expresses, read from the other end.
func TestHighestObservedLimit(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	ttl := 15 * time.Minute
	pickTx := func(p store.PeerPolicy) uint64 { return p.MaxTxSizePolicy }

	t.Run("max over fresh peers, stale excluded", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "p1", MaxTxSizePolicy: 10_000_000, LastSeen: now},
			{PeerID: "p2", MaxTxSizePolicy: 100_000_000, LastSeen: now.Add(-time.Minute)},
			// stale, and the most permissive — must be excluded
			{PeerID: "p3", MaxTxSizePolicy: 900_000_000, LastSeen: now.Add(-24 * time.Hour)},
		}
		got, ok := highestObservedLimit(peers, ttl, now, pickTx)
		if !ok || got != 100_000_000 {
			t.Fatalf("got (%d,%v), want (100000000,true)", got, ok)
		}
	})

	t.Run("zero means not advertised, not 'accepts nothing'", func(t *testing.T) {
		// A legacy peer advertises no limit. Reading its 0 as a real limit
		// would be meaningless here (max ignores it) but would be a
		// network-wide outage under any min-based rule, so it is skipped
		// explicitly rather than incidentally.
		peers := []store.PeerPolicy{
			{PeerID: "legacy", MaxTxSizePolicy: 0, LastSeen: now},
			{PeerID: "modern", MaxTxSizePolicy: 20_000_000, LastSeen: now},
		}
		got, ok := highestObservedLimit(peers, ttl, now, pickTx)
		if !ok || got != 20_000_000 {
			t.Fatalf("got (%d,%v), want (20000000,true)", got, ok)
		}
	})

	t.Run("no peer advertised a limit", func(t *testing.T) {
		peers := []store.PeerPolicy{{PeerID: "legacy", MaxTxSizePolicy: 0, LastSeen: now}}
		if _, ok := highestObservedLimit(peers, ttl, now, pickTx); ok {
			t.Fatal("expected ok=false when no peer advertised a limit")
		}
	})

	t.Run("empty", func(t *testing.T) {
		if _, ok := highestObservedLimit(nil, ttl, now, pickTx); ok {
			t.Fatal("expected ok=false for empty input")
		}
	})

	t.Run("script limit uses its own field", func(t *testing.T) {
		peers := []store.PeerPolicy{
			{PeerID: "p1", MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000, LastSeen: now},
			{PeerID: "p2", MaxTxSizePolicy: 10_000_000, MaxScriptSizePolicy: 900_000, LastSeen: now},
		}
		got, ok := highestObservedLimit(peers, ttl, now, func(p store.PeerPolicy) uint64 { return p.MaxScriptSizePolicy })
		if !ok || got != 900_000 {
			t.Fatalf("got (%d,%v), want (900000,true)", got, ok)
		}
	})
}

// TestDiscoveredLimit pins the floor, which is what makes it safe to derive a
// limit from unauthenticated gossip at all: observation may only ever widen the
// limit from arcade's built-in default, never narrow it. Without the floor a
// single misconfigured — or hostile — peer advertising a tiny limit would make
// arcade reject transactions the rest of the network mines happily.
func TestDiscoveredLimit(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	ttl := 15 * time.Minute
	pickTx := func(p store.PeerPolicy) uint64 { return p.MaxTxSizePolicy }
	const floor = uint64(config.DefaultValidatorMaxTxSizePolicy)

	cases := []struct {
		name  string
		peers []store.PeerPolicy
		want  uint64
	}{
		{
			name:  "a more permissive network widens the limit",
			peers: []store.PeerPolicy{{PeerID: "p1", MaxTxSizePolicy: 100_000_000, LastSeen: now}},
			want:  100_000_000,
		},
		{
			name:  "a hostile peer cannot narrow it below the default",
			peers: []store.PeerPolicy{{PeerID: "evil", MaxTxSizePolicy: 1000, LastSeen: now}},
			want:  floor,
		},
		{
			name: "one hostile peer among honest ones is simply outvoted by the max",
			peers: []store.PeerPolicy{
				{PeerID: "evil", MaxTxSizePolicy: 1000, LastSeen: now},
				{PeerID: "honest", MaxTxSizePolicy: 100_000_000, LastSeen: now},
			},
			want: 100_000_000,
		},
		{
			name:  "nothing observed falls back to the default",
			peers: nil,
			want:  floor,
		},
		{
			name:  "stale observations fall back to the default",
			peers: []store.PeerPolicy{{PeerID: "gone", MaxTxSizePolicy: 100_000_000, LastSeen: now.Add(-24 * time.Hour)}},
			want:  floor,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := discoveredLimit(tc.peers, ttl, now, pickTx, floor); got != tc.want {
				t.Errorf("discoveredLimit() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRefreshPolicyOnce_UpdatesValidatorToObservedMin(t *testing.T) {
	ms := &mockStore{peerPolicies: []store.PeerPolicy{
		freshPolicy("p1", 500, 0, 0),
		freshPolicy("p2", 50, 0, 0),
	}}
	s := newPolicyServer(ms)

	s.refreshPolicyOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != 50 {
		t.Errorf("validator floor = %d, want 50 (network minimum)", got)
	}
}

// TestRefreshPolicyOnce_UpdatesSizesFromObservations is the end of the chain the
// p2p-client starts: teranode advertises its size policy, arcade records it,
// and intake plus GET /policy adopt the most permissive one on the network.
func TestRefreshPolicyOnce_UpdatesSizesFromObservations(t *testing.T) {
	ms := &mockStore{peerPolicies: []store.PeerPolicy{
		freshPolicy("p1", 100, 100_000_000, 600_000),
		freshPolicy("p2", 100, 20_000_000, 500_000),
	}}
	s := newPolicyServer(ms)

	s.refreshPolicyOnce(context.Background())

	if got := s.validator.MaxTxSizePolicy(); got != 100_000_000 {
		t.Errorf("MaxTxSizePolicy = %d, want 100000000 (most permissive peer)", got)
	}
	if got := s.validator.MaxScriptSizePolicy(); got != 600_000 {
		t.Errorf("MaxScriptSizePolicy = %d, want 600000 (most permissive peer)", got)
	}
}

func TestRefreshPolicyOnce_NoObservationsUsesDefaults(t *testing.T) {
	s := newPolicyServer(&mockStore{})
	// Pretend prior observations had moved every dimension.
	s.validator.SetNetworkPolicy(7, 100_000_000, 600_000)

	s.refreshPolicyOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != config.DefaultValidatorMinFeePerKB {
		t.Errorf("validator floor = %d, want %d (default when nothing observed)", got, config.DefaultValidatorMinFeePerKB)
	}
	if got := s.validator.MaxTxSizePolicy(); got != config.DefaultValidatorMaxTxSizePolicy {
		t.Errorf("MaxTxSizePolicy = %d, want %d", got, config.DefaultValidatorMaxTxSizePolicy)
	}
	if got := s.validator.MaxScriptSizePolicy(); got != config.DefaultValidatorMaxScriptSizePolicy {
		t.Errorf("MaxScriptSizePolicy = %d, want %d", got, config.DefaultValidatorMaxScriptSizePolicy)
	}
}

func TestRefreshPolicyOnce_StoreErrorKeepsCurrentPolicy(t *testing.T) {
	s := newPolicyServer(&mockStore{listPeerPoliciesErr: errTest})
	s.validator.SetNetworkPolicy(42, 100_000_000, 600_000)

	s.refreshPolicyOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != 42 {
		t.Errorf("validator floor = %d, want 42 (unchanged on store error)", got)
	}
	if got := s.validator.MaxTxSizePolicy(); got != 100_000_000 {
		t.Errorf("MaxTxSizePolicy = %d, want 100000000 (unchanged on store error)", got)
	}
	if got := s.validator.MaxScriptSizePolicy(); got != 600_000 {
		t.Errorf("MaxScriptSizePolicy = %d, want 600000 (unchanged on store error)", got)
	}
}

// TestRefreshPolicyOnce_PinnedDimensionsNotOverwritten checks that the refresher
// running on behalf of one unpinned dimension does not quietly take over the
// dimensions the operator did configure.
func TestRefreshPolicyOnce_PinnedDimensionsNotOverwritten(t *testing.T) {
	peers := []store.PeerPolicy{freshPolicy("p1", 50, 100_000_000, 600_000)}

	t.Run("pinned max tx size survives", func(t *testing.T) {
		s := newPolicyServerWithConfig(&mockStore{peerPolicies: peers},
			config.ValidatorConfig{MaxTxSizePolicy: 5_000_000})

		s.refreshPolicyOnce(context.Background())

		if got := s.validator.MaxTxSizePolicy(); got != 5_000_000 {
			t.Errorf("MaxTxSizePolicy = %d, want the pinned 5000000", got)
		}
		// The unpinned dimensions still track the network.
		if got := s.validator.MinFeePerKB(); got != 50 {
			t.Errorf("MinFeePerKB = %d, want 50 (still tracked)", got)
		}
		if got := s.validator.MaxScriptSizePolicy(); got != 600_000 {
			t.Errorf("MaxScriptSizePolicy = %d, want 600000 (still tracked)", got)
		}
	})

	t.Run("pinned fee survives", func(t *testing.T) {
		s := newPolicyServerWithConfig(&mockStore{peerPolicies: peers},
			config.ValidatorConfig{MinFeePerKB: 250})
		s.validator.SetMinFeePerKB(250) // as app wiring would, at construction

		s.refreshPolicyOnce(context.Background())

		if got := s.validator.MinFeePerKB(); got != 250 {
			t.Errorf("MinFeePerKB = %d, want the pinned 250", got)
		}
		if got := s.validator.MaxTxSizePolicy(); got != 100_000_000 {
			t.Errorf("MaxTxSizePolicy = %d, want 100000000 (still tracked)", got)
		}
	})
}

// TestRefreshPolicyOnce_HostilePeerCannotBreakIntake is the safety property in
// one place: node_status is unauthenticated, so a peer can advertise anything.
// Whatever it sends, the refresher must leave arcade with a working validator
// whose limits are no worse than the built-in defaults.
func TestRefreshPolicyOnce_HostilePeerCannotBreakIntake(t *testing.T) {
	hostile := []store.PeerPolicy{
		{PeerID: "tiny", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, MaxTxSizePolicy: 1, MaxScriptSizePolicy: 1, LastSeen: time.Now()},
		{PeerID: "huge", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, MaxTxSizePolicy: math.MaxInt64, MaxScriptSizePolicy: math.MaxInt64, LastSeen: time.Now()},
	}
	s := newPolicyServer(&mockStore{peerPolicies: hostile})

	s.refreshPolicyOnce(context.Background())

	if got := s.validator.MaxTxSizePolicy(); got < config.DefaultValidatorMaxTxSizePolicy {
		t.Errorf("MaxTxSizePolicy = %d, must never fall below the built-in default %d",
			got, config.DefaultValidatorMaxTxSizePolicy)
	}
	if got := s.validator.MaxScriptSizePolicy(); got < config.DefaultValidatorMaxScriptSizePolicy {
		t.Errorf("MaxScriptSizePolicy = %d, must never fall below the built-in default %d",
			got, config.DefaultValidatorMaxScriptSizePolicy)
	}
	// And the engine survived: the validator still validates.
	if s.validator.MaxTxSizePolicy() == 0 {
		t.Error("validator was left with no usable policy")
	}
}

func TestPolicyPinned(t *testing.T) {
	cases := []struct {
		name                              string
		vcfg                              config.ValidatorConfig
		fee, maxTx, maxScript, anyTracked bool
	}{
		{name: "unset", vcfg: config.ValidatorConfig{}, anyTracked: true},
		{name: "accept zero fee", vcfg: config.ValidatorConfig{AcceptZeroFee: true}, fee: true, anyTracked: true},
		{name: "explicit min fee", vcfg: config.ValidatorConfig{MinFeePerKB: 100}, fee: true, anyTracked: true},
		{name: "explicit max tx size", vcfg: config.ValidatorConfig{MaxTxSizePolicy: 1}, maxTx: true, anyTracked: true},
		{name: "explicit max script size", vcfg: config.ValidatorConfig{MaxScriptSizePolicy: 1}, maxScript: true, anyTracked: true},
		{
			name: "everything pinned means nothing to refresh",
			vcfg: config.ValidatorConfig{MinFeePerKB: 100, MaxTxSizePolicy: 1, MaxScriptSizePolicy: 1},
			fee:  true, maxTx: true, maxScript: true, anyTracked: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{cfg: &config.Config{Validator: tc.vcfg}}
			if got := s.feePinned(); got != tc.fee {
				t.Errorf("feePinned() = %v, want %v", got, tc.fee)
			}
			if got := s.maxTxSizePinned(); got != tc.maxTx {
				t.Errorf("maxTxSizePinned() = %v, want %v", got, tc.maxTx)
			}
			if got := s.maxScriptSizePinned(); got != tc.maxScript {
				t.Errorf("maxScriptSizePinned() = %v, want %v", got, tc.maxScript)
			}
			if got := s.policyTracked(); got != tc.anyTracked {
				t.Errorf("policyTracked() = %v, want %v", got, tc.anyTracked)
			}
		})
	}
}

// TestConfigDefaultsMatchValidatorDefaults guards a coupling that is invisible
// at either site. discoveredLimit floors an observation at the config default,
// while the validator resolves an unset limit to its own default — if the two
// ever drift, a network with no observations would enforce one value and
// GET /policy would advertise another.
func TestConfigDefaultsMatchValidatorDefaults(t *testing.T) {
	v := validator.NewValidator(nil)
	if got := v.MaxTxSizePolicy(); got != config.DefaultValidatorMaxTxSizePolicy {
		t.Errorf("validator default max tx size %d != config default %d",
			got, config.DefaultValidatorMaxTxSizePolicy)
	}
	if got := v.MaxScriptSizePolicy(); got != config.DefaultValidatorMaxScriptSizePolicy {
		t.Errorf("validator default max script size %d != config default %d",
			got, config.DefaultValidatorMaxScriptSizePolicy)
	}
	if got := v.MinFeePerKB(); got != config.DefaultValidatorMinFeePerKB {
		t.Errorf("validator default min fee %d != config default %d",
			got, config.DefaultValidatorMinFeePerKB)
	}
}

// TestDiscoveredFeePerKB is the regression for a production report: /policy
// advertised miningFee 0 sat/kB while every endpoint in /health advertised 100.
//
// The rule is a bare minimum over unauthenticated gossip, so one peer at 0 —
// misconfigured, or a node with nil policy settings, which teranode advertises
// as min_mining_tx_fee=0 — silently disabled fee enforcement for the whole
// instance. The floor is the fee's counterpart to the one discoveredLimit
// applies to the size limits: discovery may track the cheapest node all the way
// to 1 sat/kB, but only an explicit accept_zero_fee may produce a 0 floor.
func TestDiscoveredFeePerKB(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	ttl := 15 * time.Minute

	cases := []struct {
		name     string
		peers    []store.PeerPolicy
		want     uint64
		wantPeer string
	}{
		{
			name: "tracks the cheapest peer",
			peers: []store.PeerPolicy{
				{PeerID: "p1", MiningFeeSatoshis: 500, MiningFeeBytes: 1000, LastSeen: now},
				{PeerID: "p2", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, LastSeen: now},
			},
			want: 100, wantPeer: "p2",
		},
		{
			name: "a single zero-fee peer cannot disable fee enforcement",
			peers: []store.PeerPolicy{
				{PeerID: "honest", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, LastSeen: now},
				{PeerID: "zero", MiningFeeSatoshis: 0, MiningFeeBytes: 1000, LastSeen: now},
			},
			want: minDiscoveredFeePerKB, wantPeer: "zero",
		},
		{
			name: "a whole network at zero still floors at the minimum",
			peers: []store.PeerPolicy{
				{PeerID: "zero", MiningFeeSatoshis: 0, MiningFeeBytes: 1000, LastSeen: now},
			},
			want: minDiscoveredFeePerKB, wantPeer: "zero",
		},
		{
			name: "one satoshi per kB is above the floor and passes through",
			peers: []store.PeerPolicy{
				{PeerID: "cheap", MiningFeeSatoshis: 1, MiningFeeBytes: 1000, LastSeen: now},
			},
			want: 1, wantPeer: "cheap",
		},
		{
			name:  "no observations falls back to the built-in default",
			peers: nil,
			want:  uint64(config.DefaultValidatorMinFeePerKB),
		},
		{
			name: "a stale zero-fee peer is ignored entirely",
			peers: []store.PeerPolicy{
				{PeerID: "honest", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, LastSeen: now},
				{PeerID: "gone", MiningFeeSatoshis: 0, MiningFeeBytes: 1000, LastSeen: now.Add(-24 * time.Hour)},
			},
			want: 100, wantPeer: "honest",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, peerID := discoveredFeePerKB(tc.peers, ttl, now)
			if got != tc.want {
				t.Errorf("discoveredFeePerKB() = %d, want %d", got, tc.want)
			}
			if peerID != tc.wantPeer {
				t.Errorf("cheapest peer = %q, want %q — the log line naming the "+
					"culprit is how an operator explains the floor", peerID, tc.wantPeer)
			}
		})
	}
}

// TestRefreshPolicyOnce_ZeroFeePeerDoesNotZeroTheFloor is the same guarantee
// end to end through the refresher and the validator, since that is where the
// production symptom was visible: GET /policy reads the validator's floor.
func TestRefreshPolicyOnce_ZeroFeePeerDoesNotZeroTheFloor(t *testing.T) {
	s := newPolicyServer(&mockStore{peerPolicies: []store.PeerPolicy{
		freshPolicy("honest", 100, 100_000_000, 500_000),
		freshPolicy("zero", 0, 100_000_000, 500_000),
	}})

	s.refreshPolicyOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != minDiscoveredFeePerKB {
		t.Errorf("intake fee floor = %d, want %d — a zero-fee peer must not "+
			"make arcade accept transactions no node will mine", got, minDiscoveredFeePerKB)
	}
	// The size limits still track normally.
	if got := s.validator.MaxTxSizePolicy(); got != 100_000_000 {
		t.Errorf("MaxTxSizePolicy = %d, want 100000000", got)
	}
}

// TestRefreshPolicyOnce_AcceptZeroFeeStillPinsZero confirms the floor did not
// take away the one legitimate route to a zero-fee intake: an operator opting
// in explicitly, which is what accept_zero_fee is for.
func TestRefreshPolicyOnce_AcceptZeroFeeStillPinsZero(t *testing.T) {
	s := newPolicyServerWithConfig(
		&mockStore{peerPolicies: []store.PeerPolicy{freshPolicy("honest", 100, 0, 0)}},
		config.ValidatorConfig{AcceptZeroFee: true})
	s.validator.SetMinFeePerKB(0) // as app wiring does at construction

	s.refreshPolicyOnce(context.Background())

	if got := s.validator.MinFeePerKB(); got != 0 {
		t.Errorf("intake fee floor = %d, want 0 (accept_zero_fee pins it)", got)
	}
}
