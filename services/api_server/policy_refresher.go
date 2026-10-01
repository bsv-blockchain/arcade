package api_server

import (
	"context"
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
)

// feePinned reports whether the operator has fixed the intake fee floor. When
// pinned, arcade enforces (and /policy advertises) that exact value and the
// refresher leaves the fee alone. When unpinned (min_fee_per_kb=0 and
// accept_zero_fee=false), arcade tracks the network-observed minimum instead.
func (s *Server) feePinned() bool {
	return s.cfg.Validator.AcceptZeroFee || s.cfg.Validator.MinFeePerKB != 0
}

// maxTxSizePinned and maxScriptSizePinned report whether the operator has fixed
// the corresponding size limit. Zero means "not operator-configured", the same
// sentinel min_fee_per_kb uses, and leaves the limit tracking the network.
func (s *Server) maxTxSizePinned() bool {
	return s.cfg.Validator.MaxTxSizePolicy != 0
}

func (s *Server) maxScriptSizePinned() bool {
	return s.cfg.Validator.MaxScriptSizePolicy != 0
}

// policyTracked reports whether any dimension is left to the network, i.e.
// whether running the refresher can change anything.
func (s *Server) policyTracked() bool {
	return !s.feePinned() || !s.maxTxSizePinned() || !s.maxScriptSizePinned()
}

func (s *Server) observedFeeTTLMs() uint64 {
	if s.cfg.Validator.ObservedFeeTTLMs == 0 {
		return config.DefaultValidatorObservedFeeTTLMs
	}
	return s.cfg.Validator.ObservedFeeTTLMs
}

func (s *Server) observedFeeRefreshMs() uint64 {
	if s.cfg.Validator.ObservedFeeRefreshMs == 0 {
		return config.DefaultValidatorObservedFeeRefreshMs
	}
	return s.cfg.Validator.ObservedFeeRefreshMs
}

// runPolicyRefresher periodically recomputes the transaction policy the network
// will accept — the lowest mining fee and the most permissive size limits
// advertised across peer node_status announcements within the observed-policy
// TTL — and pushes it into the validator so intake enforces it and GET /policy
// reports it (issue #212). Each dimension is skipped if the operator pinned it.
// Bound to the server lifetime ctx; returns when it is cancelled.
func (s *Server) runPolicyRefresher(ctx context.Context) {
	s.refreshPolicyOnce(ctx) // seed immediately so intake reacts before the first tick

	ticker := time.NewTicker(time.Duration(s.observedFeeRefreshMs()) * time.Millisecond) //nolint:gosec // refresh interval in ms fits int64
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshPolicyOnce(ctx)
		}
	}
}

// refreshPolicyOnce reads the observed peer policies and updates the validator
// to the network-derived policy, or to the built-in defaults when nothing fresh
// has been observed. A store read failure leaves the current policy unchanged
// rather than dropping it to the defaults.
func (s *Server) refreshPolicyOnce(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	peers, err := s.store.ListPeerPolicies(rctx, s.cfg.Network)
	if err != nil {
		s.logger.Warn("policy refresher: list peer policies failed, keeping current policy", zap.Error(err))
		return
	}

	ttl := time.Duration(s.observedFeeTTLMs()) * time.Millisecond //nolint:gosec // TTL in ms fits int64
	now := time.Now()

	// Fee: the lowest any peer will accept, so arcade rejects only what no peer
	// would mine. Sizes: the highest any peer will accept, the same rule read
	// the other way round.
	var (
		fee      uint64
		cheapest string // peer that set the floor, for the log line below
	)
	if s.feePinned() {
		fee = s.validator.MinFeePerKB() // pinned at construction; leave it be
	} else {
		fee, cheapest = discoveredFeePerKB(peers, ttl, now)
	}

	maxTxSize := s.cfg.Validator.MaxTxSizePolicy
	if !s.maxTxSizePinned() {
		maxTxSize = discoveredLimit(peers, ttl, now,
			func(p store.PeerPolicy) uint64 { return p.MaxTxSizePolicy },
			config.DefaultValidatorMaxTxSizePolicy)
	}

	maxScriptSize := s.cfg.Validator.MaxScriptSizePolicy
	if !s.maxScriptSizePinned() {
		maxScriptSize = discoveredLimit(peers, ttl, now,
			func(p store.PeerPolicy) uint64 { return p.MaxScriptSizePolicy },
			config.DefaultValidatorMaxScriptSizePolicy)
	}

	prevFee := s.validator.MinFeePerKB()
	prevTxSize := s.validator.MaxTxSizePolicy()
	prevScriptSize := s.validator.MaxScriptSizePolicy()

	// One call, so a combined change costs a single BDK rebuild.
	s.validator.SetNetworkPolicy(fee, maxTxSize, maxScriptSize)

	// Report what the validator actually adopted, not what we asked for: it
	// clamps, and can decline the change outright if the BDK rebuild fails.
	newFee := s.validator.MinFeePerKB()
	newTxSize := s.validator.MaxTxSizePolicy()
	newScriptSize := s.validator.MaxScriptSizePolicy()

	if newFee != prevFee || newTxSize != prevTxSize || newScriptSize != prevScriptSize {
		s.logger.Info("intake policy updated from network observations",
			zap.Uint64("prev_fee_sat_per_kb", prevFee),
			zap.Uint64("new_fee_sat_per_kb", newFee),
			// Which peer set the floor, and how many fresh peers advertised no
			// fee at all. Without these, "why is the floor 1?" can only be
			// answered by querying the store by hand.
			zap.String("cheapest_peer_id", cheapest),
			zap.Int("fee_unadvertised_peers", countUnadvertisedFees(peers, ttl, now)),
			zap.Int("prev_max_tx_size", prevTxSize),
			zap.Int("new_max_tx_size", newTxSize),
			zap.Int("prev_max_script_size", prevScriptSize),
			zap.Int("new_max_script_size", newScriptSize),
			zap.Int("observed_peers", len(peers)))
	}
}

// discoveredLimit resolves one network-tracked size limit: the highest value
// advertised by a fresh peer, floored at arcade's built-in default. Returns the
// default when no peer has advertised one.
//
// The floor is what makes observation safe. Peer announcements are
// unauthenticated gossip, so a hostile or simply misconfigured node advertising
// a tiny limit must not be able to shrink what arcade accepts — observation may
// only ever widen the limit from the built-in default, never narrow it. The
// ceiling is applied downstream by the validator, which caps at the BDK
// consensus limit before building an engine.
func discoveredLimit(peers []store.PeerPolicy, ttl time.Duration, now time.Time,
	pick func(store.PeerPolicy) uint64, floor uint64,
) uint64 {
	observed, ok := highestObservedLimit(peers, ttl, now, pick)
	if !ok || observed < floor {
		return floor
	}
	return observed
}

// discoveredFeePerKB resolves the network-tracked fee floor: the lowest rate a
// fresh peer actually advertised, or the built-in default when no fresh peer
// advertised one at all. It also returns the id of the peer that set the floor,
// so the value is explainable from a log line rather than only by querying the
// store.
func discoveredFeePerKB(peers []store.PeerPolicy, ttl time.Duration, now time.Time) (uint64, string) {
	observed, peerID, ok := lowestObservedFeePerKB(peers, ttl, now)
	if !ok {
		return uint64(config.DefaultValidatorMinFeePerKB), ""
	}
	return observed, peerID
}

// isFreshObservation reports whether a peer row is a live observation: re-heard
// within the TTL.
//
// A row with no LastSeen at all is stale, not fresh. An observation that cannot
// be dated cannot be shown to be current, and the TTL is the only thing that
// stops a departed peer from pinning the network policy forever — every backend
// can produce an undated row (a missing aerospike bin, a zero pebble timestamp,
// a postgres zero-value date), and treating one as perpetually fresh makes it
// immortal. Shared by both aggregators so they cannot drift on the question.
func isFreshObservation(p store.PeerPolicy, cutoff time.Time) bool {
	return !p.LastSeen.Before(cutoff)
}

// countUnadvertisedFees reports how many fresh peers advertised no fee at all.
// It exists for the log line: it is the number that explains why the floor is
// what it is, and its absence is what made a production report of a 1 sat/kB
// floor unanswerable without a store query.
func countUnadvertisedFees(peers []store.PeerPolicy, ttl time.Duration, now time.Time) int {
	cutoff := now.Add(-ttl)
	var n int
	for _, p := range peers {
		if isFreshObservation(p, cutoff) && p.MiningFeeSatoshis == 0 {
			n++
		}
	}
	return n
}

// lowestObservedFeePerKB returns the minimum mining fee rate (in satoshis per
// 1000 bytes) advertised by peers re-heard within ttl, and the id of the peer
// advertising it. ok is false when no fresh observation exists. A peer's rate
// is normalized to sat/kB using ceil division so a non-1000 byte basis never
// rounds the enforced floor *below* what the peer requires (e.g. 1 sat / 1001
// bytes must map to 1 sat/kB, not 0, or arcade would accept fee=0 that no node
// would).
func lowestObservedFeePerKB(peers []store.PeerPolicy, ttl time.Duration, now time.Time) (uint64, string, bool) {
	cutoff := now.Add(-ttl)
	var (
		best   uint64
		peerID string
		found  bool
	)
	for _, p := range peers {
		if p.MiningFeeSatoshis == 0 {
			// 0 satoshis is not an observation of a node that mines for free,
			// it is the absence of an advertisement: teranode reports
			// min_mining_tx_fee=0 whenever its policy settings are nil, and the
			// legacy BSV/kB path converts that 0.0 straight through. Counting
			// it as a real rate makes one silent non-advertisement the network
			// minimum for the whole instance — which is what production did,
			// first as a 0 sat/kB floor and then, once the 0 was clamped rather
			// than discarded, as 1 sat/kB while every peer in /health required
			// 100. This is the same sentinel highestObservedLimit applies to
			// the size limits. A network that really does mine for free is an
			// operator decision: that is what accept_zero_fee is for.
			continue
		}
		if p.MiningFeeBytes == 0 {
			continue // avoid divide-by-zero on a malformed row
		}
		if !isFreshObservation(p, cutoff) {
			continue // peer not re-heard within TTL
		}
		perKB := ceilFeePerKB(p.MiningFeeSatoshis, p.MiningFeeBytes)
		if !found || perKB < best {
			best = perKB
			peerID = p.PeerID
			found = true
		}
	}
	return best, peerID, found
}

// highestObservedLimit returns the maximum value pick reports across peers
// re-heard within ttl. ok is false when no fresh peer advertised one.
//
// Taking the maximum is the size-limit reading of the same rule
// lowestObservedFeePerKB applies to the fee: a transaction is pre-rejected only
// when *no* peer on the network would accept it. Rows where pick returns 0 are
// skipped — 0 means the peer did not advertise a limit (an older teranode with
// no fee_policy, or a value dropped as unstorable), not that it accepts
// nothing.
func highestObservedLimit(peers []store.PeerPolicy, ttl time.Duration, now time.Time,
	pick func(store.PeerPolicy) uint64,
) (uint64, bool) {
	cutoff := now.Add(-ttl)
	var (
		best  uint64
		found bool
	)
	for _, p := range peers {
		v := pick(p)
		if v == 0 {
			continue // peer did not advertise this limit
		}
		if !isFreshObservation(p, cutoff) {
			continue // peer not re-heard within TTL
		}
		if !found || v > best {
			best = v
			found = true
		}
	}
	return best, found
}

// ceilFeePerKB computes ceil(satoshis*1000 / bytes), rounding up so the derived
// sat/kB floor is never lower than the peer's advertised rate. It is
// overflow-safe for any input: an absurdly large advertised fee saturates to
// MaxUint64, so it reads as "very high" and can never be wrongly selected as
// the network minimum. bytes must be non-zero (checked by callers).
func ceilFeePerKB(satoshis, bytes uint64) uint64 {
	const scale = 1000
	whole := satoshis / bytes
	rem := satoshis % bytes // < bytes
	if whole > math.MaxUint64/scale || rem > math.MaxUint64/scale {
		return math.MaxUint64
	}
	base := whole * scale
	remScaled := rem * scale
	add := remScaled / bytes
	if remScaled%bytes != 0 {
		add++ // round up
	}
	if base > math.MaxUint64-add {
		return math.MaxUint64
	}
	return base + add
}
