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
// fee refresher does not run. When unpinned (min_fee_per_kb=0 and
// accept_zero_fee=false), arcade tracks the network-observed minimum instead.
func (s *Server) feePinned() bool {
	return s.cfg.Validator.AcceptZeroFee || s.cfg.Validator.MinFeePerKB != 0
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

// runFeeRefresher periodically recomputes the lowest mining fee the network
// will accept — the minimum advertised across peer node_status announcements
// within the observed-fee TTL — and pushes it into the validator so intake
// enforces it (issue #212). It runs only when the operator has not pinned a
// fee. Bound to the server lifetime ctx; returns when it is cancelled.
func (s *Server) runFeeRefresher(ctx context.Context) {
	s.refreshFeeOnce(ctx) // seed immediately so intake reacts before the first tick

	ticker := time.NewTicker(time.Duration(s.observedFeeRefreshMs()) * time.Millisecond) //nolint:gosec // refresh interval in ms fits int64
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshFeeOnce(ctx)
		}
	}
}

// refreshFeeOnce reads the observed peer policies and updates the validator's
// fee floor to the network minimum, or to the built-in default when nothing
// fresh has been observed. A store read failure leaves the current floor
// unchanged rather than dropping it to the default.
func (s *Server) refreshFeeOnce(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	peers, err := s.store.ListPeerPolicies(rctx, s.cfg.Network)
	if err != nil {
		s.logger.Warn("fee refresher: list peer policies failed, keeping current floor", zap.Error(err))
		return
	}

	floor := uint64(config.DefaultValidatorMinFeePerKB)
	if observed, ok := lowestObservedFeePerKB(peers, time.Duration(s.observedFeeTTLMs())*time.Millisecond, time.Now()); ok { //nolint:gosec // TTL in ms fits int64
		floor = observed
	}

	prev := s.validator.MinFeePerKB()
	s.validator.SetMinFeePerKB(floor)
	if floor != prev {
		s.logger.Info("intake fee floor updated from network observations",
			zap.Uint64("prev_sat_per_kb", prev),
			zap.Uint64("new_sat_per_kb", floor),
			zap.Int("observed_peers", len(peers)))
	}
}

// lowestObservedFeePerKB returns the minimum mining fee rate (in satoshis per
// 1000 bytes) advertised by peers re-heard within ttl. ok is false when no
// fresh observation exists. A peer's rate is normalized to sat/kB using ceil
// division so a non-1000 byte basis never rounds the enforced floor *below*
// what the peer requires (e.g. 1 sat / 1001 bytes must map to 1 sat/kB, not 0,
// or arcade would accept fee=0 that no node would).
func lowestObservedFeePerKB(peers []store.PeerPolicy, ttl time.Duration, now time.Time) (uint64, bool) {
	cutoff := now.Add(-ttl)
	var (
		best  uint64
		found bool
	)
	for _, p := range peers {
		if p.MiningFeeBytes == 0 {
			continue // avoid divide-by-zero on a malformed row
		}
		if !p.LastSeen.IsZero() && p.LastSeen.Before(cutoff) {
			continue // peer not re-heard within TTL
		}
		perKB := ceilFeePerKB(p.MiningFeeSatoshis, p.MiningFeeBytes)
		if !found || perKB < best {
			best = perKB
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
