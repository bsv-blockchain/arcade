package store

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// TestPeerPolicyValidate pins what a backend is allowed to persist.
//
// The fee originates in node_status gossip, which is unauthenticated: the peer
// chooses the number. The Postgres and Aerospike backends store it in signed
// 64-bit columns, so an unchecked value above MaxInt64 wraps negative on write
// and reads back as an astronomical uint64 — which GET /policy would then
// advertise as the network's minimum fee. Validate is what makes the narrowing
// in those backends safe, so it is tested on the type rather than once per
// backend.
// testPeerID is the subject of the peer-policy validation tests; file-scoped
// so both tests below can name the same peer.
const testPeerID = "peer-a"

func TestPeerPolicyValidate(t *testing.T) {
	const (
		okSats  = uint64(100)
		okBytes = uint64(1000)
	)

	cases := []struct {
		name    string
		pp      PeerPolicy
		wantErr bool
		// mustSay is a fragment the message has to carry, so an operator
		// reading the log learns which peer and which value were refused.
		mustSay string
	}{
		{
			name: "ordinary advertisement",
			pp:   PeerPolicy{PeerID: testPeerID, Network: "mainnet", MiningFeeSatoshis: okSats, MiningFeeBytes: okBytes},
		},
		{
			name: "zero fee is legitimate — a miner may accept free transactions",
			pp:   PeerPolicy{PeerID: testPeerID, Network: "mainnet", MiningFeeSatoshis: 0, MiningFeeBytes: okBytes},
		},
		{
			name:    "empty peer id",
			pp:      PeerPolicy{Network: "mainnet", MiningFeeSatoshis: okSats, MiningFeeBytes: okBytes},
			wantErr: true,
			mustSay: "empty peer id",
		},
		{
			name:    "zero byte basis would make the rate meaningless",
			pp:      PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: okSats, MiningFeeBytes: 0},
			wantErr: true,
			mustSay: testPeerID,
		},
		{
			name:    "satoshis above the signed-64-bit ceiling",
			pp:      PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: math.MaxInt64 + 1, MiningFeeBytes: okBytes},
			wantErr: true,
			mustSay: testPeerID,
		},
		{
			name:    "byte basis above the signed-64-bit ceiling",
			pp:      PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: okSats, MiningFeeBytes: math.MaxInt64 + 1},
			wantErr: true,
			mustSay: testPeerID,
		},
		{
			name: "exactly at the ceiling is storable",
			pp:   PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: math.MaxInt64, MiningFeeBytes: math.MaxInt64},
		},
		{
			// Zero is the "peer did not advertise" sentinel for the size
			// limits, so — unlike the fee's byte basis — it must validate.
			name: "unadvertised size limits are storable",
			pp:   PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: okSats, MiningFeeBytes: 1000},
		},
		{
			name: "max tx size above the signed-64-bit ceiling",
			pp: PeerPolicy{
				PeerID: testPeerID, MiningFeeSatoshis: okSats, MiningFeeBytes: 1000,
				MaxTxSizePolicy: math.MaxInt64 + 1,
			},
			wantErr: true,
			mustSay: testPeerID,
		},
		{
			name: "max script size above the signed-64-bit ceiling",
			pp: PeerPolicy{
				PeerID: testPeerID, MiningFeeSatoshis: okSats, MiningFeeBytes: 1000,
				MaxScriptSizePolicy: math.MaxInt64 + 1,
			},
			wantErr: true,
			mustSay: testPeerID,
		},
		{
			name: "realistic size limits are storable",
			pp: PeerPolicy{
				PeerID: testPeerID, MiningFeeSatoshis: okSats, MiningFeeBytes: 1000,
				MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.pp.Validate()
			if tc.wantErr != (err != nil) {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil {
				return
			}
			if !errors.Is(err, ErrInvalidPeerPolicy) {
				t.Errorf("error %v does not wrap ErrInvalidPeerPolicy; callers branch on it to tell "+
					"a bad advertisement from a store fault", err)
			}
			if tc.mustSay != "" && !strings.Contains(err.Error(), tc.mustSay) {
				t.Errorf("error = %q, want it to mention %q", err, tc.mustSay)
			}
		})
	}
}

// TestPeerPolicyValidate_CeilingCannotRejectAnHonestFee guards the choice of
// bound. Rejecting a real advertisement would silently drop a peer from the
// GET /policy calculation, so the ceiling has to sit far above anything the
// chain can express: the entire 21-million-BSV supply is ~2.1e15 satoshis,
// which must validate comfortably.
func TestPeerPolicyValidate_CeilingCannotRejectAnHonestFee(t *testing.T) {
	const wholeMoneySupplySatoshis = uint64(21_000_000) * uint64(100_000_000)

	pp := PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: wholeMoneySupplySatoshis, MiningFeeBytes: 1000}
	if err := pp.Validate(); err != nil {
		t.Fatalf("a fee of the entire money supply must still be storable, got %v", err)
	}
	if wholeMoneySupplySatoshis >= maxStorablePolicyValue {
		t.Fatalf("the storable ceiling (%d) is not above the money supply (%d); the bound would be "+
			"rejecting values the chain can actually express", maxStorablePolicyValue, wholeMoneySupplySatoshis)
	}
}

// TestSanitizePolicySizes pins the asymmetry between the fee and the size
// limits. An unstorable fee fails the whole write, because a fee is the reason
// the row exists. An unstorable size must not: the peer's fee observation is
// independently useful, so a peer advertising a garbage tx size has to keep
// counting toward the network minimum rather than silently removing itself
// from it.
func TestSanitizePolicySizes(t *testing.T) {
	cases := []struct {
		name           string
		pp             PeerPolicy
		wantTx         uint64
		wantScript     uint64
		wantDropped    int
		wantStillValid bool
	}{
		{
			name:           "in-range limits are untouched",
			pp:             PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: 100, MiningFeeBytes: 1000, MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000},
			wantTx:         100_000_000,
			wantScript:     500_000,
			wantStillValid: true,
		},
		{
			name:           "unstorable tx size is zeroed, fee survives",
			pp:             PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: 100, MiningFeeBytes: 1000, MaxTxSizePolicy: math.MaxUint64, MaxScriptSizePolicy: 500_000},
			wantTx:         0,
			wantScript:     500_000,
			wantDropped:    1,
			wantStillValid: true,
		},
		{
			name:           "both unstorable",
			pp:             PeerPolicy{PeerID: testPeerID, MiningFeeSatoshis: 100, MiningFeeBytes: 1000, MaxTxSizePolicy: math.MaxUint64, MaxScriptSizePolicy: math.MaxInt64 + 1},
			wantTx:         0,
			wantScript:     0,
			wantDropped:    2,
			wantStillValid: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pp, dropped := tc.pp.SanitizePolicySizes()
			if dropped != tc.wantDropped {
				t.Errorf("dropped = %d, want %d", dropped, tc.wantDropped)
			}
			if pp.MaxTxSizePolicy != tc.wantTx {
				t.Errorf("MaxTxSizePolicy = %d, want %d", pp.MaxTxSizePolicy, tc.wantTx)
			}
			if pp.MaxScriptSizePolicy != tc.wantScript {
				t.Errorf("MaxScriptSizePolicy = %d, want %d", pp.MaxScriptSizePolicy, tc.wantScript)
			}
			if tc.wantStillValid {
				if err := pp.Validate(); err != nil {
					t.Errorf("after sanitizing, the row must be storable so the fee observation "+
						"is not lost; got %v", err)
				}
			}
		})
	}
}
