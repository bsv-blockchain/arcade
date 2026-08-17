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
	if wholeMoneySupplySatoshis >= maxStorableFee {
		t.Fatalf("the storable ceiling (%d) is not above the money supply (%d); the bound would be "+
			"rejecting values the chain can actually express", maxStorableFee, wholeMoneySupplySatoshis)
	}
}
