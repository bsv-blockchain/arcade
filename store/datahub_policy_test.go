package store

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// TestEndpointPolicyValidate pins what a backend may persist against a datahub
// URL registration.
//
// The values come from node_status gossip, which is unauthenticated: the peer
// chooses them. Postgres and Aerospike store them in signed 64-bit columns, so
// an unchecked value above MaxInt64 wraps negative on write and reads back as
// an astronomical uint64 — which GET /health would then report as that node's
// advertised policy.
func TestEndpointPolicyValidate(t *testing.T) {
	const ok = uint64(500_000)

	cases := []struct {
		name    string
		ep      EndpointPolicy
		wantErr bool
		// mustSay names the field, so an operator reading the log learns which
		// value was refused rather than just that something was.
		mustSay string
	}{
		{
			name: "ordinary advertisement",
			ep: EndpointPolicy{
				MiningFeeSatoshis: 100, MiningFeeBytes: 1000,
				MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: ok,
				MaxTxSigopsCountsPolicy: 4_294_967_295,
			},
		},
		{
			// A node advertising nothing but a fee is a legitimate legacy peer,
			// and every zero here is a real value rather than a sentinel — the
			// nil *EndpointPolicy carries "advertised none".
			name: "all zero is storable",
			ep:   EndpointPolicy{},
		},
		{
			name:    "mining fee satoshis above the signed-64-bit ceiling",
			ep:      EndpointPolicy{MiningFeeSatoshis: math.MaxInt64 + 1},
			wantErr: true,
			mustSay: "mining fee satoshis",
		},
		{
			name:    "mining fee byte basis above the ceiling",
			ep:      EndpointPolicy{MiningFeeBytes: math.MaxInt64 + 1},
			wantErr: true,
			mustSay: "mining fee byte basis",
		},
		{
			name:    "max tx size above the ceiling",
			ep:      EndpointPolicy{MaxTxSizePolicy: math.MaxUint64},
			wantErr: true,
			mustSay: "max tx size",
		},
		{
			name:    "max script size above the ceiling",
			ep:      EndpointPolicy{MaxScriptSizePolicy: math.MaxInt64 + 1},
			wantErr: true,
			mustSay: "max script size",
		},
		{
			name:    "max sigops above the ceiling",
			ep:      EndpointPolicy{MaxTxSigopsCountsPolicy: math.MaxInt64 + 1},
			wantErr: true,
			mustSay: "max tx sigops count",
		},
		{
			name: "exactly at the ceiling is storable",
			ep: EndpointPolicy{
				MiningFeeSatoshis: math.MaxInt64, MiningFeeBytes: math.MaxInt64,
				MaxTxSizePolicy: math.MaxInt64, MaxScriptSizePolicy: math.MaxInt64,
				MaxTxSigopsCountsPolicy: math.MaxInt64,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ep.Validate()
			if tc.wantErr != (err != nil) {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil {
				return
			}
			if !errors.Is(err, ErrInvalidEndpointPolicy) {
				t.Errorf("error %v does not wrap ErrInvalidEndpointPolicy", err)
			}
			if !strings.Contains(err.Error(), tc.mustSay) {
				t.Errorf("error = %q, want it to name the field %q", err, tc.mustSay)
			}
		})
	}
}

// TestEndpointPolicyValidate_CeilingCannotRejectARealPolicy guards the choice
// of bound. Refusing a genuine advertisement would silently blank an endpoint's
// policy in GET /health, so the ceiling has to sit far above anything a node
// can meaningfully configure: teranode's own excessiveblocksize default is 4
// GiB, and MaxInt64 is some two billion times larger.
func TestEndpointPolicyValidate_CeilingCannotRejectARealPolicy(t *testing.T) {
	const fourGiB = uint64(4) * 1024 * 1024 * 1024

	ep := EndpointPolicy{
		MiningFeeSatoshis: 100, MiningFeeBytes: 1000,
		MaxTxSizePolicy: fourGiB, MaxScriptSizePolicy: fourGiB,
		MaxTxSigopsCountsPolicy: 4_294_967_295,
	}
	if err := ep.Validate(); err != nil {
		t.Fatalf("a whole-block-sized limit must still be storable, got %v", err)
	}
	if fourGiB >= maxStorablePolicyValue {
		t.Fatalf("the storable ceiling (%d) is not above a realistic block size (%d)",
			maxStorablePolicyValue, fourGiB)
	}
}
