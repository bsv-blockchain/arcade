package p2p_client

import (
	"fmt"
	"math"
	"net"
	"testing"

	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/store"
)

func TestPickDatahubURL(t *testing.T) {
	cases := []struct {
		name string
		msg  teranodep2p.NodeStatusMessage
		want string
	}{
		{"both set prefers propagation", teranodep2p.NodeStatusMessage{BaseURL: "https://base", PropagationURL: "https://prop"}, "https://prop"},
		{"only base", teranodep2p.NodeStatusMessage{BaseURL: "https://base"}, "https://base"},
		{"only propagation", teranodep2p.NodeStatusMessage{PropagationURL: "https://prop"}, "https://prop"},
		{"both empty", teranodep2p.NodeStatusMessage{}, ""},
		{"propagation whitespace falls back to base", teranodep2p.NodeStatusMessage{BaseURL: "https://base", PropagationURL: "   "}, "https://base"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pickDatahubURL(tc.msg)
			if got != tc.want {
				t.Errorf("pickDatahubURL = %q, want %q", got, tc.want)
			}
		})
	}
}

// Lookup stubs. validateURL only resolves non-literal hostnames, so
// IP-literal cases use noLookup to assert DNS is never consulted for them.
func publicLookup(string) ([]net.IP, error)  { return []net.IP{net.ParseIP("93.184.216.34")}, nil }
func privateLookup(string) ([]net.IP, error) { return []net.IP{net.ParseIP("10.0.0.5")}, nil }
func multiLookup(string) ([]net.IP, error) {
	return []net.IP{net.ParseIP("93.184.216.34"), net.ParseIP("10.0.0.5")}, nil
}

func unresolvableLookup(host string) ([]net.IP, error) {
	return nil, fmt.Errorf("lookup %s: no such host", host)
}

func TestValidateURL(t *testing.T) {
	// noLookup fails the test if DNS is consulted — used for IP-literal and
	// syntactically-invalid cases that must be decided without resolution.
	noLookup := func(host string) ([]net.IP, error) {
		t.Fatalf("unexpected DNS lookup for %q", host)
		return nil, nil
	}

	cases := []struct {
		name         string
		raw          string
		allowPrivate bool
		lookup       func(string) ([]net.IP, error)
		wantErr      bool
		want         string
	}{
		{"public https", "https://public.example.com", false, publicLookup, false, "https://public.example.com"},
		{"public https trailing slash trimmed", "https://public.example.com/", false, publicLookup, false, "https://public.example.com"},
		{"public http", "http://public.example.com:8080", false, publicLookup, false, "http://public.example.com:8080"},
		{"rfc1918 rejected", "http://192.168.5.10:8080", false, noLookup, true, ""},
		{"rfc1918 allowed with opt-in", "http://192.168.5.10:8080", true, noLookup, false, "http://192.168.5.10:8080"},
		{"loopback rejected", "http://127.0.0.1:8080", false, noLookup, true, ""},
		{"loopback allowed with opt-in", "http://127.0.0.1:8080", true, noLookup, false, "http://127.0.0.1:8080"},
		{"link-local rejected", "http://169.254.1.1", false, noLookup, true, ""},
		{"ipv6 loopback rejected", "http://[::1]:8080", false, noLookup, true, ""},
		{"ftp rejected", "ftp://peer.example/", false, noLookup, true, ""},
		{"file scheme rejected", "file:///etc/passwd", true, noLookup, true, ""},
		{"empty host rejected", "https://", false, noLookup, true, ""},
		{"empty string rejected", "", false, noLookup, true, ""},
		{"userinfo rejected", "https://user@public.example.com", false, noLookup, true, ""},

		// DNS-resolution cases — the gap the old validator had. A hostname
		// resolving privately used to be "treated as public"; now every
		// resolved IP is checked.
		{"dns name resolving privately rejected", "http://internal.corp.local", false, privateLookup, true, ""},
		{"dns name resolving privately allowed with opt-in", "http://internal.corp.local", true, privateLookup, false, "http://internal.corp.local"},
		{"multi-IP with one private rejected", "https://mixed.example.com", false, multiLookup, true, ""},

		// The headline production bug: a peer announcing its own
		// cluster-internal service name. Unresolvable from here — rejected
		// regardless of allow_private_urls.
		{"unresolvable hostname rejected", "http://asset:8090/api/v1", false, unresolvableLookup, true, ""},
		{"unresolvable hostname rejected even with opt-in", "http://asset:8090/api/v1", true, unresolvableLookup, true, ""},

		// Metadata deny-list holds even with the private opt-in.
		{"metadata hostname rejected with opt-in", "http://metadata.google.internal/computeMetadata", true, publicLookup, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateURL(tc.raw, tc.allowPrivate, tc.lookup)
			if tc.wantErr {
				if err == nil {
					t.Errorf("validateURL(%q) = %q, nil — want error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateURL(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("validateURL(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestEndpointPolicyFrom covers what gets recorded against a peer's datahub URL
// registration and surfaced per endpoint by GET /health.
//
// Unlike recordPeerPolicy, which feeds the network-wide policy arcade enforces
// and so keeps only values it can compute with, this path records what the node
// advertised verbatim — it answers "what will this endpoint accept". The one
// thing it will not record is a value too large to store faithfully, since
// node_status is unauthenticated and a wrapped value would be reported to
// operators as that node's real policy.
func TestEndpointPolicyFrom(t *testing.T) {
	feePolicy := &teranodep2p.FeePolicy{
		MiningFee:               teranodep2p.FeeAmount{Satoshis: 100, Bytes: 1000},
		MaxTxSizePolicy:         100_000_000,
		MaxScriptSizePolicy:     500_000,
		MaxTxSigopsCountsPolicy: 4_294_967_295,
	}
	legacyFee := 0.0000005 // BSV/kB -> 50 sat per 1000 bytes
	nan := math.NaN()
	absurd := 1e30

	cases := []struct {
		name string
		msg  teranodep2p.NodeStatusMessage
		want *store.EndpointPolicy
	}{
		{
			name: "structured fee policy is recorded whole",
			msg:  teranodep2p.NodeStatusMessage{FeePolicy: feePolicy},
			want: &store.EndpointPolicy{
				MiningFeeSatoshis: 100, MiningFeeBytes: 1000,
				MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000,
				MaxTxSigopsCountsPolicy: 4_294_967_295,
			},
		},
		{
			name: "legacy peer yields a fee and no size limits",
			msg:  teranodep2p.NodeStatusMessage{MinMiningTxFee: &legacyFee},
			want: &store.EndpointPolicy{MiningFeeSatoshis: 50, MiningFeeBytes: 1000},
		},
		{
			name: "peer advertising nothing yields no policy",
			msg:  teranodep2p.NodeStatusMessage{BaseURL: testPeerURL},
			want: nil,
		},
		{
			name: "unstorable size drops the whole policy",
			msg: teranodep2p.NodeStatusMessage{FeePolicy: &teranodep2p.FeePolicy{
				MiningFee:       teranodep2p.FeeAmount{Satoshis: 100, Bytes: 1000},
				MaxTxSizePolicy: math.MaxUint64,
			}},
			want: nil,
		},
		{
			name: "malformed legacy fee yields no policy",
			msg:  teranodep2p.NodeStatusMessage{MinMiningTxFee: &nan},
			want: nil,
		},
		{
			name: "absurd legacy fee yields no policy",
			msg:  teranodep2p.NodeStatusMessage{MinMiningTxFee: &absurd},
			want: nil,
		},
		{
			// A node that genuinely accepts free transactions and imposes no
			// size limits advertises zeros. They are real values, and the nil
			// pointer — not a zero struct — is what means "advertised none".
			name: "an all-zero advertisement is a policy, not absence",
			msg:  teranodep2p.NodeStatusMessage{FeePolicy: &teranodep2p.FeePolicy{}},
			want: &store.EndpointPolicy{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := endpointPolicyFrom(tc.msg)
			switch {
			case tc.want == nil && got != nil:
				t.Fatalf("endpointPolicyFrom() = %+v, want nil", *got)
			case tc.want != nil && got == nil:
				t.Fatalf("endpointPolicyFrom() = nil, want %+v", *tc.want)
			case tc.want != nil && *got != *tc.want:
				t.Errorf("endpointPolicyFrom() = %+v, want %+v", *got, *tc.want)
			}
		})
	}
}

// TestLegacyMiningFeeSatPerKB pins the shared untrusted-float guard that both
// recordPeerPolicy and endpointPolicyFrom depend on, so the two cannot drift on
// what counts as an implausible fee.
func TestLegacyMiningFeeSatPerKB(t *testing.T) {
	ok := func(v float64) *float64 { return &v }

	cases := []struct {
		name  string
		fee   *float64
		want  uint64
		wantK bool
	}{
		{name: "nil", fee: nil, wantK: false},
		{name: "ordinary", fee: ok(0.0000005), want: 50, wantK: true},
		{name: "zero fee is legitimate", fee: ok(0), want: 0, wantK: true},
		{name: "NaN", fee: ok(math.NaN()), wantK: false},
		{name: "positive infinity", fee: ok(math.Inf(1)), wantK: false},
		{name: "negative", fee: ok(-0.0001), wantK: false},
		{name: "absurd magnitude", fee: ok(1e30), wantK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotOK := legacyMiningFeeSatPerKB(tc.fee)
			if gotOK != tc.wantK {
				t.Fatalf("ok = %v, want %v", gotOK, tc.wantK)
			}
			if gotOK && got != tc.want {
				t.Errorf("got %d sat/kB, want %d", got, tc.want)
			}
		})
	}
}
