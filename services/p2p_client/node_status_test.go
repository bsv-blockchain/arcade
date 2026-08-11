package p2p_client

import (
	"fmt"
	"net"
	"testing"

	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"
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
