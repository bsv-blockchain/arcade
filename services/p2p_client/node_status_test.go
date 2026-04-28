package p2p_client

import (
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

func TestValidateURL(t *testing.T) {
	cases := []struct {
		name         string
		raw          string
		allowPrivate bool
		wantErr      bool
		want         string
	}{
		{"public https", "https://public.example.com", false, false, "https://public.example.com"},
		{"public https trailing slash trimmed", "https://public.example.com/", false, false, "https://public.example.com"},
		{"public http", "http://public.example.com:8080", false, false, "http://public.example.com:8080"},
		{"rfc1918 rejected", "http://192.168.5.10:8080", false, true, ""},
		{"rfc1918 allowed with opt-in", "http://192.168.5.10:8080", true, false, "http://192.168.5.10:8080"},
		{"loopback rejected", "http://127.0.0.1:8080", false, true, ""},
		{"loopback allowed with opt-in", "http://127.0.0.1:8080", true, false, "http://127.0.0.1:8080"},
		{"link-local rejected", "http://169.254.1.1", false, true, ""},
		{"ipv6 loopback rejected", "http://[::1]:8080", false, true, ""},
		{"ftp rejected", "ftp://peer.example/", false, true, ""},
		{"file scheme rejected", "file:///etc/passwd", true, true, ""},
		{"empty host rejected", "https://", false, true, ""},
		{"empty string rejected", "", false, true, ""},
		{"dns name resolving privately is treated as public", "http://internal.corp.local", false, false, "http://internal.corp.local"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateURL(tc.raw, tc.allowPrivate)
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
