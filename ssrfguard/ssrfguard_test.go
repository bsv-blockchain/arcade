package ssrfguard

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestIsBlockedIP(t *testing.T) {
	cases := []struct {
		name         string
		ip           string
		allowPrivate bool
		blocked      bool
	}{
		{"loopback v4", "127.0.0.1", false, true},
		{"loopback v4 random", "127.10.20.30", false, true},
		{"loopback v6", "::1", false, true},
		{"link-local v4 metadata", "169.254.169.254", false, true},
		{"link-local v4 generic", "169.254.0.1", false, true},
		{"link-local v6", "fe80::1", false, true},
		{"rfc1918 10/8", "10.0.0.1", false, true},
		{"rfc1918 172.16/12", "172.16.0.1", false, true},
		{"rfc1918 172.31/12 edge", "172.31.255.255", false, true},
		{"rfc1918 192.168/16", "192.168.1.1", false, true},
		{"unspecified v4", "0.0.0.0", false, true},
		{"unspecified v6", "::", false, true},
		{"multicast v4", "224.0.0.1", false, true},
		{"multicast v6", "ff02::1", false, true},
		{"unique-local v6", "fc00::1", false, true},
		{"unique-local v6 fd", "fd12:3456:789a::1", false, true},
		{"cgnat 100.64/10 low", "100.64.0.1", false, true},
		{"cgnat 100.64/10 high", "100.127.255.254", false, true},
		{"just below cgnat", "100.63.255.255", false, false},
		{"just above cgnat", "100.128.0.1", false, false},
		{"cgnat (allowPrivate)", "100.64.0.1", true, false},
		{"public v4", "8.8.8.8", false, false},
		{"public v6", "2606:4700:4700::1111", false, false},
		{"public v4 (allowPrivate)", "8.8.8.8", true, false},
		// allowPrivate flips RFC1918/loopback/link-local but NOT
		// unspecified/multicast which are never reachable destinations.
		{"loopback (allowPrivate)", "127.0.0.1", true, false},
		{"rfc1918 (allowPrivate)", "10.0.0.1", true, false},
		{"link-local (allowPrivate)", "169.254.169.254", true, false},
		{"unspecified (allowPrivate)", "0.0.0.0", true, true},
		{"multicast (allowPrivate)", "224.0.0.1", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("could not parse %q as IP", tc.ip)
			}
			got := IsBlockedIP(ip, tc.allowPrivate)
			if got != tc.blocked {
				t.Fatalf("IsBlockedIP(%s, allowPrivate=%v) = %v, want %v", tc.ip, tc.allowPrivate, got, tc.blocked)
			}
		})
	}
}

func TestIsBlockedIP_NilIP(t *testing.T) {
	if !IsBlockedIP(nil, false) {
		t.Fatal("nil IP should be blocked")
	}
	if !IsBlockedIP(nil, true) {
		t.Fatal("nil IP should be blocked even when allowPrivate=true")
	}
}

func TestIsBlockedHostname(t *testing.T) {
	cases := []struct {
		host    string
		blocked bool
	}{
		{"metadata.google.internal", true},
		{"METADATA.GOOGLE.INTERNAL", true},
		{"metadata", true},
		{"169.254.169.254", true},
		{"example.com", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			got := IsBlockedHostname(tc.host)
			if got != tc.blocked {
				t.Fatalf("IsBlockedHostname(%q) = %v, want %v", tc.host, got, tc.blocked)
			}
		})
	}
}

// stubLookup builds a fake DNS function for tests.
func stubLookup(m map[string][]string) func(string) ([]net.IP, error) {
	return func(host string) ([]net.IP, error) {
		ips, ok := m[host]
		if !ok {
			return nil, fmt.Errorf("no such host: %s", host)
		}
		out := make([]net.IP, 0, len(ips))
		for _, s := range ips {
			out = append(out, net.ParseIP(s))
		}
		return out, nil
	}
}

func TestValidateURL_AcceptsPublic(t *testing.T) {
	lookup := stubLookup(map[string][]string{
		"example.com": {"93.184.216.34"},
	})
	if err := ValidateURL("https://example.com/foo", false, lookup); err != nil {
		t.Fatalf("expected accept, got %v", err)
	}
}

func TestValidateURL_RejectsBadScheme(t *testing.T) {
	cases := []string{
		"file:///etc/passwd",
		"gopher://evil.example.com/",
		"ftp://example.com/",
		"javascript:alert(1)",
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			err := ValidateURL(raw, false, nil)
			if !errors.Is(err, ErrInvalidURL) {
				t.Fatalf("expected ErrInvalidURL, got %v", err)
			}
		})
	}
}

func TestValidateURL_RejectsIPLiterals(t *testing.T) {
	cases := []string{
		"http://127.0.0.1/foo",
		"http://10.0.0.1/foo",
		"http://192.168.1.1/foo",
		"http://172.16.0.1/foo",
		"http://172.31.255.255/foo",
		"http://169.254.169.254/latest/meta-data/",
		"http://0.0.0.0/foo",
		"http://[::1]/foo",
		"http://[fe80::1]/foo",
		"http://[fc00::1]/foo",
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			err := ValidateURL(raw, false, nil)
			if !errors.Is(err, ErrBlockedAddress) {
				t.Fatalf("expected ErrBlockedAddress, got %v", err)
			}
		})
	}
}

func TestValidateURL_RejectsResolvedToPrivate(t *testing.T) {
	lookup := stubLookup(map[string][]string{
		"sneaky.example.com": {"10.0.0.42"},
	})
	err := ValidateURL("https://sneaky.example.com/foo", false, lookup)
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress for hostname resolving to RFC1918, got %v", err)
	}
}

func TestValidateURL_RejectsAnyOfMultiResolved(t *testing.T) {
	// Attacker DNS returns one public + one private; must reject.
	lookup := stubLookup(map[string][]string{
		"mixed.example.com": {"8.8.8.8", "10.0.0.1"},
	})
	err := ValidateURL("https://mixed.example.com/foo", false, lookup)
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress when any resolved IP is private, got %v", err)
	}
}

func TestValidateURL_RejectsUserinfo(t *testing.T) {
	// Even though Go's url.Parse correctly extracts hostname=127.0.0.1,
	// we explicitly reject userinfo to dodge downstream parser quirks.
	err := ValidateURL("https://example.com:80@127.0.0.1/foo", false, nil)
	if !errors.Is(err, ErrInvalidURL) && !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected rejection of userinfo URL, got %v", err)
	}
}

func TestValidateURL_RejectsMetadataHostname(t *testing.T) {
	lookup := stubLookup(map[string][]string{
		"metadata.google.internal": {"8.8.8.8"}, // Even if it "resolves" public.
	})
	err := ValidateURL("https://metadata.google.internal/computeMetadata/v1/", false, lookup)
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress for metadata hostname, got %v", err)
	}
}

func TestValidateURL_AllowPrivateOptIn(t *testing.T) {
	lookup := stubLookup(map[string][]string{
		"internal.example": {"10.0.0.1"},
	})
	// allowPrivate=true: same input now passes.
	if err := ValidateURL("https://internal.example/foo", true, lookup); err != nil {
		t.Fatalf("expected accept with allowPrivate=true, got %v", err)
	}
	if err := ValidateURL("http://127.0.0.1:8080/cb", true, nil); err != nil {
		t.Fatalf("expected accept with allowPrivate=true, got %v", err)
	}
}

func TestValidateURL_DNSFailureIsValidationError(t *testing.T) {
	lookup := stubLookup(map[string][]string{}) // empty = always errors
	err := ValidateURL("https://nonexistent.example/", false, lookup)
	if !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL on DNS failure, got %v", err)
	}
}

func TestValidateURL_EmptyAndMissingPieces(t *testing.T) {
	if err := ValidateURL("", false, nil); !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL for empty, got %v", err)
	}
	if err := ValidateURL("https:///foo", false, nil); !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL for missing host, got %v", err)
	}
	// url.Parse rejects this with a parse error.
	if err := ValidateURL("http://[::1", false, nil); !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL for malformed IPv6 literal, got %v", err)
	}
}

func TestCheckDialAddress(t *testing.T) {
	cases := []struct {
		name         string
		address      string
		allowPrivate bool
		blocked      bool
	}{
		{"loopback", "127.0.0.1:8080", false, true},
		{"loopback v6", "[::1]:443", false, true},
		{"private", "10.0.0.1:80", false, true},
		{"link-local", "169.254.169.254:80", false, true},
		{"public", "93.184.216.34:443", false, false},
		{"loopback (allowPrivate)", "127.0.0.1:8080", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckDialAddress(tc.address, tc.allowPrivate)
			if !tc.blocked {
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected block, got nil")
			}
			if !errors.Is(err, ErrBlockedAddress) {
				t.Fatalf("expected ErrBlockedAddress, got %v", err)
			}
			meaningful := strings.Contains(err.Error(), "refusing") ||
				strings.Contains(err.Error(), "blocked") ||
				strings.Contains(err.Error(), "is not an IP") ||
				strings.Contains(err.Error(), "cannot parse")
			if !meaningful {
				t.Fatalf("expected meaningful message, got %v", err)
			}
		})
	}
}

func TestCheckDialAddress_Malformed(t *testing.T) {
	if err := CheckDialAddress("not-a-real-address", false); !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress, got %v", err)
	}
	// Hostname rather than IP — Dialer.Control always receives an IP, so a
	// non-IP host is anomalous and must fail closed.
	if err := CheckDialAddress("example.com:80", false); !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress for non-IP host, got %v", err)
	}
}
