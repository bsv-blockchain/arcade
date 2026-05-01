package callbackurl

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestValidateURL exercises every IP class the registration-time guard
// is supposed to reject, plus the obvious wins (https://example.com and
// the allow-private opt-in).
func TestValidateURL(t *testing.T) {
	cases := []struct {
		name         string
		raw          string
		allowPrivate bool
		wantErr      bool
	}{
		// Accept paths.
		{"public https accepted", "https://example.com/foo", false, false},
		{"public http accepted", "http://example.com:8080/cb", false, false},
		{"dns name treated as public at validation", "http://internal.corp.local/cb", false, false},
		// Reject paths — IP-class.
		{"loopback v4 rejected", "http://127.0.0.1/cb", false, true},
		{"loopback v4 alt", "http://127.5.6.7/cb", false, true},
		{"loopback v6 rejected", "http://[::1]/cb", false, true},
		{"unspecified v4 rejected", "http://0.0.0.0/cb", false, true},
		{"unspecified v6 rejected", "http://[::]/cb", false, true},
		{"metadata 169.254.169.254 rejected", "http://169.254.169.254/latest/meta-data/", false, true},
		{"link-local v4 rejected", "http://169.254.1.1/cb", false, true},
		{"link-local v6 rejected", "http://[fe80::1]/cb", false, true},
		{"rfc1918 10.x rejected", "http://10.0.0.5/cb", false, true},
		{"rfc1918 172.16 rejected", "http://172.16.0.1/cb", false, true},
		{"rfc1918 192.168 rejected", "http://192.168.1.1/cb", false, true},
		{"unique local v6 rejected", "http://[fc00::1]/cb", false, true},
		{"ipv4-mapped loopback rejected", "http://[::ffff:127.0.0.1]/cb", false, true},
		// Reject paths — scheme.
		{"ftp scheme rejected", "ftp://example.com/cb", false, true},
		{"file scheme rejected", "file:///etc/passwd", false, true},
		{"empty scheme rejected", "example.com/cb", false, true},
		// Reject paths — structural.
		{"empty url rejected", "", false, true},
		{"whitespace-only rejected", "    ", false, true},
		{"missing host rejected", "http://", false, true},
		// Allow-private opt-in.
		{"loopback accepted with opt-in", "http://127.0.0.1/cb", true, false},
		{"rfc1918 accepted with opt-in", "http://192.168.1.1/cb", true, false},
		{"metadata accepted with opt-in", "http://169.254.169.254/", true, false},
		{"file scheme still rejected with opt-in", "file:///etc/passwd", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateURL(tc.raw, tc.allowPrivate)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ValidateURL(%q, allowPrivate=%v) = nil, want error", tc.raw, tc.allowPrivate)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateURL(%q, allowPrivate=%v) unexpected error: %v", tc.raw, tc.allowPrivate, err)
			}
		})
	}
}

// TestIsBlockedIP covers the predicate directly so callers can rely on
// it standalone (e.g. an operator-friendly validator that takes a parsed
// net.IP).
func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "127.255.255.255", "::1",
		"0.0.0.0", "::",
		"169.254.169.254", "169.254.1.1",
		"fe80::1",
		"10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.0.1",
		"fc00::1", "fd00::1",
		"224.0.0.1", "ff02::1",
	}
	for _, s := range blocked {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("test bug: %q does not parse", s)
		}
		if !IsBlockedIP(ip) {
			t.Errorf("IsBlockedIP(%s) = false, want true", s)
		}
	}
	allowed := []string{
		"8.8.8.8", "1.1.1.1", "203.0.113.5",
		"2606:4700:4700::1111",
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("test bug: %q does not parse", s)
		}
		if IsBlockedIP(ip) {
			t.Errorf("IsBlockedIP(%s) = true, want false", s)
		}
	}
	if IsBlockedIP(nil) {
		t.Error("IsBlockedIP(nil) = true, want false")
	}
}

// TestDialControlBlocksLoopback wires DialControl into a live
// http.Transport and confirms that a request to an httptest.Server
// listening on 127.0.0.1 is refused at the dial step. This is the
// dial-time layer that catches DNS rebinding — even if a registered
// hostname survives ValidateURL, the dialer still says no.
func TestDialControlBlocksLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: time.Second,
				Control: DialControl(false),
			}).DialContext,
		},
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("expected dial to be refused for %s, got status %d", srv.URL, resp.StatusCode)
	}
	if !strings.Contains(err.Error(), "blocked") && !errors.Is(err, ErrBlockedHost) {
		t.Errorf("expected error to mention blocked host, got: %v", err)
	}
}

// TestDialControlAllowPrivate verifies the opt-in path: with
// allowPrivate=true, a request to a 127.0.0.1 listener succeeds.
func TestDialControlAllowPrivate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: time.Second,
				Control: DialControl(true),
			}).DialContext,
		},
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expected dial to succeed with allowPrivate=true, got: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// TestDialControlBlocksMetadataIP exercises the dial-time guard with a
// fake address — we don't actually open a socket to 169.254.169.254;
// the Control hook fires before connect and returns an error that
// surfaces as net.OpError via the dialer.
func TestDialControlBlocksMetadataIP(t *testing.T) {
	ctrl := DialControl(false)
	err := ctrl("tcp", "169.254.169.254:80", fakeRawConn{})
	if err == nil {
		t.Fatal("expected DialControl to reject metadata IP, got nil")
	}
	if !errors.Is(err, ErrBlockedHost) {
		t.Errorf("expected ErrBlockedHost, got %v", err)
	}
}

// fakeRawConn is a no-op stand-in; DialControl never touches the
// syscall.RawConn, it just inspects the address string.
type fakeRawConn struct{}

func (fakeRawConn) Control(func(uintptr)) error { return nil }
func (fakeRawConn) Read(func(uintptr) bool) error {
	return errors.New("not implemented")
}

func (fakeRawConn) Write(func(uintptr) bool) error {
	return errors.New("not implemented")
}

var _ syscall.RawConn = fakeRawConn{}
