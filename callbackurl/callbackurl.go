// Package callbackurl centralizes the SSRF guard used by the api-server
// (registration-time validation of X-CallbackUrl) and by the webhook
// delivery client (dial-time refusal). One blocking predicate keeps the
// two layers from drifting and ensures a host that survives validation
// can't sneak past delivery via DNS rebinding.
//
// Threat model: arcade is reachable by clients that we don't fully trust,
// and the callback URL they register is dialed from inside our network.
// Without a guard, an attacker can register a callback that points at
// loopback (`127.0.0.1`), link-local (`169.254.0.0/16` — including the
// AWS/GCP/Azure metadata endpoint at `169.254.169.254`), RFC1918 ranges,
// or `0.0.0.0`/`::`, and turn arcade into a blind SSRF primitive.
package callbackurl

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"syscall"
)

// ErrBlockedHost is returned when an URL's host or a dial target resolves
// to an IP class that the SSRF guard refuses to talk to.
var ErrBlockedHost = errors.New("callback host resolves to a blocked address")

// ValidateURL parses raw, enforces http/https, and rejects the URL when
// its literal host parses as a blocked IP. DNS names are accepted at
// this layer — operators may legitimately use internal-looking DNS
// names that resolve to public IPs, and the dial-time guard catches
// the rebinding case where the name resolves to a private address.
//
// allowPrivate=true short-circuits the IP-class check for operators
// whose deployment intentionally posts callbacks to internal services
// (testing, k8s service-DNS, internal webhooks). The default is false.
func ValidateURL(raw string, allowPrivate bool) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("callback url is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse callback url: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("callback url scheme %q not allowed (only http/https)", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("callback url has no host")
	}
	if allowPrivate {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && IsBlockedIP(ip) {
		return fmt.Errorf("%w: %s", ErrBlockedHost, host)
	}
	return nil
}

// IsBlockedIP reports whether ip falls into a class arcade refuses to
// dial when the SSRF guard is active. The set:
//
//   - loopback (127.0.0.0/8, ::1)
//   - unspecified (0.0.0.0, ::)
//   - link-local unicast (169.254.0.0/16 — covers cloud metadata at
//     169.254.169.254 — and fe80::/10)
//   - RFC1918 / RFC4193 private (IP.IsPrivate())
//   - multicast (224.0.0.0/4, ff00::/8)
//   - interface-local multicast (covered by IsInterfaceLocalMulticast)
//
// Treating all of these as "blocked" intentionally errs on the side of
// safety; an operator who needs to dial one explicitly opts in via
// callback.allow_private_ips.
func IsBlockedIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() ||
		ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsPrivate() {
		return true
	}
	// IPv4-mapped IPv6 (::ffff:127.0.0.1) — re-check the underlying v4.
	if v4 := ip.To4(); v4 != nil && !v4.Equal(ip) {
		if v4.IsLoopback() || v4.IsUnspecified() || v4.IsLinkLocalUnicast() || v4.IsPrivate() {
			return true
		}
	}
	return false
}

// DialControl returns a net.Dialer.Control function that refuses to
// connect to a blocked IP. It runs after DNS resolution, so it catches
// the rebinding case where a hostname resolved to a now-private address
// even though ValidateURL accepted the literal hostname at registration
// time.
//
// allowPrivate=true makes the control a no-op so operators can opt out.
func DialControl(allowPrivate bool) func(network, address string, c syscall.RawConn) error {
	if allowPrivate {
		return func(string, string, syscall.RawConn) error { return nil }
	}
	return func(network, address string, _ syscall.RawConn) error {
		// The Go resolver hands us "<ip>:<port>" — both v4 ("1.2.3.4:80")
		// and v6 ("[::1]:80") flavors go through net.SplitHostPort.
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("dial %s: parsing address %q: %w", network, address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			// Should not happen — Dialer.Control runs after resolution —
			// but if a custom resolver hands us a name, fail closed.
			return fmt.Errorf("%w: dial target %q is not an IP literal", ErrBlockedHost, host)
		}
		if IsBlockedIP(ip) {
			return fmt.Errorf("%w: dial %s", ErrBlockedHost, ip)
		}
		return nil
	}
}
