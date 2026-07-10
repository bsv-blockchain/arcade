// Package ssrfguard implements a shared SSRF (Server-Side Request Forgery)
// blocking predicate for URLs arcade learns from untrusted sources — most
// importantly DataHub URLs announced by teranode peers over p2p, which
// arcade later POSTs transactions to from inside the cluster.
//
// Ported from merkle-service's internal/ssrfguard (see merkle-service PR
// #181, which fixed the same bug class at its p2p announcement intake) so
// the two repos validate discovered URLs identically.
//
// # Threat model
//
// A p2p peer announces the DataHub URL other nodes should propagate
// transactions to. Without guards a hostile (or simply misconfigured) peer
// can announce, e.g.,
//
//	http://169.254.169.254/latest/meta-data/iam/security-credentials/
//	http://asset:8090/api/v1  (its own cluster-internal service name)
//
// and either turn arcade's propagation path into an SSRF primitive against
// cloud metadata endpoints / RFC1918 neighbors, or pollute the shared
// endpoint registry with URLs that can never be reached from this cluster.
// ValidateURL blocks by destination IP rather than hostname — checking
// every address the name resolves to at validation time — and treats
// DNS-resolution failure as invalid, so an unresolvable name never enters
// the registry.
//
// Validation-time resolution alone does not defeat DNS rebinding: a
// hostname can resolve publicly when validated and privately when later
// dialed. Closing that window requires wiring CheckDialAddress into the
// dialer (net.Dialer.Control) of any client that connects to validated
// URLs; callers that skip the dial-time hook accept the rebinding
// residual risk.
package ssrfguard

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ErrBlockedAddress is returned when a URL or destination address points
// at a private/loopback/link-local/multicast/unspecified IP and the
// caller has not opted in to allow private destinations.
var ErrBlockedAddress = errors.New("destination address is blocked by SSRF policy")

// ErrInvalidURL is returned when a URL is structurally invalid (parse
// failure, missing scheme/host, disallowed scheme, unresolvable host, etc.).
var ErrInvalidURL = errors.New("invalid URL")

// blockedHostnames is a small deny list of hostnames that are considered
// SSRF-relevant on top of the IP-based predicate. They mostly resolve to
// link-local IPs (e.g. 169.254.169.254) which are already covered by
// IsLinkLocalUnicast, but we list them defensively so the rejection
// message is human-readable when someone tries them by name.
var blockedHostnames = map[string]struct{}{
	"metadata.google.internal": {},
	"metadata":                 {},
	"metadata.goog":            {},
	"169.254.169.254":          {},
	"fd00:ec2::254":            {}, // AWS IMDSv2 link-local IPv6
}

// cgnatNet is 100.64.0.0/10 (RFC 6598 shared address space / carrier-grade
// NAT). Go's IP.IsPrivate only covers RFC1918, so CGNAT needs an explicit
// check. (merkle-service's copy documents CGNAT but never checks it — fixed
// here; worth upstreaming.)
var cgnatNet = net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

// IsBlockedIP reports whether ip is on the SSRF deny-list. The deny-list
// covers loopback, link-local unicast/multicast, multicast, RFC1918
// private (10/8, 172.16/12, 192.168/16) and CGNAT (100.64/10),
// unique-local IPv6 (fc00::/7), the IPv4 unspecified address (0.0.0.0)
// and the IPv6 unspecified address (::). Loopback covers 127/8 and ::1.
//
// Pass allowPrivate=true to opt out of the private/loopback/link-local
// checks (operator-controlled escape hatch for testing or legitimately
// internal deployments).
func IsBlockedIP(ip net.IP, allowPrivate bool) bool {
	if ip == nil {
		// A nil IP cannot be safely dialed; treat as blocked.
		return true
	}
	if ip.IsUnspecified() {
		// 0.0.0.0 and :: never identify a real remote host. They
		// resolve to the local machine's own listening sockets, which
		// is exactly what an SSRF attacker wants.
		return true
	}
	if ip.IsMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if allowPrivate {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsPrivate() {
		return true
	}
	if v4 := ip.To4(); v4 != nil && cgnatNet.Contains(v4) {
		return true
	}
	// IPv6 unique local (fc00::/7) — IsPrivate covers this in Go 1.17+
	// but we keep an explicit fallback for clarity / future-proofing.
	if ip.To4() == nil && len(ip) == net.IPv6len && ip[0]&0xfe == 0xfc {
		return true
	}
	return false
}

// IsBlockedHostname returns true if the hostname (lowercased, port
// stripped) matches a known cloud metadata or internal hostname.
// Hostname-based blocking is a best-effort defense-in-depth check on top
// of IP-based blocking; the IP check is authoritative.
func IsBlockedHostname(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	// Strip an optional brackets + port from IPv6 literals.
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	_, ok := blockedHostnames[host]
	return ok
}

// ValidateURL parses raw and rejects it if the URL is malformed, uses a
// scheme other than http/https, has no host, or resolves to a blocked
// destination. lookup is the DNS function used to resolve a hostname to
// IP addresses; pass net.LookupIP in production and a stub in tests. If
// lookup is nil, net.LookupIP is used.
//
// allowPrivate lets operators opt in to private/loopback/link-local
// destinations. The metadata-hostname, unspecified/multicast, and
// DNS-resolvability checks remain in force regardless.
func ValidateURL(raw string, allowPrivate bool, lookup func(host string) ([]net.IP, error)) error {
	if raw == "" {
		return fmt.Errorf("%w: empty URL", ErrInvalidURL)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidURL, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("%w: scheme %q not allowed (must be http or https)", ErrInvalidURL, u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("%w: missing host", ErrInvalidURL)
	}
	// Reject userinfo entirely — `https://example.com:80@127.0.0.1/` would
	// parse with Hostname() == "127.0.0.1" so the IP check below already
	// catches the canonical form, but stripping userinfo also dodges any
	// downstream tooling that incorrectly treats user@host as the host.
	if u.User != nil {
		return fmt.Errorf("%w: URL must not contain userinfo", ErrInvalidURL)
	}

	if IsBlockedHostname(host) {
		return fmt.Errorf("%w: hostname %q is on the metadata-endpoint deny list", ErrBlockedAddress, host)
	}

	// If host is an IP literal, check it directly. Otherwise resolve and
	// check every returned address — an attacker who controls DNS could
	// otherwise return a single internal address that we miss if we only
	// check the first.
	if ip := net.ParseIP(host); ip != nil {
		if IsBlockedIP(ip, allowPrivate) {
			return fmt.Errorf("%w: %s", ErrBlockedAddress, ip.String())
		}
		return nil
	}

	if lookup == nil {
		lookup = net.LookupIP
	}
	ips, err := lookup(host)
	if err != nil {
		// DNS resolution failure at discovery time is treated as a
		// validation error so the caller knows the URL is unusable; a
		// cluster-internal name like "asset" must not enter the shared
		// endpoint registry only to fail on every propagation attempt.
		return fmt.Errorf("%w: failed to resolve %s: %w", ErrInvalidURL, host, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("%w: %s did not resolve to any addresses", ErrInvalidURL, host)
	}
	for _, ip := range ips {
		if IsBlockedIP(ip, allowPrivate) {
			return fmt.Errorf("%w: %s resolves to %s", ErrBlockedAddress, host, ip.String())
		}
	}
	return nil
}

// CheckDialAddress validates a host:port address that the dialer is
// about to connect to. Used as a hook in net.Dialer.Control so DNS
// rebinding or any host that slipped past ValidateURL is rejected at
// connection time. address is the canonical "ip:port" form passed to
// the network stack — it is always an IP literal at this point.
func CheckDialAddress(address string, allowPrivate bool) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// If we can't even parse the address, fail closed.
		return fmt.Errorf("%w: cannot parse dial address %q: %w", ErrBlockedAddress, address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// Dialer.Control receives the resolved IP, so a non-IP host is
		// itself anomalous; refuse rather than guess.
		return fmt.Errorf("%w: dial address %q is not an IP", ErrBlockedAddress, host)
	}
	if IsBlockedIP(ip, allowPrivate) {
		return fmt.Errorf("%w: refusing to dial %s", ErrBlockedAddress, ip.String())
	}
	return nil
}
