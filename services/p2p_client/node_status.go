package p2p_client

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"
)

// pickDatahubURL returns the URL peers should use to propagate transactions.
// PropagationURL wins when present; BaseURL is the fallback. Empty string
// means the peer has advertised no usable endpoint.
func pickDatahubURL(m teranodep2p.NodeStatusMessage) string {
	if strings.TrimSpace(m.PropagationURL) != "" {
		return m.PropagationURL
	}
	return m.BaseURL
}

// validateURL enforces that a discovered URL is safe to POST transactions to.
// It returns the normalized form (single trailing slash trimmed) or an error
// with a human-readable reason suitable for a warn-level log.
func validateURL(raw string, allowPrivate bool) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("empty url")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return "", errors.New("empty host")
	}
	if !allowPrivate && isPrivateHost(host) {
		return "", fmt.Errorf("private address %q (enable p2p.allow_private_urls to accept)", host)
	}
	return strings.TrimSuffix(raw, "/"), nil
}

// isPrivateHost returns true for loopback, link-local, and RFC1918 addresses.
// A hostname that doesn't parse as an IP is treated as public — DNS names
// resolving to private ranges are the operator's problem, not ours, since
// resolving here would add a network round-trip per announcement.
func isPrivateHost(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsPrivate()
}
