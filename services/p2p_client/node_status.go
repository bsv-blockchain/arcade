package p2p_client

import (
	"net"
	"strings"

	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/ssrfguard"
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
//
// Validation is delegated to ssrfguard.ValidateURL: scheme allowlist,
// userinfo rejection, metadata-hostname deny list, and — the part the old
// in-package validator lacked — DNS resolution of non-literal hostnames with
// every resolved IP checked against the blocked ranges. An unresolvable
// hostname (e.g. a peer announcing its own cluster-internal service name
// like "asset") is rejected regardless of allowPrivate: it can never be
// reached from this cluster and must not enter the shared endpoint registry.
// lookup is injectable for tests; nil falls back to net.LookupIP.
func validateURL(raw string, allowPrivate bool, lookup func(host string) ([]net.IP, error)) (string, error) {
	raw = strings.TrimSpace(raw)
	if err := ssrfguard.ValidateURL(raw, allowPrivate, lookup); err != nil {
		return "", err
	}
	return strings.TrimSuffix(raw, "/"), nil
}
