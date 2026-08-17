package p2p_client

import (
	"math"
	"net"
	"strings"

	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/ssrfguard"
	"github.com/bsv-blockchain/arcade/store"
)

// legacyMiningFeeBytesBasis is the byte basis the legacy MinMiningTxFee field
// is expressed against once converted: satoshis per 1000 bytes.
const legacyMiningFeeBytesBasis = 1000

// pickDatahubURL returns the URL peers should use to propagate transactions.
// PropagationURL wins when present; BaseURL is the fallback. Empty string
// means the peer has advertised no usable endpoint.
func pickDatahubURL(m teranodep2p.NodeStatusMessage) string {
	if strings.TrimSpace(m.PropagationURL) != "" {
		return m.PropagationURL
	}
	return m.BaseURL
}

// legacyMiningFeeSatPerKB converts a legacy node_status MinMiningTxFee (BSV/kB)
// to satoshis per 1000 bytes. ok is false when the value cannot be trusted:
// node_status is unauthenticated, so a malformed or malicious announcement can
// carry NaN/Inf/negative or an absurd magnitude, which uint64() would turn into
// a wrapped garbage value polluting metrics and the int64-backed store columns.
//
// Shared by recordPeerPolicy and endpointPolicyFrom so the two paths cannot
// drift on what counts as an implausible fee.
func legacyMiningFeeSatPerKB(fee *float64) (uint64, bool) {
	if fee == nil {
		return 0, false
	}
	v := math.Round(*fee * 1e8) // 1 BSV = 1e8 satoshis
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > math.MaxInt64 {
		return 0, false
	}
	return uint64(v), true
}

// endpointPolicyFrom builds the policy to record against a peer's datahub URL
// registration, or nil when the peer advertised none.
//
// This is the per-endpoint, operator-facing view — "what will this specific
// node accept" — and so records what the node advertised verbatim, unlike
// recordPeerPolicy, which feeds the network-wide aggregate arcade enforces and
// therefore only keeps values it can compute with. An advertisement too large
// to store faithfully drops the whole policy rather than the row: the URL
// registration is the reason the row exists, and a peer sending a garbage size
// must not be able to remove its own endpoint from the registry.
func endpointPolicyFrom(m teranodep2p.NodeStatusMessage) *store.EndpointPolicy {
	var p *store.EndpointPolicy

	switch {
	case m.FeePolicy != nil:
		p = &store.EndpointPolicy{
			MiningFeeSatoshis:       m.FeePolicy.MiningFee.Satoshis,
			MiningFeeBytes:          m.FeePolicy.MiningFee.Bytes,
			MaxTxSizePolicy:         m.FeePolicy.MaxTxSizePolicy,
			MaxScriptSizePolicy:     m.FeePolicy.MaxScriptSizePolicy,
			MaxTxSigopsCountsPolicy: m.FeePolicy.MaxTxSigopsCountsPolicy,
		}
	case m.MinMiningTxFee != nil:
		// A node old enough to send only the scalar fee advertises no size
		// limits; they stay zero, and the /health view omits what is unset.
		sats, ok := legacyMiningFeeSatPerKB(m.MinMiningTxFee)
		if !ok {
			return nil
		}
		p = &store.EndpointPolicy{MiningFeeSatoshis: sats, MiningFeeBytes: legacyMiningFeeBytesBasis}
	default:
		return nil
	}

	if p.Validate() != nil {
		return nil
	}
	return p
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
