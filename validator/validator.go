// Package validator provides transaction validation functionality.
//
// Validation delegates to go-bdk (github.com/bitcoin-sv/bdk/module/gobdk), the
// cgo bindings around the same C++ script engine svnode/Teranode use for
// consensus. This keeps arcade's intake verdict aligned with the datahub it
// broadcasts to, across protocol upgrades (Genesis, Chronicle, …), instead of
// re-implementing consensus rules in Go. See the package-level adapter in
// script_verifier_gobdk.go.
//
// arcade is an intake/broadcast service and does not own a UTXO set or a
// chaintracker, so it cannot supply BDK with per-input UTXO heights or the live
// chain tip. It validates in BDK "policy" mode against a fixed, configured
// height that sits past every network's Chronicle activation (see
// Policy.ValidationBlockHeight). Absolute/relative locktime and true input
// maturity are therefore not authoritatively checked at intake — the datahub
// remains the consensus gate, the same posture as the previous
// GullibleHeadersClient path.
package validator

import (
	"context"
	"errors"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

const (
	// DefaultValidationBlockHeight is the candidate block height arcade reports
	// to go-bdk for policy validation. It is deliberately far past every
	// network's Chronicle activation height (mainnet 943,816) so BDK evaluates
	// every input under the current (post-Chronicle) ruleset.
	DefaultValidationBlockHeight int32 = 1_000_000

	// maturityGap is added to the configured UTXO height to derive the
	// candidate block height, so inputs read as sufficiently mature and
	// standard spends are not falsely rejected on coinbase-maturity / CSV
	// grounds at intake.
	maturityGap int32 = 1_000
)

// DefaultMinFeePerKB defines the minimum fee per kilobyte in satoshis.
var DefaultMinFeePerKB = uint64(100)

var (
	// ErrCoinbaseNotSupported is returned when a coinbase transaction is
	// submitted for validation; arcade never broadcasts coinbase transactions.
	ErrCoinbaseNotSupported = errors.New("coinbase transactions are not supported")

	// ErrNotExtendedFormat is returned when a transaction cannot be serialised
	// to extended format because an input is missing its source output. go-bdk
	// requires extended-format bytes (prev locking script + satoshis) to verify
	// scripts, so submissions must be EF or BEEF.
	ErrNotExtendedFormat = errors.New("transaction is not in extended format (missing source outputs)")
)

// Policy defines validation policy settings fed to go-bdk. Zero-valued size /
// sigops fields leave BDK's built-in network defaults in place; only explicitly
// configured values override them.
type Policy struct {
	// MaxTxSizePolicy, when > 0, overrides BDK's max-tx-size policy (bytes).
	MaxTxSizePolicy int
	// MaxTxSigopsCountsPolicy, when > 0, overrides BDK's post-genesis sigops
	// policy limit.
	MaxTxSigopsCountsPolicy int64
	// MinFeePerKB is the minimum fee in satoshis per kilobyte. nil falls back to
	// DefaultMinFeePerKB; a pointer to 0 accepts zero-fee transactions.
	MinFeePerKB *uint64
	// Network is the canonical arcade network name (mainnet/testnet/
	// teratestnet/regtest). Empty defaults to mainnet.
	Network string
	// ValidationBlockHeight is the UTXO/candidate height reported to BDK. 0
	// falls back to DefaultValidationBlockHeight.
	ValidationBlockHeight int32
}

// Validator performs intake transaction validation by delegating to go-bdk.
type Validator struct {
	policy   *Policy
	verifier bdkValidator
}

// bdkValidator is the go-bdk-backed verification surface. It is implemented by
// the cgo adapter (script_verifier_gobdk.go) and stubbed out for non-cgo builds
// (script_verifier_nocgo.go).
type bdkValidator interface {
	validate(tx *sdkTx.Transaction) error
}

// NewValidator creates a transaction validator with the given policy. A nil
// policy uses defaults (mainnet, default fee floor, default height). It returns
// an error if the go-bdk engine cannot be constructed (e.g. unknown network, or
// a binary built without cgo).
func NewValidator(policy *Policy) (*Validator, error) {
	if policy == nil {
		policy = &Policy{}
	}
	if policy.MinFeePerKB == nil {
		policy.MinFeePerKB = &DefaultMinFeePerKB
	}
	if policy.Network == "" {
		policy.Network = "mainnet"
	}
	if policy.ValidationBlockHeight == 0 {
		policy.ValidationBlockHeight = DefaultValidationBlockHeight
	}

	verifier, err := newVerifier(policy)
	if err != nil {
		return nil, err
	}
	return &Validator{policy: policy, verifier: verifier}, nil
}

// ValidateTransaction validates a transaction against node policy via go-bdk.
//
// ctx is accepted for call-site compatibility; go-bdk validation is synchronous
// and CPU-bound, so it is not used. skipFees is likewise accepted for
// compatibility — the fee floor is governed by Policy.MinFeePerKB
// (AcceptZeroFee maps to a zero floor); production callers pass false.
func (v *Validator) ValidateTransaction(_ context.Context, tx *sdkTx.Transaction, _ bool) error {
	return v.verifier.validate(tx)
}
