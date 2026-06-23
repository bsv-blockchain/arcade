// Package validator provides transaction validation functionality.
//
// Validation is delegated to teranode's BDK-backed TxValidator
// (github.com/bsv-blockchain/teranode/services/validator), the exact validator
// teranode/svnode use to admit transactions. This guarantees that arcade's
// intake decision matches node consensus across protocol upgrades instead of
// re-implementing the rules in pure Go, which previously drifted (e.g. the
// Chronicle push-only relaxation had to be hand-patched here). See issue #192.
//
// Because the BDK engine is a cgo binding to the BSV C++ consensus library,
// this package requires CGO_ENABLED=1 and the gobdk static archive (delivered
// by `go mod download`) to build and run.
package validator

import (
	"context"
	"fmt"
	"math"

	chaincfg "github.com/bsv-blockchain/go-chaincfg"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	tnvalidator "github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	// allForksActiveHeight is the block height arcade reports to the BDK
	// validator at intake. It sits above every network's Genesis and Chronicle
	// activation heights — so all protocol upgrades evaluate as active and
	// arcade validates against current consensus rules — yet stays below
	// math.MaxInt32. The BDK adapter converts the height to int32 and, in policy
	// mode, evaluates blockHeight-1, so math.MaxUint32 would overflow and reject
	// every transaction. arcade's intake is height-agnostic post-Genesis, so a
	// static height avoids a chain-tip lookup on the hot path.
	allForksActiveHeight uint32 = 2_000_000_000

	// unknownParentHeight is the per-input source height arcade reports for every
	// spent output. arcade does not track per-UTXO confirmation heights on the
	// intake path, so instead of fabricating a concrete height it reports the
	// "parent height unknown" sentinel — the value of teranode's internal
	// unconfirmedParentHeight (teranode .../services/validator/Validator.go), which
	// teranode itself stamps for parents whose height it cannot resolve.
	//
	// In policy mode — arcade's only mode; NewDefaultOptions sets
	// SkipPolicyChecks=false — teranode's BDK adapter (substituteUnconfirmedHeights
	// in .../services/validator/ScriptVerifierGoBDK.go) resolves this sentinel to
	// the candidate block height (allForksActiveHeight), which is post-Genesis and
	// post-Chronicle. Every input is therefore evaluated against current consensus,
	// matching how teranode's own mempool intake treats an unconfirmed parent.
	//
	// The previous value of 1 was below every network's Genesis activation height,
	// so BDK keyed each input's protocol era to pre-Genesis and wrongly enforced the
	// historical 520-byte MAX_SCRIPT_ELEMENT_SIZE_BEFORE_GENESIS limit on modern
	// post-Genesis spends (issue #214). The per-input UTXO height — not the spending
	// block height — selects that limit, so reporting a post-Genesis source height is
	// what lifts it. arcade's path runs no coinbase-maturity or relative-locktime
	// check, so reporting an "immature" height has no downside here; teranode remains
	// the authority and re-checks with real heights downstream.
	//
	// This is correct only in policy mode: in consensus mode the adapter maps the
	// sentinel to MEMPOOL_HEIGHT and BDK rejects every transaction as having an
	// unconfirmed input. arcade never validates in consensus mode.
	unknownParentHeight uint32 = math.MaxUint32

	// maxTxSizePolicyConsensusLimit is the BDK consensus ceiling on the policy
	// max-tx-size setting; the engine rejects construction above it.
	maxTxSizePolicyConsensusLimit = 1_000_000_000

	// defaultNetwork is used by NewValidator, which retains the legacy
	// network-less constructor for tests and callers that do not select one.
	defaultNetwork = "mainnet"
)

// DefaultMinFeePerKB defines the default minimum fee per kilobyte, in satoshis.
var DefaultMinFeePerKB = uint64(100)

// Policy defines the operator-facing validation policy. Fields left at their
// zero value fall back to arcade/teranode defaults.
type Policy struct {
	// MaxTxSizePolicy caps transaction size in bytes (0 => defaultMaxTxSizePolicy).
	MaxTxSizePolicy int
	// MaxTxSigopsCountsPolicy caps sigops per transaction (0 => unlimited, the
	// BSV post-Genesis default).
	MaxTxSigopsCountsPolicy int64
	// MinFeePerKB is the fee floor in satoshis/kB. A nil pointer uses
	// DefaultMinFeePerKB; an explicit pointer-to-zero accepts any fee.
	MinFeePerKB *uint64
}

// Validator performs intake transaction validation by delegating to teranode's
// BDK-backed TxValidator. It holds two validators: one that enforces the fee
// floor and one that does not, so ValidateTransaction can honour skipFees
// without disabling the script/standardness checks (which SkipPolicyChecks
// would also turn off).
type Validator struct {
	tv          *tnvalidator.TxValidator
	tvNoFee     *tnvalidator.TxValidator
	minFeePerKB uint64
}

// NewValidator creates a validator for mainnet with the given policy. A nil
// policy uses defaults. Retained for backward compatibility; production wiring
// should use NewValidatorForNetwork to select the configured network.
func NewValidator(policy *Policy) *Validator {
	v, err := NewValidatorForNetwork(defaultNetwork, policy)
	if err != nil {
		// mainnet params always resolve; a failure here is a programming error.
		panic(err)
	}
	return v
}

// NewValidatorForNetwork creates a validator bound to the given network
// (mainnet/testnet/teratestnet/regtest). It returns an error for an unknown
// network rather than letting the BDK adapter fail fatally.
func NewValidatorForNetwork(network string, policy *Policy) (*Validator, error) {
	params, err := chaincfg.GetChainParams(network)
	if err != nil {
		return nil, fmt.Errorf("validator: resolve chain params for network %q: %w", network, err)
	}

	minFee := DefaultMinFeePerKB
	if policy != nil && policy.MinFeePerKB != nil {
		minFee = *policy.MinFeePerKB
	}

	// 0 => keep the canonical default from defaultPolicySettings. An explicit
	// override is capped at the BDK consensus limit so construction never panics.
	maxTxSize := 0
	if policy != nil && policy.MaxTxSizePolicy > 0 {
		maxTxSize = policy.MaxTxSizePolicy
		if maxTxSize > maxTxSizePolicyConsensusLimit {
			maxTxSize = maxTxSizePolicyConsensusLimit
		}
	}

	var maxSigops int64 // 0 => unlimited (BSV post-Genesis)
	if policy != nil && policy.MaxTxSigopsCountsPolicy > 0 {
		maxSigops = policy.MaxTxSigopsCountsPolicy
	}

	logger := ulogger.New("arcade-validator")

	return &Validator{
		tv:          tnvalidator.NewTxValidator(logger, buildSettings(params, maxTxSize, maxSigops, minFee)),
		tvNoFee:     tnvalidator.NewTxValidator(logger, buildSettings(params, maxTxSize, maxSigops, 0)),
		minFeePerKB: minFee,
	}, nil
}

// MinFeePerKB returns the configured minimum fee per kB, in satoshis.
func (v *Validator) MinFeePerKB() uint64 {
	return v.minFeePerKB
}

// ValidateTransaction validates a transaction against current node consensus
// rules using the BDK engine. The transaction is converted to extended go-bt
// form from the source data carried in EF/BEEF; a missing source output is a
// hard rejection (StatusTxFormat). When skipFees is true the fee floor is not
// enforced but all script and standardness checks still run.
//
// ctx is accepted for call-site compatibility; BDK validation is synchronous
// and does not observe it.
func (v *Validator) ValidateTransaction(_ context.Context, tx *sdkTx.Transaction, skipFees bool) error {
	btx, err := toExtendedBT(tx)
	if err != nil {
		return mapTeranodeError(err)
	}

	utxoHeights := make([]uint32, len(btx.Inputs))
	for i := range utxoHeights {
		utxoHeights[i] = unknownParentHeight
	}

	tv := v.tv
	if skipFees {
		tv = v.tvNoFee
	}

	if vErr := tv.ValidateTransaction(btx, allForksActiveHeight, utxoHeights, tnvalidator.NewDefaultOptions()); vErr != nil {
		return mapTeranodeError(vErr)
	}
	return nil
}

// ValidatePolicy runs structural, script and standardness validation without
// enforcing the fee floor. Retained for callers that want a fee-agnostic check.
func (v *Validator) ValidatePolicy(tx *sdkTx.Transaction) error {
	return v.ValidateTransaction(context.Background(), tx, true)
}

// buildSettings assembles a teranode settings.Settings for the BDK validator:
// canonical BSV policy defaults, with the operator-controlled tx-size, sigop
// and fee values applied. minFeePerKB is in satoshis/kB and converted to the
// BSV/kB units teranode's fee check expects (0 => no fee floor).
func buildSettings(params *chaincfg.Params, maxTxSize int, maxSigops int64, minFeePerKB uint64) *settings.Settings {
	pol := defaultPolicySettings()
	if maxTxSize > 0 {
		pol.MaxTxSizePolicy = maxTxSize
	}
	pol.MaxTxSigopsCountsPolicy = maxSigops
	pol.MinMiningTxFee = satPerKBToBSVPerKB(minFeePerKB)
	return &settings.Settings{ChainCfgParams: params, Policy: pol}
}

// defaultPolicySettings returns teranode's canonical BSV policy values.
// settings.NewPolicySettings() returns an all-zero struct (zeros would make the
// C++ engine reject memory-bound scripts), so every field the BDK adapter reads
// is set explicitly. DataCarrier is enabled because arcade accepts OP_RETURN
// data transactions; MaxTxSizePolicy and MinMiningTxFee are overwritten by
// buildSettings.
func defaultPolicySettings() *settings.PolicySettings {
	return &settings.PolicySettings{
		ExcessiveBlockSize:              4294967296,
		MaxTxSizePolicy:                 10485760,
		MaxOrphanTxSize:                 1000000,
		DataCarrierSize:                 1000000,
		MaxScriptSizePolicy:             500000,
		MaxOpsPerScriptPolicy:           1000000,
		MaxScriptNumLengthPolicy:        10000,
		MaxPubKeysPerMultisigPolicy:     0,
		MaxTxSigopsCountsPolicy:         0,
		MaxStackMemoryUsagePolicy:       104857600,
		MaxStackMemoryUsageConsensus:    0,
		LimitAncestorCount:              1000000,
		LimitCPFPGroupMembersCount:      1000000,
		AcceptNonStdOutputs:             true,
		RequireStandard:                 false,
		DataCarrier:                     true,
		PermitBareMultisig:              true,
		MinMiningTxFee:                  0,
		MaxStdTxValidationDuration:      3,
		MaxNonStdTxValidationDuration:   1000,
		MaxTxChainValidationBudget:      50,
		ValidationClockCPU:              false,
		MinConsolidationFactor:          20,
		MaxConsolidationInputScriptSize: 150,
		MinConfConsolidationInput:       6,
		MinConsolidationInputMaturity:   6,
		AcceptNonStdConsolidationInput:  false,
		MaxCoinsViewCacheSize:           0,
	}
}

// satPerKBToBSVPerKB converts a satoshis/kB fee rate to BSV/kB, the unit
// teranode's checkFees expects (it converts back via *1e8/1000). 100 sat/kB =>
// 0.000001 BSV/kB.
func satPerKBToBSVPerKB(satPerKB uint64) float64 {
	return float64(satPerKB) / 1e8
}
