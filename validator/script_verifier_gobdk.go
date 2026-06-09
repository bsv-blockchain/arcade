//go:build cgo

// This file implements the go-bdk transaction validation adapter, mirroring
// teranode's services/validator/ScriptVerifierGoBDK.go but adapted to arcade's
// go-sdk transaction type and intake-only role (policy mode, constant height).
package validator

import (
	"errors"
	"fmt"

	gobdk "github.com/bitcoin-sv/bdk/module/gobdk"
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bsv-blockchain/go-chaincfg"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

// bdkNetName maps an arcade canonical network name to the BDK/svnode C++ chain
// name accepted by bdkscript.NewTxValidator.
func bdkNetName(network string) (string, error) {
	switch network {
	case "", config_mainnet:
		return "main", nil
	case config_testnet:
		return "test", nil
	case config_teratestnet:
		return "teratestnet", nil
	case config_regtest:
		return "regtest", nil
	default:
		return "", fmt.Errorf("unsupported network %q", network)
	}
}

// Canonical arcade network names. Duplicated here (rather than importing
// config) to avoid an import cycle between validator and config.
const (
	config_mainnet     = "mainnet"
	config_testnet     = "testnet"
	config_teratestnet = "teratestnet"
	config_regtest     = "regtest"
)

// scriptVerifierGoBDK adapts arcade validation data to go-bdk.
type scriptVerifierGoBDK struct {
	se          *bdkscript.TxValidator
	utxoHeight  int32
	blockHeight int32
}

// newVerifier constructs the go-bdk-backed verifier from policy. It is the cgo
// build's implementation of the bdkValidator factory (see
// script_verifier_nocgo.go for the !cgo counterpart).
func newVerifier(policy *Policy) (bdkValidator, error) {
	netName, err := bdkNetName(policy.Network)
	if err != nil {
		return nil, err
	}
	params, err := chaincfg.GetChainParams(policy.Network)
	if err != nil {
		return nil, fmt.Errorf("go-bdk: %w", err)
	}

	se := bdkscript.NewTxValidator(netName)
	if se == nil {
		return nil, fmt.Errorf("go-bdk: unable to create tx validator for network %q (gobdk %s)", netName, gobdk.BDK_VERSION_STRING())
	}

	// Activation heights are required so BDK can compute per-input script flags
	// (genesis/chronicle era) against the candidate height we report.
	// #nosec G115 -- activation heights are small, well within int32.
	if err := se.SetGenesisActivationHeight(int32(params.GenesisActivationHeight)); err != nil {
		return nil, fmt.Errorf("go-bdk: set genesis activation height: %w", err)
	}
	// #nosec G115 -- activation heights are small, well within int32.
	if err := se.SetChronicleActivationHeight(int32(params.ChronicleActivationHeight)); err != nil {
		return nil, fmt.Errorf("go-bdk: set chronicle activation height: %w", err)
	}

	// Fee floor. MinFeePerKB is already integer sat/kB (no float rounding).
	var satoshisPerKB int64
	if policy.MinFeePerKB != nil {
		// #nosec G115 -- fee floor is operator-configured and bounded.
		satoshisPerKB = int64(*policy.MinFeePerKB)
	}
	if err := se.SetMinMiningTxFee(satoshisPerKB); err != nil {
		return nil, fmt.Errorf("go-bdk: set min mining tx fee=%d sat/kB: %w", satoshisPerKB, err)
	}

	// Optional size / sigops overrides; leave BDK network defaults otherwise.
	if policy.MaxTxSizePolicy > 0 {
		if err := se.SetMaxTxSizePolicy(int64(policy.MaxTxSizePolicy)); err != nil {
			return nil, fmt.Errorf("go-bdk: set max tx size policy: %w", err)
		}
	}
	if policy.MaxTxSigopsCountsPolicy > 0 {
		if err := se.SetMaxSigOpsPostGenesisPolicy(policy.MaxTxSigopsCountsPolicy); err != nil {
			return nil, fmt.Errorf("go-bdk: set max sigops post-genesis policy: %w", err)
		}
	}

	return &scriptVerifierGoBDK{
		se:          se,
		utxoHeight:  policy.ValidationBlockHeight,
		blockHeight: policy.ValidationBlockHeight + maturityGap,
	}, nil
}

// validate runs go-bdk policy validation for a single transaction.
func (v *scriptVerifierGoBDK) validate(tx *sdkTx.Transaction) error {
	if tx.IsCoinbase() {
		return arcerrors.NewArcError(ErrCoinbaseNotSupported, arcerrors.StatusInputs)
	}

	// go-bdk consumes extended-format bytes (prev locking script + satoshis per
	// input). EF() fails if any input lacks its source output, i.e. the
	// submission was not EF/BEEF.
	eTxBytes, err := tx.EF()
	if err != nil {
		return arcerrors.NewArcError(fmt.Errorf("%w: %w", ErrNotExtendedFormat, err), arcerrors.StatusTxFormat)
	}

	utxoHeights := make([]int32, len(tx.Inputs))
	for i := range utxoHeights {
		utxoHeights[i] = v.utxoHeight
	}

	// consensus=false → policy (mempool) context: policy + consensus checks.
	if err := v.se.ValidateTransaction(eTxBytes, utxoHeights, v.blockHeight, false); err != nil {
		return mapBDKValidationError(err)
	}
	return nil
}

// mapBDKValidationError translates a go-bdk DoSError/ScriptError into an
// arcade ARC-status error, replacing the previous wrapPolicyError/wrapSPVError.
func mapBDKValidationError(errVerify error) error {
	var dosErr bdkscript.DoSError
	if errors.As(errVerify, &dosErr) {
		return arcerrors.NewArcError(errVerify, dosStatusCode(dosErr.Code()))
	}

	var scriptErr bdkscript.ScriptError
	if errors.As(errVerify, &scriptErr) {
		if scriptErr.Code() == bdkscript.SCRIPT_ERR_CGO_EXCEPTION {
			return arcerrors.NewArcError(errVerify, arcerrors.StatusGeneric)
		}
		// Every script-execution failure maps to the unlocking-scripts bucket.
		return arcerrors.NewArcError(errVerify, arcerrors.StatusUnlockingScripts)
	}

	return arcerrors.NewArcError(errVerify, arcerrors.StatusGeneric)
}

// dosStatusCode maps a BDK transaction-level DoS error code to an ARC status.
func dosStatusCode(code bdkscript.DoSErrorCode) arcerrors.StatusCode {
	switch code {
	case bdkscript.DOS_ERR_INSUFFICIENT_FEE:
		return arcerrors.StatusFees
	case bdkscript.DOS_ERR_OVERSIZE:
		return arcerrors.StatusTxSize
	case bdkscript.DOS_ERR_VIN_EMPTY,
		bdkscript.DOS_ERR_NULL_PREVOUT,
		bdkscript.DOS_ERR_DUPLICATE_INPUTS,
		bdkscript.DOS_ERR_UNCONFIRMED_INPUT_IN_BLOCK,
		bdkscript.DOS_ERR_INPUT_VALUES_OUT_OF_RANGE,
		bdkscript.DOS_ERR_INPUTS_BELOW_OUTPUTS,
		bdkscript.DOS_ERR_COINBASE_NOT_ALLOWED:
		return arcerrors.StatusInputs
	case bdkscript.DOS_ERR_VOUT_EMPTY,
		bdkscript.DOS_ERR_OUTPUT_NEGATIVE,
		bdkscript.DOS_ERR_OUTPUT_TOO_LARGE,
		bdkscript.DOS_ERR_OUTPUT_TOTAL_TOO_LARGE:
		return arcerrors.StatusOutputs
	case bdkscript.DOS_ERR_SIGOPS_POLICY,
		bdkscript.DOS_ERR_SIGOPS_CONSENSUS,
		bdkscript.DOS_ERR_NOT_STANDARD,
		bdkscript.DOS_ERR_NOT_FREE_CONSOLIDATION,
		bdkscript.DOS_ERR_P2SH_OUTPUT_POST_GENESIS:
		return arcerrors.StatusUnlockingScripts
	default:
		return arcerrors.StatusGeneric
	}
}
