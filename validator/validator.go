// Package validator provides transaction validation functionality.
package validator

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/spv"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
	feemodel "github.com/bsv-blockchain/go-sdk/transaction/fee_model"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

const (
	maxBlockSize                       = 4 * 1024 * 1024 * 1024
	maxSatoshis                        = 21_000_000_00_000_000
	maxTxSigopsCountPolicyAfterGenesis = ^uint32(0)
	minTxSizeBytes                     = 61
	dustLimit                          = 1
)

// DefaultMinFeePerKB defines the minimum fee per kilobyte.
var DefaultMinFeePerKB = uint64(100)

var (
	ErrNoInputsOrOutputs              = errors.New("transaction has no inputs or outputs")
	ErrTxOutputInvalid                = errors.New("transaction output is invalid")
	ErrTxOutputSatoshisInvalid        = errors.New("output satoshis is invalid")
	ErrTxOutputNonZeroOpReturn        = errors.New("output has non 0 value op return")
	ErrTxOutputTotalSatoshisTooHigh   = errors.New("output total satoshis is too high")
	ErrTxInputInvalid                 = errors.New("transaction input is invalid")
	ErrTxInputCoinbaseInput           = errors.New("input is a coinbase input")
	ErrTxInputSatoshisTooHigh         = errors.New("input satoshis is too high")
	ErrTxInputTotalSatoshisTooHigh    = errors.New("input total satoshis is too high")
	ErrUnlockingScriptHasTooManySigOps = errors.New("transaction unlocking scripts have too many sigops")
	ErrEmptyUnlockingScript           = errors.New("transaction input unlocking script is empty")
	ErrUnlockingScriptNotPushOnly     = errors.New("transaction input unlocking script is not push only")
	ErrTxSizeLessThanMinSize          = fmt.Errorf("transaction size in bytes is less than %d bytes", minTxSizeBytes)
	ErrTxSizeGreaterThanMax           = fmt.Errorf("transaction size in bytes is greater than %d bytes", maxBlockSize)
)

// Policy defines validation policy settings
type Policy struct {
	MaxTxSizePolicy         int
	MaxTxSigopsCountsPolicy int64
	MinFeePerKB             *uint64
}

// Validator performs local transaction validation before submission
type Validator struct {
	policy       *Policy
	chainTracker chaintracker.ChainTracker
}

// NewValidator creates a new transaction validator with policy and optional chaintracker
func NewValidator(policy *Policy, ct chaintracker.ChainTracker) *Validator {
	if policy == nil {
		policy = &Policy{}
	}
	if policy.MaxTxSizePolicy == 0 {
		policy.MaxTxSizePolicy = maxBlockSize
	}
	if policy.MaxTxSigopsCountsPolicy == 0 {
		policy.MaxTxSigopsCountsPolicy = int64(maxTxSigopsCountPolicyAfterGenesis)
	}
	if policy.MinFeePerKB == nil {
		policy.MinFeePerKB = &DefaultMinFeePerKB
	}
	return &Validator{
		policy:       policy,
		chainTracker: ct,
	}
}

// ValidatePolicy validates a transaction against node policy rules
func (v *Validator) ValidatePolicy(tx *sdkTx.Transaction) error {
	txSize := tx.Size()

	if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
		return ErrNoInputsOrOutputs
	}

	if txSize > v.policy.MaxTxSizePolicy {
		return ErrTxSizeGreaterThanMax
	}

	if txSize < minTxSizeBytes {
		return ErrTxSizeLessThanMinSize
	}

	if err := v.checkInputs(tx); err != nil {
		return err
	}

	if err := v.checkOutputs(tx); err != nil {
		return err
	}

	if err := v.sigOpsCheck(tx); err != nil {
		return err
	}

	if err := v.pushDataCheck(tx); err != nil {
		return err
	}

	return nil
}

// MinFeePerKB returns the configured minimum fee per KB
func (v *Validator) MinFeePerKB() uint64 {
	return *v.policy.MinFeePerKB
}

// ValidateTransaction validates policy, and optionally fees and scripts.
// Uses go-sdk v1.2.19's spv.Verify which takes a context.Context as its first
// argument. Callers that don't have a ctx can pass context.TODO().
func (v *Validator) ValidateTransaction(ctx context.Context, tx *sdkTx.Transaction, skipFees, skipScripts bool) error {
	if err := v.ValidatePolicy(tx); err != nil {
		return v.wrapPolicyError(err)
	}

	if skipFees && skipScripts {
		return nil
	}

	var feeModel sdkTx.FeeModel
	if !skipFees {
		feeModel = &feemodel.SatoshisPerKilobyte{Satoshis: *v.policy.MinFeePerKB}
	}

	ct := v.chainTracker
	if skipScripts {
		ct = &spv.GullibleHeadersClient{}
	}

	if ctx == nil {
		ctx = context.TODO()
	}

	if ct != nil || feeModel != nil {
		if _, err := spv.Verify(ctx, tx, ct, feeModel); err != nil {
			return v.wrapSPVError(err)
		}
	}

	return nil
}

// wrapPolicyError wraps policy validation errors with ARC-compatible status codes.
func (v *Validator) wrapPolicyError(err error) error {
	switch {
	case errors.Is(err, ErrNoInputsOrOutputs):
		return arcerrors.NewArcError(err, arcerrors.StatusMalformed)
	case errors.Is(err, ErrTxSizeGreaterThanMax), errors.Is(err, ErrTxSizeLessThanMinSize):
		return arcerrors.NewArcError(err, arcerrors.StatusTxSize)
	case errors.Is(err, ErrTxInputInvalid):
		return arcerrors.NewArcError(err, arcerrors.StatusInputs)
	case errors.Is(err, ErrTxOutputInvalid):
		return arcerrors.NewArcError(err, arcerrors.StatusOutputs)
	case errors.Is(err, ErrUnlockingScriptHasTooManySigOps),
		errors.Is(err, ErrEmptyUnlockingScript),
		errors.Is(err, ErrUnlockingScriptNotPushOnly):
		return arcerrors.NewArcError(err, arcerrors.StatusUnlockingScripts)
	default:
		return arcerrors.NewArcError(err, arcerrors.StatusMalformed)
	}
}

// wrapSPVError wraps SPV verification errors with ARC-compatible status codes.
func (v *Validator) wrapSPVError(err error) error {
	errStr := err.Error()
	switch {
	case contains(errStr, "fee"):
		return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusFees, errStr)
	case contains(errStr, "script"):
		return arcerrors.NewArcError(err, arcerrors.StatusUnlockingScripts)
	case contains(errStr, "input"), contains(errStr, "source"):
		return arcerrors.NewArcError(err, arcerrors.StatusInputs)
	case contains(errStr, "merkle"):
		return arcerrors.NewArcError(err, arcerrors.StatusGeneric)
	default:
		return arcerrors.NewArcError(err, arcerrors.StatusGeneric)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func (v *Validator) checkOutputs(tx *sdkTx.Transaction) error {
	total := uint64(0)
	for _, output := range tx.Outputs {
		isData := output.LockingScript.IsData()
		switch {
		case !isData && (output.Satoshis > maxSatoshis || output.Satoshis < dustLimit):
			return errors.Join(ErrTxOutputInvalid, ErrTxOutputSatoshisInvalid)
		case isData && output.Satoshis != 0:
			return errors.Join(ErrTxOutputInvalid, ErrTxOutputNonZeroOpReturn)
		}
		total += output.Satoshis
	}
	if total > maxSatoshis {
		return errors.Join(ErrTxOutputInvalid, ErrTxOutputTotalSatoshisTooHigh)
	}
	return nil
}

func (v *Validator) checkInputs(tx *sdkTx.Transaction) error {
	total := uint64(0)
	for _, input := range tx.Inputs {
		if *input.SourceTXID == (chainhash.Hash{}) {
			return errors.Join(ErrTxInputInvalid, ErrTxInputCoinbaseInput)
		}

		inputSatoshis := uint64(0)
		if input.SourceTxSatoshis() != nil {
			inputSatoshis = *input.SourceTxSatoshis()
		}

		if inputSatoshis > maxSatoshis {
			return errors.Join(ErrTxInputInvalid, ErrTxInputSatoshisTooHigh)
		}
		total += inputSatoshis
	}
	if total > maxSatoshis {
		return errors.Join(ErrTxInputInvalid, ErrTxInputTotalSatoshisTooHigh)
	}
	return nil
}

func (v *Validator) sigOpsCheck(tx *sdkTx.Transaction) error {
	parser := interpreter.DefaultOpcodeParser{}
	numSigOps := int64(0)

	for _, input := range tx.Inputs {
		parsedUnlockingScript, err := parser.Parse(input.UnlockingScript)
		if err != nil {
			return err
		}
		numSigOps += countSigOps(parsedUnlockingScript)
	}

	for _, output := range tx.Outputs {
		parsedLockingScript, err := parser.Parse(output.LockingScript)
		if err != nil {
			return err
		}
		numSigOps += countSigOps(parsedLockingScript)
	}

	if numSigOps > v.policy.MaxTxSigopsCountsPolicy {
		return ErrUnlockingScriptHasTooManySigOps
	}
	return nil
}

func countSigOps(lockingScript interpreter.ParsedScript) int64 {
	numSigOps := int64(0)
	for _, op := range lockingScript {
		if op.Value() == 0xac || op.Value() == 0xad { // OP_CHECKSIG, OP_CHECKSIGVERIFY
			numSigOps++
		}
	}
	return numSigOps
}

func (v *Validator) pushDataCheck(tx *sdkTx.Transaction) error {
	for _, input := range tx.Inputs {
		if input.UnlockingScript == nil {
			return ErrEmptyUnlockingScript
		}
		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.UnlockingScript)
		if err != nil {
			return err
		}
		if !parsedUnlockingScript.IsPushOnly() {
			return ErrUnlockingScriptNotPushOnly
		}
	}
	return nil
}
