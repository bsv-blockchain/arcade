package validator

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/spv"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	feemodel "github.com/bsv-blockchain/go-sdk/transaction/fee_model"
)

const (
	maxBlockSize                       = 4 * 1024 * 1024 * 1024
	maxSatoshis                        = 21_000_000_00_000_000
	maxTxSigopsCountPolicyAfterGenesis = ^uint32(0)
	minTxSizeBytes                     = 61
	dustLimit                          = 1
	DefaultMinFeePerKB                 = uint64(100)
)

var coinbaseTxID = chainhash.Hash{}

var (
	ErrNoInputsOrOutputs               = errors.New("transaction has no inputs or outputs")
	ErrTxOutputInvalid                 = errors.New("transaction output is invalid")
	ErrTxInputInvalid                  = errors.New("transaction input is invalid")
	ErrUnlockingScriptHasTooManySigOps = errors.New("transaction unlocking scripts have too many sigops")
	ErrEmptyUnlockingScript            = errors.New("transaction input unlocking script is empty")
	ErrUnlockingScriptNotPushOnly      = errors.New("transaction input unlocking script is not push only")
	ErrTxSizeLessThanMinSize           = fmt.Errorf("transaction size in bytes is less than %d bytes", minTxSizeBytes)
	ErrTxSizeGreaterThanMax            = fmt.Errorf("transaction size in bytes is greater than %d bytes", maxBlockSize)
)

// Policy defines validation policy settings
type Policy struct {
	MaxTxSizePolicy         int
	MaxTxSigopsCountsPolicy int64
	MinFeePerKB             uint64
}

// Validator performs local transaction validation before submission
type Validator struct {
	policy *Policy
}

// NewValidator creates a new transaction validator with policy
func NewValidator(policy *Policy) *Validator {
	if policy == nil {
		policy = &Policy{}
	}
	if policy.MaxTxSizePolicy == 0 {
		policy.MaxTxSizePolicy = maxBlockSize
	}
	if policy.MaxTxSigopsCountsPolicy == 0 {
		policy.MaxTxSigopsCountsPolicy = int64(maxTxSigopsCountPolicyAfterGenesis)
	}
	if policy.MinFeePerKB == 0 {
		policy.MinFeePerKB = DefaultMinFeePerKB
	}
	return &Validator{policy: policy}
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
	return v.policy.MinFeePerKB
}

// ValidateTransaction validates policy, and optionally fees and scripts
func (v *Validator) ValidateTransaction(ctx context.Context, tx *sdkTx.Transaction, skipFees, skipScripts bool) error {
	if err := v.ValidatePolicy(tx); err != nil {
		return err
	}

	if skipFees && skipScripts {
		return nil
	}

	var feeModel *feemodel.SatoshisPerKilobyte
	if !skipFees {
		feeModel = &feemodel.SatoshisPerKilobyte{Satoshis: v.policy.MinFeePerKB}
	}

	if skipScripts {
		// Fee validation only - use spv.Verify with fee model but gullible headers
		if _, err := spv.Verify(ctx, tx, &spv.GullibleHeadersClient{}, feeModel); err != nil {
			return err
		}
	} else {
		// Script validation (and fees if not skipped)
		if _, err := spv.Verify(ctx, tx, nil, feeModel); err != nil {
			return err
		}
	}

	return nil
}

func (v *Validator) checkOutputs(tx *sdkTx.Transaction) error {
	total := uint64(0)
	for index, output := range tx.Outputs {
		isData := output.LockingScript.IsData()
		switch {
		case !isData && (output.Satoshis > maxSatoshis || output.Satoshis < dustLimit):
			return errors.Join(ErrTxOutputInvalid, fmt.Errorf("output %d satoshis is invalid", index))
		case isData && output.Satoshis != 0:
			return errors.Join(ErrTxOutputInvalid, fmt.Errorf("output %d has non 0 value op return", index))
		}
		total += output.Satoshis
	}
	if total > maxSatoshis {
		return errors.Join(ErrTxOutputInvalid, errors.New("output total satoshis is too high"))
	}
	return nil
}

func (v *Validator) checkInputs(tx *sdkTx.Transaction) error {
	total := uint64(0)
	for index, input := range tx.Inputs {
		if *input.SourceTXID == coinbaseTxID {
			return errors.Join(ErrTxInputInvalid, fmt.Errorf("input %d is a coinbase input", index))
		}

		inputSatoshis := uint64(0)
		if input.SourceTxSatoshis() != nil {
			inputSatoshis = *input.SourceTxSatoshis()
		}

		if inputSatoshis > maxSatoshis {
			return errors.Join(ErrTxInputInvalid, fmt.Errorf("input %d satoshis is too high", index))
		}
		total += inputSatoshis
	}
	if total > maxSatoshis {
		return errors.Join(ErrTxInputInvalid, errors.New("input total satoshis is too high"))
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
		return errors.Join(ErrUnlockingScriptHasTooManySigOps, fmt.Errorf("sigops: %d", numSigOps))
	}
	return nil
}

func countSigOps(lockingScript interpreter.ParsedScript) int64 {
	numSigOps := int64(0)
	for _, op := range lockingScript {
		if op.Value() == script.OpCHECKSIG || op.Value() == script.OpCHECKSIGVERIFY {
			numSigOps++
		}
	}
	return numSigOps
}

func (v *Validator) pushDataCheck(tx *sdkTx.Transaction) error {
	for index, input := range tx.Inputs {
		if input.UnlockingScript == nil {
			return errors.Join(ErrEmptyUnlockingScript, fmt.Errorf("input: %d", index))
		}
		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.UnlockingScript)
		if err != nil {
			return err
		}
		if !parsedUnlockingScript.IsPushOnly() {
			return errors.Join(ErrUnlockingScriptNotPushOnly, fmt.Errorf("input: %d", index))
		}
	}
	return nil
}
