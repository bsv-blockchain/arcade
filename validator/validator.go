package validator

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/spv"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

const (
	maxBlockSize                       = 4 * 1024 * 1024 * 1024
	maxSatoshis                        = 21_000_000_00_000_000
	coinbaseTxID                       = "0000000000000000000000000000000000000000000000000000000000000000"
	maxTxSigopsCountPolicyAfterGenesis = ^uint32(0)
	minTxSizeBytes                     = 61
	dustLimit                          = 1
)

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

// ValidationError represents a validation error with an HTTP status code
type ValidationError struct {
	Code    int
	Message string
	Err     error
}

func (e *ValidationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

// Extended Arc-compatible error codes
var (
	ErrMalformed           = &ValidationError{Code: 460, Message: "malformed transaction"}
	ErrInputs              = &ValidationError{Code: 461, Message: "invalid inputs"}
	ErrOutputs             = &ValidationError{Code: 462, Message: "invalid outputs"}
	ErrFees                = &ValidationError{Code: 463, Message: "invalid fee"}
	ErrScript              = &ValidationError{Code: 464, Message: "invalid script"}
	ErrDoubleSpend         = &ValidationError{Code: 465, Message: "double spend"}
	ErrMalformedDoubleSpend = &ValidationError{Code: 466, Message: "malformed double spend"}
	ErrFrozen              = &ValidationError{Code: 468, Message: "frozen transaction"}
	ErrConsensus           = &ValidationError{Code: 469, Message: "consensus violation"}
)

// Policy defines validation policy settings
type Policy struct {
	MaxTxSizePolicy          int
	MaxTxSigopsCountsPolicy  int64
	MaxScriptSizePolicy      int
	MinFeePerKB              uint64
	EnableFeeCheck           bool
	EnableScriptExecution    bool
}

// Validator performs local transaction validation before submission
type Validator struct {
	policy *Policy
}

// NewValidator creates a new transaction validator with policy
func NewValidator(policy *Policy) *Validator {
	if policy == nil {
		policy = &Policy{
			MaxTxSizePolicy:         maxBlockSize,
			MaxTxSigopsCountsPolicy: int64(maxTxSigopsCountPolicyAfterGenesis),
			MaxScriptSizePolicy:     500000,
			MinFeePerKB:             50,
			EnableFeeCheck:          false,
			EnableScriptExecution:   false,
		}
	}
	return &Validator{
		policy: policy,
	}
}

// ValidateTransaction validates a transaction against policy rules
func (v *Validator) ValidateTransaction(rawTx []byte) error {
	tx, err := sdkTx.NewTransactionFromBytes(rawTx)
	if err != nil {
		return fmt.Errorf("failed to parse transaction: %w", err)
	}

	return v.commonValidateTransaction(tx)
}

// GetTxID extracts the transaction ID from raw transaction bytes
func (v *Validator) GetTxID(rawTx []byte) (string, error) {
	tx, err := sdkTx.NewTransactionFromBytes(rawTx)
	if err != nil {
		return "", fmt.Errorf("failed to parse transaction: %w", err)
	}
	return tx.TxID().String(), nil
}

func (v *Validator) commonValidateTransaction(tx *sdkTx.Transaction) error {
	txSize := tx.Size()

	if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
		return &ValidationError{Code: 460, Message: "transaction has no inputs or outputs", Err: ErrNoInputsOrOutputs}
	}

	if err := v.checkTxSize(txSize); err != nil {
		if errors.Is(err, ErrTxSizeGreaterThanMax) || errors.Is(err, ErrTxSizeLessThanMinSize) {
			return &ValidationError{Code: 460, Message: "invalid transaction size", Err: err}
		}
		return err
	}

	if err := v.checkInputs(tx); err != nil {
		return &ValidationError{Code: 461, Message: "invalid inputs", Err: err}
	}

	if err := v.checkOutputs(tx); err != nil {
		return &ValidationError{Code: 462, Message: "invalid outputs", Err: err}
	}

	if txSize < minTxSizeBytes {
		return &ValidationError{Code: 460, Message: "transaction too small", Err: ErrTxSizeLessThanMinSize}
	}

	if err := v.sigOpsCheck(tx); err != nil {
		return &ValidationError{Code: 464, Message: "too many sigops", Err: err}
	}

	if err := v.pushDataCheck(tx); err != nil {
		return &ValidationError{Code: 464, Message: "unlocking script not push only", Err: err}
	}

	if v.policy.EnableScriptExecution {
		if err := v.executeScripts(tx); err != nil {
			return &ValidationError{Code: 464, Message: "script execution failed", Err: err}
		}
	}

	return nil
}

func (v *Validator) executeScripts(tx *sdkTx.Transaction) error {
	ctx := context.Background()
	_, err := spv.VerifyScripts(ctx, tx)
	return err
}

func (v *Validator) checkTxSize(txSize int) error {
	maxTxSizePolicy := v.policy.MaxTxSizePolicy
	if maxTxSizePolicy == 0 {
		maxTxSizePolicy = maxBlockSize
	}
	if txSize > maxTxSizePolicy {
		return ErrTxSizeGreaterThanMax
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
		default:
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
		if input.SourceTXID.String() == coinbaseTxID {
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
	maxSigOps := v.policy.MaxTxSigopsCountsPolicy

	if maxSigOps == 0 {
		maxSigOps = int64(maxTxSigopsCountPolicyAfterGenesis)
	}

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

	if numSigOps > maxSigOps {
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
		if input.UnlockingScript == nil || len(*input.UnlockingScript) == 0 {
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
