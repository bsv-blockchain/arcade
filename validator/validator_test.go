package validator

import (
	"testing"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

func TestNewValidator_Defaults(t *testing.T) {
	v := NewValidator(nil, nil)
	if v.policy.MaxTxSizePolicy != maxBlockSize {
		t.Errorf("expected maxBlockSize default, got %d", v.policy.MaxTxSizePolicy)
	}
	if v.policy.MinFeePerKB == nil || *v.policy.MinFeePerKB != DefaultMinFeePerKB {
		t.Error("expected default min fee per KB")
	}
}

func TestNewValidator_CustomPolicy(t *testing.T) {
	minFee := uint64(50)
	p := &Policy{
		MaxTxSizePolicy:         1000,
		MaxTxSigopsCountsPolicy: 500,
		MinFeePerKB:             &minFee,
	}
	v := NewValidator(p, nil)
	if v.policy.MaxTxSizePolicy != 1000 {
		t.Errorf("expected 1000, got %d", v.policy.MaxTxSizePolicy)
	}
	if *v.policy.MinFeePerKB != 50 {
		t.Errorf("expected 50, got %d", *v.policy.MinFeePerKB)
	}
}

func TestMinFeePerKB(t *testing.T) {
	v := NewValidator(nil, nil)
	if v.MinFeePerKB() != DefaultMinFeePerKB {
		t.Errorf("expected %d, got %d", DefaultMinFeePerKB, v.MinFeePerKB())
	}
}

func TestWrapPolicyError_Malformed(t *testing.T) {
	v := NewValidator(nil, nil)
	err := v.wrapPolicyError(ErrNoInputsOrOutputs)

	arcErr := arcerrors.GetArcError(err)
	if arcErr == nil {
		t.Fatal("expected ArcError")
	}
	if arcErr.StatusCode != arcerrors.StatusMalformed {
		t.Errorf("expected StatusMalformed (463), got %d", arcErr.StatusCode)
	}
}

func TestWrapPolicyError_TxSize(t *testing.T) {
	v := NewValidator(nil, nil)
	err := v.wrapPolicyError(ErrTxSizeGreaterThanMax)

	arcErr := arcerrors.GetArcError(err)
	if arcErr == nil {
		t.Fatal("expected ArcError")
	}
	if arcErr.StatusCode != arcerrors.StatusTxSize {
		t.Errorf("expected StatusTxSize (474), got %d", arcErr.StatusCode)
	}
}

func TestWrapPolicyError_Inputs(t *testing.T) {
	v := NewValidator(nil, nil)
	err := v.wrapPolicyError(ErrTxInputInvalid)

	arcErr := arcerrors.GetArcError(err)
	if arcErr == nil {
		t.Fatal("expected ArcError")
	}
	if arcErr.StatusCode != arcerrors.StatusInputs {
		t.Errorf("expected StatusInputs (462), got %d", arcErr.StatusCode)
	}
}

func TestWrapPolicyError_Outputs(t *testing.T) {
	v := NewValidator(nil, nil)
	err := v.wrapPolicyError(ErrTxOutputInvalid)

	arcErr := arcerrors.GetArcError(err)
	if arcErr == nil {
		t.Fatal("expected ArcError")
	}
	if arcErr.StatusCode != arcerrors.StatusOutputs {
		t.Errorf("expected StatusOutputs (464), got %d", arcErr.StatusCode)
	}
}

func TestWrapPolicyError_UnlockingScripts(t *testing.T) {
	v := NewValidator(nil, nil)
	err := v.wrapPolicyError(ErrUnlockingScriptHasTooManySigOps)

	arcErr := arcerrors.GetArcError(err)
	if arcErr == nil {
		t.Fatal("expected ArcError")
	}
	if arcErr.StatusCode != arcerrors.StatusUnlockingScripts {
		t.Errorf("expected StatusUnlockingScripts (461), got %d", arcErr.StatusCode)
	}
}
