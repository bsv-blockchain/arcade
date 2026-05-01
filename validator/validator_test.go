package validator

import (
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

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

// nonDataLockingScript returns a minimal non-data locking script suitable for
// exercising the output validation paths. A single OP_TRUE (0x51) is treated
// as a non-data, non-empty locking script by the SDK's IsData heuristic.
func nonDataLockingScript() *script.Script {
	s := script.Script([]byte{0x51})
	return &s
}

// nonZeroSourceTXID returns a pointer to a non-zero chainhash so an input is
// not treated as a coinbase input by checkInputs.
func nonZeroSourceTXID() *chainhash.Hash {
	h := chainhash.Hash{}
	h[0] = 0x01
	return &h
}

// TestCheckOutputs_OverflowGuarded crafts a transaction with two non-data
// outputs each at maxSatoshis. Each value individually passes the
// "output.Satoshis > maxSatoshis" check, and their sum exceeds maxSatoshis;
// the per-iteration guard must reject this before the unbounded total can
// be relied upon.
func TestCheckOutputs_OverflowGuarded(t *testing.T) {
	v := NewValidator(nil, nil)

	tx := &sdkTx.Transaction{
		Outputs: []*sdkTx.TransactionOutput{
			{Satoshis: maxSatoshis, LockingScript: nonDataLockingScript()},
			{Satoshis: maxSatoshis, LockingScript: nonDataLockingScript()},
		},
	}

	err := v.checkOutputs(tx)
	if err == nil {
		t.Fatal("expected error for total satoshis above maxSatoshis, got nil")
	}
	if !errors.Is(err, ErrTxOutputTotalSatoshisTooHigh) {
		t.Errorf("expected ErrTxOutputTotalSatoshisTooHigh, got %v", err)
	}
	if !errors.Is(err, ErrTxOutputInvalid) {
		t.Errorf("expected ErrTxOutputInvalid wrap, got %v", err)
	}
}

// TestCheckOutputs_ManySmallOutputsCannotOverflow exercises the per-iteration
// guard by accumulating many outputs whose total exceeds maxSatoshis. This is
// the regression case for F-004: prior to the fix a sequence of values that
// summed past 2^64 could wrap and slip past the post-loop check.
func TestCheckOutputs_ManySmallOutputsCannotOverflow(t *testing.T) {
	v := NewValidator(nil, nil)

	half := uint64(maxSatoshis / 2)
	tx := &sdkTx.Transaction{
		Outputs: []*sdkTx.TransactionOutput{
			{Satoshis: half, LockingScript: nonDataLockingScript()},
			{Satoshis: half, LockingScript: nonDataLockingScript()},
			{Satoshis: half, LockingScript: nonDataLockingScript()},
		},
	}

	err := v.checkOutputs(tx)
	if err == nil {
		t.Fatal("expected error for cumulative output satoshis above maxSatoshis, got nil")
	}
	if !errors.Is(err, ErrTxOutputTotalSatoshisTooHigh) {
		t.Errorf("expected ErrTxOutputTotalSatoshisTooHigh, got %v", err)
	}
}

// TestCheckOutputs_ValidPasses confirms a legitimate transaction is still
// accepted by checkOutputs after the overflow guard is added.
func TestCheckOutputs_ValidPasses(t *testing.T) {
	v := NewValidator(nil, nil)

	tx := &sdkTx.Transaction{
		Outputs: []*sdkTx.TransactionOutput{
			{Satoshis: 1_000, LockingScript: nonDataLockingScript()},
			{Satoshis: 2_000, LockingScript: nonDataLockingScript()},
		},
	}

	if err := v.checkOutputs(tx); err != nil {
		t.Fatalf("expected no error for valid outputs, got %v", err)
	}
}

// TestCheckInputs_OverflowGuarded crafts a transaction whose input
// SourceTxSatoshis values are each <= maxSatoshis but whose cumulative sum
// exceeds maxSatoshis. Without the per-iteration guard a uint64 wrap could
// allow the post-loop check to pass.
func TestCheckInputs_OverflowGuarded(t *testing.T) {
	v := NewValidator(nil, nil)

	makeInput := func(sats uint64) *sdkTx.TransactionInput {
		in := &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID()}
		in.SetSourceTxOutput(&sdkTx.TransactionOutput{Satoshis: sats, LockingScript: nonDataLockingScript()})
		return in
	}

	tx := &sdkTx.Transaction{
		Inputs: []*sdkTx.TransactionInput{
			makeInput(maxSatoshis),
			makeInput(maxSatoshis),
		},
	}

	err := v.checkInputs(tx)
	if err == nil {
		t.Fatal("expected error for total input satoshis above maxSatoshis, got nil")
	}
	if !errors.Is(err, ErrTxInputTotalSatoshisTooHigh) {
		t.Errorf("expected ErrTxInputTotalSatoshisTooHigh, got %v", err)
	}
	if !errors.Is(err, ErrTxInputInvalid) {
		t.Errorf("expected ErrTxInputInvalid wrap, got %v", err)
	}
}

// TestCheckInputs_ManySmallInputsCannotOverflow exercises the per-iteration
// guard via accumulation across multiple inputs.
func TestCheckInputs_ManySmallInputsCannotOverflow(t *testing.T) {
	v := NewValidator(nil, nil)

	half := uint64(maxSatoshis / 2)
	makeInput := func(sats uint64) *sdkTx.TransactionInput {
		in := &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID()}
		in.SetSourceTxOutput(&sdkTx.TransactionOutput{Satoshis: sats, LockingScript: nonDataLockingScript()})
		return in
	}

	tx := &sdkTx.Transaction{
		Inputs: []*sdkTx.TransactionInput{
			makeInput(half),
			makeInput(half),
			makeInput(half),
		},
	}

	err := v.checkInputs(tx)
	if err == nil {
		t.Fatal("expected error for cumulative input satoshis above maxSatoshis, got nil")
	}
	if !errors.Is(err, ErrTxInputTotalSatoshisTooHigh) {
		t.Errorf("expected ErrTxInputTotalSatoshisTooHigh, got %v", err)
	}
}

// parseScript is a small test helper that runs the SDK's default opcode
// parser over the supplied raw script bytes. The parser itself is exercised
// by the SDK's own tests, so any error here indicates a bug in the test
// fixture rather than the code under test.
func parseScript(t *testing.T, raw []byte) interpreter.ParsedScript {
	t.Helper()
	parser := interpreter.DefaultOpcodeParser{}
	s := script.Script(raw)
	parsed, err := parser.Parse(&s)
	if err != nil {
		t.Fatalf("parse script %x: %v", raw, err)
	}
	return parsed
}

// TestCountSigOps_EmptyScript ensures an empty script yields zero sigops.
func TestCountSigOps_EmptyScript(t *testing.T) {
	parsed := parseScript(t, []byte{})
	if got := countSigOps(parsed); got != 0 {
		t.Errorf("empty script: expected 0 sigops, got %d", got)
	}
}

// TestCountSigOps_CheckSig regresses the original behavior: a single
// OP_CHECKSIG and a single OP_CHECKSIGVERIFY each count as one sigop.
func TestCountSigOps_CheckSig(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want int64
	}{
		{"OP_CHECKSIG", []byte{script.OpCHECKSIG}, 1},
		{"OP_CHECKSIGVERIFY", []byte{script.OpCHECKSIGVERIFY}, 1},
		{"two OP_CHECKSIG", []byte{script.OpCHECKSIG, script.OpCHECKSIG}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed := parseScript(t, tc.raw)
			if got := countSigOps(parsed); got != tc.want {
				t.Errorf("countSigOps(%s) = %d, want %d", tc.name, got, tc.want)
			}
		})
	}
}

// TestCountSigOps_CheckMultisigWithSmallInt verifies that an OP_CHECKMULTISIG
// preceded by OP_3 contributes 3 sigops, matching the canonical
// "n-of-m signed by n keys" multisig accounting.
func TestCountSigOps_CheckMultisigWithSmallInt(t *testing.T) {
	parsed := parseScript(t, []byte{script.Op3, script.OpCHECKMULTISIG})
	if got := countSigOps(parsed); got != 3 {
		t.Errorf("OP_3 OP_CHECKMULTISIG: expected 3 sigops, got %d", got)
	}
}

// TestCountSigOps_CheckMultisigDefaults verifies that an OP_CHECKMULTISIG
// with no preceding small-int contributes the standard upper bound of 20
// sigops (MaxPubKeysPerMultiSigBeforeGenesis).
func TestCountSigOps_CheckMultisigDefaults(t *testing.T) {
	parsed := parseScript(t, []byte{script.OpCHECKMULTISIG})
	if got := countSigOps(parsed); got != 20 {
		t.Errorf("bare OP_CHECKMULTISIG: expected 20 sigops, got %d", got)
	}
}

// TestCountSigOps_CheckMultisigVerifyImmediatelyPrecedingSmallInt ensures we
// look at the immediately-preceding opcode (OP_15), not some earlier one
// (OP_3), when assigning sigop weight to OP_CHECKMULTISIGVERIFY.
func TestCountSigOps_CheckMultisigVerifyImmediatelyPrecedingSmallInt(t *testing.T) {
	parsed := parseScript(t, []byte{
		script.Op3, script.OpDROP,
		script.Op15, script.OpCHECKMULTISIGVERIFY,
	})
	if got := countSigOps(parsed); got != 15 {
		t.Errorf("OP_3 OP_DROP OP_15 OP_CHECKMULTISIGVERIFY: expected 15 sigops, got %d", got)
	}
}

// TestCountSigOps_MixedCheckSigAndMultisig combines OP_CHECKSIG and a
// small-int weighted OP_CHECKMULTISIG to confirm the sigops are summed.
func TestCountSigOps_MixedCheckSigAndMultisig(t *testing.T) {
	parsed := parseScript(t, []byte{
		script.OpCHECKSIG,
		script.Op2, script.OpCHECKMULTISIG,
	})
	if got := countSigOps(parsed); got != 3 {
		t.Errorf("OP_CHECKSIG OP_2 OP_CHECKMULTISIG: expected 3 sigops, got %d", got)
	}
}

// TestCountSigOps_OpZeroIsNotSmallInt confirms OP_0 does not satisfy the
// small-int branch (it is 0x00, not 0x51..0x60), and so an
// OP_CHECKMULTISIG preceded by OP_0 falls back to the default 20.
func TestCountSigOps_OpZeroIsNotSmallInt(t *testing.T) {
	parsed := parseScript(t, []byte{script.Op0, script.OpCHECKMULTISIG})
	if got := countSigOps(parsed); got != 20 {
		t.Errorf("OP_0 OP_CHECKMULTISIG: expected 20 sigops (no small-int), got %d", got)
	}
}

// TestCheckInputs_ValidPasses confirms a legitimate transaction is still
// accepted by checkInputs after the overflow guard is added.
func TestCheckInputs_ValidPasses(t *testing.T) {
	v := NewValidator(nil, nil)

	makeInput := func(sats uint64) *sdkTx.TransactionInput {
		in := &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID()}
		in.SetSourceTxOutput(&sdkTx.TransactionOutput{Satoshis: sats, LockingScript: nonDataLockingScript()})
		return in
	}

	tx := &sdkTx.Transaction{
		Inputs: []*sdkTx.TransactionInput{
			makeInput(1_000),
			makeInput(2_000),
		},
	}

	if err := v.checkInputs(tx); err != nil {
		t.Fatalf("expected no error for valid inputs, got %v", err)
	}
}
