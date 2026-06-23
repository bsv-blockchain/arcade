package validator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	tnerr "github.com/bsv-blockchain/teranode/errors"
	tnvalidator "github.com/bsv-blockchain/teranode/services/validator"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

// --- construction --------------------------------------------------------

func TestNewValidator_Defaults(t *testing.T) {
	v := NewValidator(nil)
	if v.MinFeePerKB() != DefaultMinFeePerKB {
		t.Errorf("expected default min fee %d, got %d", DefaultMinFeePerKB, v.MinFeePerKB())
	}
}

func TestNewValidator_CustomMinFee(t *testing.T) {
	minFee := uint64(50)
	v := NewValidator(&Policy{MinFeePerKB: &minFee})
	if v.MinFeePerKB() != 50 {
		t.Errorf("expected 50, got %d", v.MinFeePerKB())
	}
}

// TestNewValidator_PreservesExplicitZeroFee guards the accept_zero_fee invariant
// that validatorPolicyFromConfig (app/app.go) relies on: a non-nil pointer to a
// zero value must be preserved verbatim rather than substituted with the default.
func TestNewValidator_PreservesExplicitZeroFee(t *testing.T) {
	zero := uint64(0)
	v := NewValidator(&Policy{MinFeePerKB: &zero})
	if v.MinFeePerKB() != 0 {
		t.Errorf("expected MinFeePerKB=0, got %d", v.MinFeePerKB())
	}
}

func TestNewValidatorForNetwork_KnownNetworks(t *testing.T) {
	for _, n := range []string{"mainnet", "testnet", "teratestnet", "regtest"} {
		if _, err := NewValidatorForNetwork(n, nil); err != nil {
			t.Errorf("network %q: unexpected error %v", n, err)
		}
	}
}

func TestNewValidatorForNetwork_UnknownNetwork(t *testing.T) {
	if _, err := NewValidatorForNetwork("nope", nil); err == nil {
		t.Fatal("expected error for unknown network, got nil")
	}
}

func TestSatPerKBToBSVPerKB(t *testing.T) {
	// 100 sat/kB == 0.000001 BSV/kB; teranode converts back via *1e8/1000.
	if got := satPerKBToBSVPerKB(100); got != 0.000001 {
		t.Errorf("expected 0.000001, got %v", got)
	}
	if got := satPerKBToBSVPerKB(0); got != 0 {
		t.Errorf("expected 0, got %v", got)
	}
}

// --- conversion ----------------------------------------------------------

func nonZeroSourceTXID() *chainhash.Hash {
	h := chainhash.Hash{}
	h[0] = 0x01
	return &h
}

func scriptFromBytes(b []byte) *script.Script {
	s := script.Script(b)
	return &s
}

func TestToExtendedBT_PopulatesSourceData(t *testing.T) {
	in := &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID()}
	in.UnlockingScript = scriptFromBytes([]byte{script.Op0})
	in.SetSourceTxOutput(&sdkTx.TransactionOutput{Satoshis: 1234, LockingScript: scriptFromBytes([]byte{script.Op1})})

	tx := &sdkTx.Transaction{
		Version: 2,
		Inputs:  []*sdkTx.TransactionInput{in},
		Outputs: []*sdkTx.TransactionOutput{{Satoshis: 1000, LockingScript: scriptFromBytes([]byte{script.Op1})}},
	}

	btx, err := toExtendedBT(tx)
	if err != nil {
		t.Fatalf("toExtendedBT: %v", err)
	}
	if len(btx.Inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(btx.Inputs))
	}
	if btx.Inputs[0].PreviousTxSatoshis != 1234 {
		t.Errorf("PreviousTxSatoshis = %d, want 1234", btx.Inputs[0].PreviousTxSatoshis)
	}
	if btx.Inputs[0].PreviousTxScript == nil || len(*btx.Inputs[0].PreviousTxScript) != 1 {
		t.Errorf("PreviousTxScript not populated: %v", btx.Inputs[0].PreviousTxScript)
	}
}

func TestToExtendedBT_MissingSourceData(t *testing.T) {
	in := &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID()}
	in.UnlockingScript = scriptFromBytes([]byte{script.Op0})
	// No source output set.

	tx := &sdkTx.Transaction{
		Version: 2,
		Inputs:  []*sdkTx.TransactionInput{in},
		Outputs: []*sdkTx.TransactionOutput{{Satoshis: 1000, LockingScript: scriptFromBytes([]byte{script.Op1})}},
	}

	_, err := toExtendedBT(tx)
	if !errors.Is(err, errMissingSourceData) {
		t.Fatalf("expected errMissingSourceData, got %v", err)
	}
}

// --- error mapping -------------------------------------------------------

func TestMapTeranodeError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want arcerrors.StatusCode
	}{
		{"missing source data", errMissingSourceData, arcerrors.StatusTxFormat},
		{"fee too low", tnerr.NewTxInvalidError("transaction fee is too low: 1 < 10 required"), arcerrors.StatusFees},
		{"input < output", tnerr.NewTxInvalidError("transaction input satoshis is less than output satoshis: 1 < 2"), arcerrors.StatusFees},
		{"coinbase", tnerr.NewTxInvalidError("transaction input 0 is a coinbase input"), arcerrors.StatusInputs},
		{"inputs too large", tnerr.NewTxPolicyError("bad-txns-inputs-too-large"), arcerrors.StatusInputs},
		{"bdk script", tnerr.NewTxInvalidError("GoBDK fail to ValidateTransaction", tnerr.NewTxPolicyError("GoBDK fail to ValidateTransaction by policy settings")), arcerrors.StatusUnlockingScripts},
		{"cgo processing", tnerr.NewProcessingError("GoBDK fail to ValidateTransaction"), arcerrors.StatusGeneric},
		{"invalid argument", tnerr.NewInvalidArgumentError("bad height"), arcerrors.StatusMalformed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mapped := mapTeranodeError(tc.err)
			arcErr := arcerrors.GetArcError(mapped)
			if arcErr == nil {
				t.Fatalf("expected ArcError, got %v", mapped)
			}
			if arcErr.StatusCode != tc.want {
				t.Errorf("status = %d, want %d (err=%v)", arcErr.StatusCode, tc.want, mapped)
			}
			if arcErr.ExtraInfo == "" {
				t.Error("expected ExtraInfo to carry the reason string")
			}
		})
	}
}

func TestMapTeranodeError_Nil(t *testing.T) {
	if mapTeranodeError(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

// --- end-to-end BDK validation (#192 Chronicle regression) ---------------

// spendableSource builds an input that spends a fabricated previous output with
// the given locking script and satoshis. The prev txid is non-zero so the input
// is not treated as a coinbase input.
func spendableSource(unlocking []byte, prevSats uint64, prevLock []byte) *sdkTx.TransactionInput {
	in := &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID(), SourceTxOutIndex: 0}
	in.UnlockingScript = scriptFromBytes(unlocking)
	in.SetSourceTxOutput(&sdkTx.TransactionOutput{Satoshis: prevSats, LockingScript: scriptFromBytes(prevLock)})
	return in
}

// pushDropUnlocking returns an unlocking script that pushes n bytes then OP_DROP
// — a functional (non-push-only) opcode that leaves the stack empty, so paired
// with an OP_TRUE locking script the combined script evaluates to true. Because
// it is non-push-only it is only accepted for version>=2 (post-Chronicle)
// transactions (issue #192). AppendPushData emits the minimal push encoding
// (direct, OP_PUSHDATA1 or OP_PUSHDATA2), so element sizes above the historical
// 520-byte pre-Genesis limit are exercised faithfully (issue #214).
func pushDropUnlocking(n int) []byte {
	s := &script.Script{}
	if err := s.AppendPushData(make([]byte, n)); err != nil {
		panic(err)
	}
	if err := s.AppendOpcodes(script.OpDROP); err != nil {
		panic(err)
	}
	return *s
}

// nonPushUnlocking pushes a 50-byte blob then OP_DROP. The leading data push also
// pads the transaction comfortably past any minimum-size rule. This is the exact
// shape the Chronicle upgrade re-allowed for version>=2 transactions (issue #192).
func nonPushUnlocking() []byte {
	return pushDropUnlocking(50)
}

func chronicleTx(version uint32) *sdkTx.Transaction {
	// prev output: OP_TRUE (anyone-can-spend), 100_000 sats so fees are easily covered.
	in := spendableSource(nonPushUnlocking(), 100_000, []byte{script.OpTRUE})
	return &sdkTx.Transaction{
		Version: version,
		Inputs:  []*sdkTx.TransactionInput{in},
		Outputs: []*sdkTx.TransactionOutput{{Satoshis: 90_000, LockingScript: scriptFromBytes([]byte{script.OpTRUE})}},
	}
}

// TestValidate_ChronicleVersionGate is the core #192 regression, driven through
// the real BDK engine: a non-push-only unlocking script must be rejected for
// version<2 (pre-Chronicle push-only rule) and accepted for version>=2.
func TestValidate_ChronicleVersionGate(t *testing.T) {
	v := NewValidator(nil)
	ctx := context.Background()

	if err := v.ValidateTransaction(ctx, chronicleTx(2), false); err != nil {
		t.Fatalf("version 2 non-push unlocking script must be accepted, got %v", err)
	}
	if err := v.ValidateTransaction(ctx, chronicleTx(3), false); err != nil {
		t.Fatalf("version 3 non-push unlocking script must be accepted, got %v", err)
	}

	err := v.ValidateTransaction(ctx, chronicleTx(1), false)
	if err == nil {
		t.Fatal("version 1 non-push unlocking script must be rejected")
	}
	if arcErr := arcerrors.GetArcError(err); arcErr == nil || arcErr.StatusCode != arcerrors.StatusUnlockingScripts {
		t.Errorf("expected StatusUnlockingScripts, got %v", err)
	}
}

// largePushTx builds a version-2 transaction whose single input spends an OP_TRUE
// (anyone-can-spend) source output with a <pushSize-byte push> OP_DROP unlocking
// script. The input is funded generously so fees can never be the reason a size is
// rejected — the only variable under test is the pushed-element size.
func largePushTx(pushSize int) *sdkTx.Transaction {
	in := spendableSource(pushDropUnlocking(pushSize), 100_000, []byte{script.OpTRUE})
	return &sdkTx.Transaction{
		Version: 2,
		Inputs:  []*sdkTx.TransactionInput{in},
		Outputs: []*sdkTx.TransactionOutput{{Satoshis: 10_000, LockingScript: scriptFromBytes([]byte{script.OpTRUE})}},
	}
}

// TestValidate_PostGenesisLargePushes is the #214 regression, driven through the
// real BDK engine. The historical 520-byte MAX_SCRIPT_ELEMENT_SIZE_BEFORE_GENESIS
// limit is keyed off the *source UTXO height*, not the spending block height. Arcade
// reported height 1 (pre-Genesis) for every input, so BDK applied the limit to
// post-Genesis spends and rejected otherwise-valid transactions that push >520-byte
// elements (the same transactions ARC accepts and miners mine).
//
// The fix reports the unknown-parent sentinel instead, which teranode resolves to the
// post-Genesis candidate block height in policy mode — so the limit no longer applies.
// Each case proves the rejection is purely a function of the reported source height:
// at pre-Genesis height 1 BDK rejects >520-byte pushes, at a post-Genesis height it
// accepts them, and arcade's public intake (which now reports the sentinel) accepts.
func TestValidate_PostGenesisLargePushes(t *testing.T) {
	v := NewValidator(nil)
	ctx := context.Background()

	// A concrete, unambiguously post-Genesis source height (well above every
	// network's Genesis/Chronicle activation height, at/below the spending height).
	const postGenesisHeight = allForksActiveHeight - 1

	for _, size := range []int{520, 521, 5210} {
		t.Run(fmt.Sprintf("push_%d_bytes", size), func(t *testing.T) {
			tx := largePushTx(size)
			overLimit := size > 520

			// Root-cause control: call the BDK validator directly, varying only the
			// source UTXO height. Use the fee-agnostic validator so fees never
			// interfere; NewDefaultOptions keeps it in policy mode (arcade's mode).
			btx, err := toExtendedBT(tx)
			if err != nil {
				t.Fatalf("toExtendedBT: %v", err)
			}
			preGenesisErr := v.tvNoFee.ValidateTransaction(btx, allForksActiveHeight, []uint32{1}, tnvalidator.NewDefaultOptions())
			postGenesisErr := v.tvNoFee.ValidateTransaction(btx, allForksActiveHeight, []uint32{postGenesisHeight}, tnvalidator.NewDefaultOptions())

			if overLimit && preGenesisErr == nil {
				t.Errorf("BDK must reject a %d-byte push at pre-Genesis source height 1 (pre-Genesis 520-byte limit)", size)
			}
			if !overLimit && preGenesisErr != nil {
				t.Errorf("BDK must accept a %d-byte push even at source height 1, got %v", size, preGenesisErr)
			}
			if postGenesisErr != nil {
				t.Errorf("BDK must accept a %d-byte push at a post-Genesis source height, got %v", size, postGenesisErr)
			}

			// Regression guard: arcade's public intake must accept all sizes. Before
			// the fix this fails for 521 and 5210 with a push-value-size error.
			if err := v.ValidateTransaction(ctx, tx, true); err != nil {
				t.Errorf("arcade intake must accept a %d-byte post-Genesis push, got %v", size, err)
			}
		})
	}
}

// TestValidate_FeeTooLow confirms an underpaid transaction is rejected with the
// fee status. The validator is built with a deliberately high fee floor so the
// 1-satoshi fee (a 100_000-sat input spent to a 99_999-sat output) is
// unambiguously below it regardless of BDK's default fee policy.
func TestValidate_FeeTooLow(t *testing.T) {
	highFee := uint64(1_000_000) // 1M sat/kB — any realistic tx underpays.
	v := NewValidator(&Policy{MinFeePerKB: &highFee})
	in := spendableSource(nonPushUnlocking(), 100_000, []byte{script.OpTRUE})
	tx := &sdkTx.Transaction{
		Version: 2,
		Inputs:  []*sdkTx.TransactionInput{in},
		Outputs: []*sdkTx.TransactionOutput{{Satoshis: 99_999, LockingScript: scriptFromBytes([]byte{script.OpTRUE})}},
	}

	err := v.ValidateTransaction(context.Background(), tx, false)
	if err == nil {
		t.Fatal("underpaid tx must be rejected with a fee error")
	}
	if arcErr := arcerrors.GetArcError(err); arcErr == nil || arcErr.StatusCode != arcerrors.StatusFees {
		t.Errorf("expected StatusFees, got %v", err)
	}

	// skipFees must accept the same transaction (tvNoFee ignores the floor).
	if err := v.ValidateTransaction(context.Background(), tx, true); err != nil {
		t.Errorf("skipFees should accept underpaid tx, got %v", err)
	}
}
