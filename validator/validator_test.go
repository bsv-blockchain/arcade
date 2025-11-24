package validator

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

func TestNewValidator(t *testing.T) {
	v := NewValidator(nil)
	if v == nil {
		t.Fatal("expected validator to be created")
	}
	if v.policy == nil {
		t.Fatal("expected default policy to be set")
	}
	if v.policy.MaxTxSizePolicy != maxBlockSize {
		t.Errorf("expected MaxTxSizePolicy=%d, got %d", maxBlockSize, v.policy.MaxTxSizePolicy)
	}
}

func TestNewValidator_CustomPolicy(t *testing.T) {
	policy := &Policy{
		MaxTxSizePolicy:         1000000,
		MaxTxSigopsCountsPolicy: 10000,
		MaxScriptSizePolicy:     100000,
		MinFeePerKB:             100,
		EnableFeeCheck:          true,
		EnableScriptExecution:   true,
	}

	v := NewValidator(policy)
	if v.policy.MaxTxSizePolicy != 1000000 {
		t.Errorf("expected custom policy MaxTxSizePolicy=1000000, got %d", v.policy.MaxTxSizePolicy)
	}
}

func TestGetTxID(t *testing.T) {
	v := NewValidator(nil)

	// Simple P2PKH transaction hex
	rawTxHex := "0100000001a15d57094aa7a21a28cb20b59aab8fc7d1149a3bdbcddba9c622e4f5f6a99ece010000006b483045022100f3581e1972ae8ac7c7367a7a253bc1135223adb9a468bb3a59233f45bc578380022059af01ca17d00e41837a1d58e97aa31bae584edec28d35bd96923690913bae9a012103b0bd634234abbb1ba1e986e884185c61cf43e001f9137f23c2c409273eb16e65ffffffff02404b4c00000000001976a91404ff367be719efa79d76e4416ffb072cd53b208888acde94a905000000001976a91404ff367be719efa79d76e4416ffb072cd53b208888ac00000000"
	rawTx, err := hex.DecodeString(rawTxHex)
	if err != nil {
		t.Fatalf("failed to decode hex: %v", err)
	}

	txID, err := v.GetTxID(rawTx)
	if err != nil {
		t.Fatalf("GetTxID failed: %v", err)
	}

	if len(txID) != 64 {
		t.Errorf("expected txID length 64, got %d", len(txID))
	}
	if txID == "" {
		t.Error("expected non-empty txID")
	}

	// Verify it's a valid hex string
	_, err = hex.DecodeString(txID)
	if err != nil {
		t.Errorf("txID is not valid hex: %v", err)
	}
}

func TestValidateTransaction_ValidRealTransaction(t *testing.T) {
	v := NewValidator(nil)

	// Real P2PKH transaction
	rawTxHex := "0100000001a15d57094aa7a21a28cb20b59aab8fc7d1149a3bdbcddba9c622e4f5f6a99ece010000006b483045022100f3581e1972ae8ac7c7367a7a253bc1135223adb9a468bb3a59233f45bc578380022059af01ca17d00e41837a1d58e97aa31bae584edec28d35bd96923690913bae9a012103b0bd634234abbb1ba1e986e884185c61cf43e001f9137f23c2c409273eb16e65ffffffff02404b4c00000000001976a91404ff367be719efa79d76e4416ffb072cd53b208888acde94a905000000001976a91404ff367be719efa79d76e4416ffb072cd53b208888ac00000000"
	rawTx, err := hex.DecodeString(rawTxHex)
	if err != nil {
		t.Fatalf("failed to decode hex: %v", err)
	}

	err = v.ValidateTransaction(rawTx)
	if err != nil {
		t.Errorf("expected valid transaction to pass, got error: %v", err)
	}
}

func TestValidateTransaction_CoinbaseInput(t *testing.T) {
	v := NewValidator(nil)

	// Create transaction with coinbase input
	tx := sdkTx.NewTransaction()
	coinbaseTxID, _ := hex.DecodeString(coinbaseTxID)
	hash, _ := chainhash.NewHash(coinbaseTxID)

	tx.AddInput(&sdkTx.TransactionInput{
		SourceTXID:      hash,
		SourceTxOutIndex: 0,
		UnlockingScript: script.NewFromBytes([]byte{0x00}),
		SequenceNumber:  sdkTx.DefaultSequenceNumber,
	})

	tx.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      5000,
		LockingScript: script.NewFromBytes([]byte{0x76, 0xa9, 0x14}),
	})

	rawTx := tx.Bytes()
	err := v.ValidateTransaction(rawTx)
	if !errors.Is(err, ErrTxInputInvalid) {
		t.Errorf("expected ErrTxInputInvalid for coinbase, got %v", err)
	}
}

func TestValidateTransaction_OutputBelowDustLimit(t *testing.T) {
	v := NewValidator(nil)

	tx := sdkTx.NewTransaction()
	nonZeroTxID := make([]byte, 32)
	nonZeroTxID[0] = 0x01
	hash, _ := chainhash.NewHash(nonZeroTxID)

	tx.AddInput(&sdkTx.TransactionInput{
		SourceTXID:       hash,
		SourceTxOutIndex: 0,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
		SequenceNumber:   sdkTx.DefaultSequenceNumber,
	})

	// Output with 0 satoshis (below dust limit) for non-OP_RETURN
	tx.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      0,
		LockingScript: script.NewFromBytes([]byte{0x76, 0xa9, 0x14}), // Not OP_RETURN
	})

	rawTx := tx.Bytes()
	err := v.ValidateTransaction(rawTx)
	if !errors.Is(err, ErrTxOutputInvalid) {
		t.Errorf("expected ErrTxOutputInvalid for dust, got %v", err)
	}
}

func TestValidateTransaction_OutputSatoshisTooHigh(t *testing.T) {
	v := NewValidator(nil)

	tx := sdkTx.NewTransaction()
	nonZeroTxID := make([]byte, 32)
	nonZeroTxID[0] = 0x01
	hash, _ := chainhash.NewHash(nonZeroTxID)

	tx.AddInput(&sdkTx.TransactionInput{
		SourceTXID:       hash,
		SourceTxOutIndex: 0,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
		SequenceNumber:   sdkTx.DefaultSequenceNumber,
	})

	// Output exceeding max satoshis
	tx.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      maxSatoshis + 1,
		LockingScript: script.NewFromBytes([]byte{0x76, 0xa9, 0x14}),
	})

	rawTx := tx.Bytes()
	err := v.ValidateTransaction(rawTx)
	if !errors.Is(err, ErrTxOutputInvalid) {
		t.Errorf("expected ErrTxOutputInvalid, got %v", err)
	}
}

func TestValidateTransaction_TxSizeExceedsPolicy(t *testing.T) {
	v := NewValidator(&Policy{
		MaxTxSizePolicy: 100, // Very small limit
	})

	tx := sdkTx.NewTransaction()
	nonZeroTxID := make([]byte, 32)
	nonZeroTxID[0] = 0x01
	hash, _ := chainhash.NewHash(nonZeroTxID)

	tx.AddInput(&sdkTx.TransactionInput{
		SourceTXID:       hash,
		SourceTxOutIndex: 0,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
		SequenceNumber:   sdkTx.DefaultSequenceNumber,
	})

	// Add multiple outputs to exceed size limit
	for i := 0; i < 10; i++ {
		tx.AddOutput(&sdkTx.TransactionOutput{
			Satoshis:      5000,
			LockingScript: script.NewFromBytes([]byte{0x76, 0xa9, 0x14}),
		})
	}

	rawTx := tx.Bytes()
	err := v.ValidateTransaction(rawTx)
	if !errors.Is(err, ErrTxSizeGreaterThanMax) {
		t.Errorf("expected ErrTxSizeGreaterThanMax, got %v", err)
	}
}

func TestValidateTransaction_NoInputs(t *testing.T) {
	v := NewValidator(nil)

	tx := sdkTx.NewTransaction()
	// No inputs added

	tx.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      5000,
		LockingScript: script.NewFromBytes([]byte{0x76, 0xa9, 0x14}),
	})

	rawTx := tx.Bytes()
	err := v.ValidateTransaction(rawTx)
	if !errors.Is(err, ErrNoInputsOrOutputs) {
		t.Errorf("expected ErrNoInputsOrOutputs, got %v", err)
	}
}

func TestValidateTransaction_NoOutputs(t *testing.T) {
	v := NewValidator(nil)

	tx := sdkTx.NewTransaction()
	nonZeroTxID := make([]byte, 32)
	nonZeroTxID[0] = 0x01
	hash, _ := chainhash.NewHash(nonZeroTxID)

	tx.AddInput(&sdkTx.TransactionInput{
		SourceTXID:       hash,
		SourceTxOutIndex: 0,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
		SequenceNumber:   sdkTx.DefaultSequenceNumber,
	})
	// No outputs added

	rawTx := tx.Bytes()
	err := v.ValidateTransaction(rawTx)
	if !errors.Is(err, ErrNoInputsOrOutputs) {
		t.Errorf("expected ErrNoInputsOrOutputs, got %v", err)
	}
}
