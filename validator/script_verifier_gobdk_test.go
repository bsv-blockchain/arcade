//go:build cgo

package validator

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

// buildModernP2PKHTx constructs a fully-signed, post-genesis P2PKH spend with a
// default (SIGHASH_ALL|FORKID) signature. It is self-contained — a fresh key, a
// synthetic funding output, and a spend of it — so it validates cleanly under
// arcade's constant post-Chronicle height (where FORKID is mandatory and inputs
// are treated as post-genesis). The returned tx carries its source output, so
// tx.EF() succeeds.
func buildModernP2PKHTx(t *testing.T) *sdkTx.Transaction {
	t.Helper()

	priv, err := ec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new private key: %v", err)
	}
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), true)
	if err != nil {
		t.Fatalf("address: %v", err)
	}
	lock, err := p2pkh.Lock(addr)
	if err != nil {
		t.Fatalf("lock script: %v", err)
	}

	source := &sdkTx.Transaction{Version: 1}
	source.AddOutput(&sdkTx.TransactionOutput{Satoshis: 100_000, LockingScript: lock})

	unlock, err := p2pkh.Unlock(priv, nil) // nil → default SIGHASH_ALL|FORKID
	if err != nil {
		t.Fatalf("unlock template: %v", err)
	}

	spend := &sdkTx.Transaction{Version: 2}
	spend.AddInputFromTx(source, 0, unlock)
	spend.AddOutput(&sdkTx.TransactionOutput{Satoshis: 90_000, LockingScript: lock}) // 10k sat fee
	if err := spend.Sign(); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return spend
}

// zeroFeeValidator builds a mainnet validator with a zero fee floor so tests
// exercise script/structure validation in isolation from fee policy.
func zeroFeeValidator(t *testing.T) *Validator {
	t.Helper()
	zero := uint64(0)
	v, err := NewValidator(&Policy{MinFeePerKB: &zero})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	return v
}

func statusOf(t *testing.T, err error) arcerrors.StatusCode {
	t.Helper()
	var ae *arcerrors.ArcError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *arcerrors.ArcError, got %T: %v", err, err)
	}
	return ae.StatusCode
}

// TestValidate_GoodEFTxPasses is the core success path: a well-formed,
// FORKID-signed extended-format transaction passes go-bdk policy validation at
// arcade's constant post-Chronicle height.
func TestValidate_GoodEFTxPasses(t *testing.T) {
	v := zeroFeeValidator(t)
	tx := buildModernP2PKHTx(t)

	if err := v.ValidateTransaction(context.Background(), tx, false); err != nil {
		t.Fatalf("expected acceptance, got %v", err)
	}
}

// TestValidate_CorruptedScriptRejected flips a byte in the unlocking script so
// signature verification fails inside go-bdk; the failure must surface as the
// unlocking-scripts ARC status.
func TestValidate_CorruptedScriptRejected(t *testing.T) {
	v := zeroFeeValidator(t)
	tx := buildModernP2PKHTx(t)

	// Corrupt a byte inside the DER signature push so the script still parses
	// but verification fails.
	us := *tx.Inputs[0].UnlockingScript
	us[10] ^= 0xff
	tx.Inputs[0].UnlockingScript = &us

	err := v.ValidateTransaction(context.Background(), tx, false)
	if err == nil {
		t.Fatal("expected rejection of corrupted-signature tx")
	}
	if got := statusOf(t, err); got != arcerrors.StatusUnlockingScripts {
		t.Fatalf("status = %d, want %d (StatusUnlockingScripts)", got, arcerrors.StatusUnlockingScripts)
	}
}

// TestValidate_NonExtendedRejected confirms a transaction submitted without
// source-output data (non-EF) is rejected with StatusTxFormat — go-bdk cannot
// verify scripts without the previous locking scripts/satoshis.
func TestValidate_NonExtendedRejected(t *testing.T) {
	v := zeroFeeValidator(t)
	tx := buildModernP2PKHTx(t)

	// Re-parse the non-extended serialization, dropping source outputs.
	plain, err := sdkTx.NewTransactionFromBytes(tx.Bytes())
	if err != nil {
		t.Fatalf("parse plain tx: %v", err)
	}

	err = v.ValidateTransaction(context.Background(), plain, false)
	if err == nil {
		t.Fatal("expected rejection of non-extended tx")
	}
	if !errors.Is(err, ErrNotExtendedFormat) {
		t.Fatalf("expected ErrNotExtendedFormat, got %v", err)
	}
	if got := statusOf(t, err); got != arcerrors.StatusTxFormat {
		t.Fatalf("status = %d, want %d (StatusTxFormat)", got, arcerrors.StatusTxFormat)
	}
}

// TestValidate_CoinbaseRejected confirms coinbase transactions are rejected by
// the thin pre-check before reaching go-bdk.
func TestValidate_CoinbaseRejected(t *testing.T) {
	v := zeroFeeValidator(t)

	cb := &sdkTx.Transaction{Version: 1}
	in := &sdkTx.TransactionInput{SourceTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff}
	in.SourceTXID = &chainhash.Hash{}
	cb.Inputs = append(cb.Inputs, in)

	err := v.ValidateTransaction(context.Background(), cb, false)
	if !errors.Is(err, ErrCoinbaseNotSupported) {
		t.Fatalf("expected ErrCoinbaseNotSupported, got %v", err)
	}
	if got := statusOf(t, err); got != arcerrors.StatusInputs {
		t.Fatalf("status = %d, want %d (StatusInputs)", got, arcerrors.StatusInputs)
	}
}

// TestValidate_LowFeeRejected confirms the fee floor flows through to go-bdk:
// the same modern tx is rejected with StatusFees under a fee floor it cannot
// meet.
func TestValidate_LowFeeRejected(t *testing.T) {
	high := uint64(1_000_000_000) // absurd floor: 1e9 sat/kB
	v, err := NewValidator(&Policy{MinFeePerKB: &high})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	tx := buildModernP2PKHTx(t)

	err = v.ValidateTransaction(context.Background(), tx, false)
	if err == nil {
		t.Fatal("expected fee rejection")
	}
	if got := statusOf(t, err); got != arcerrors.StatusFees {
		t.Fatalf("status = %d, want %d (StatusFees)", got, arcerrors.StatusFees)
	}
}

// TestNewValidator_UnknownNetwork confirms construction fails for an
// unsupported network name rather than silently defaulting.
func TestNewValidator_UnknownNetwork(t *testing.T) {
	if _, err := NewValidator(&Policy{Network: "bogusnet"}); err == nil {
		t.Fatal("expected error for unknown network")
	}
}

// TestNewValidator_DefaultsAccepted confirms a nil policy constructs a working
// mainnet validator that accepts a modern tx paying above the default floor.
func TestNewValidator_DefaultsAccepted(t *testing.T) {
	v, err := NewValidator(nil)
	if err != nil {
		t.Fatalf("NewValidator(nil): %v", err)
	}
	tx := buildModernP2PKHTx(t)
	if err := v.ValidateTransaction(context.Background(), tx, false); err != nil {
		t.Fatalf("expected acceptance under default policy, got %v", err)
	}
}
