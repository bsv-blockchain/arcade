package validator

import (
	"context"
	"errors"
	"testing"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// finalInput returns an input whose sequence number is final (0xffffffff).
func finalInput() *sdkTx.TransactionInput {
	return &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID(), SequenceNumber: sdkTx.MaxTxInSequenceNum}
}

// nonFinalInput returns an input whose sequence number is NOT final, leaving
// the transaction subject to its nLockTime.
func nonFinalInput() *sdkTx.TransactionInput {
	return &sdkTx.TransactionInput{SourceTXID: nonZeroSourceTXID(), SequenceNumber: 0}
}

func TestIsFinal_LockTimeZero(t *testing.T) {
	// LockTime 0 is final regardless of sequence numbers.
	tx := &sdkTx.Transaction{LockTime: 0, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if !IsFinal(tx, 0, 0) {
		t.Error("locktime 0 must be final")
	}
}

func TestIsFinal_AllInputsFinal(t *testing.T) {
	// A future locktime is disabled when every input sequence is final.
	tx := &sdkTx.Transaction{LockTime: 1_000_000_000, Inputs: []*sdkTx.TransactionInput{finalInput(), finalInput()}}
	if !IsFinal(tx, 0, 0) {
		t.Error("all-inputs-final must be final despite future locktime")
	}
}

func TestIsFinal_HeightLockPassed(t *testing.T) {
	// Height-based locktime (< 500000000) that is below the chain height
	// has expired -> final, even with a non-final input.
	tx := &sdkTx.Transaction{LockTime: 800_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if !IsFinal(tx, 800_001, 0) {
		t.Error("height lock in the past must be final")
	}
}

func TestIsFinal_HeightLockNotPassed(t *testing.T) {
	tx := &sdkTx.Transaction{LockTime: 800_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if IsFinal(tx, 799_999, 0) {
		t.Error("future height lock with non-final input must be non-final")
	}
}

func TestIsFinal_TimeLockPassed(t *testing.T) {
	// Time-based locktime (>= 500000000) not greater than blockTime has elapsed.
	tx := &sdkTx.Transaction{LockTime: 1_700_000_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if !IsFinal(tx, 0, uint32(1_700_000_001)) {
		t.Error("time lock in the past must be final")
	}
}

func TestIsFinal_TimeLockNotPassed(t *testing.T) {
	tx := &sdkTx.Transaction{LockTime: 1_700_000_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if IsFinal(tx, 0, uint32(1_699_999_999)) {
		t.Error("future time lock with non-final input must be non-final")
	}
}

func TestIsFinal_TimeLockNotPassedButOneInputFinalEnough(t *testing.T) {
	// Mixed sequences: any single non-final input keeps the tx non-final.
	tx := &sdkTx.Transaction{LockTime: 1_700_000_000, Inputs: []*sdkTx.TransactionInput{finalInput(), nonFinalInput()}}
	if IsFinal(tx, 0, uint32(1_699_999_999)) {
		t.Error("a single non-final input must keep an unexpired tx non-final")
	}
}

func TestIsFinal_ThresholdMinusOne_UsesHeightNotTimestamp(t *testing.T) {
	// lockTime == lockTimeThreshold-1 is still height-based. With blockHeight
	// meeting it and blockTime zero, it must be final — proving the height
	// branch was taken (a timestamp comparison against 0 would reject).
	tx := &sdkTx.Transaction{LockTime: 499_999_999, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if !IsFinal(tx, 499_999_999, 0) {
		t.Error("locktime 499999999 must be evaluated as a block height, not a timestamp")
	}
}

func TestIsFinal_Threshold_UsesTimestampNotHeight(t *testing.T) {
	// lockTime == lockTimeThreshold is the first timestamp-based value. With
	// blockHeight large enough to satisfy a height comparison but blockTime
	// zero, it must be non-final — proving the timestamp branch was taken (a
	// height comparison would accept).
	tx := &sdkTx.Transaction{LockTime: 500_000_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if IsFinal(tx, 500_000_000, 0) {
		t.Error("locktime 500000000 must be evaluated as a timestamp, not a block height")
	}
}

// fakeChainTip implements ChainTip for finality tests.
type fakeChainTip struct {
	height uint64
	err    error
}

func (f fakeChainTip) GetActiveTipBlockHeight(_ context.Context) (uint64, error) {
	return f.height, f.err
}

func TestCheckFinality_NilChainTipSkipsHeightLock(t *testing.T) {
	// Without a chain-tip source the height-based case cannot be evaluated;
	// the validator must not reject it (graceful degradation).
	v := NewValidator(nil)
	tx := &sdkTx.Transaction{LockTime: 800_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if err := v.checkFinality(context.Background(), tx); err != nil {
		t.Errorf("nil chain tip must accept height-locked tx, got %v", err)
	}
}

func TestCheckFinality_RejectsFutureHeightLock(t *testing.T) {
	v := NewValidator(nil)
	v.SetChainTip(fakeChainTip{height: 799_998}) // next block = 799_999, < 800_000
	tx := &sdkTx.Transaction{LockTime: 800_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if err := v.checkFinality(context.Background(), tx); !errors.Is(err, ErrTxNotFinal) {
		t.Errorf("expected ErrTxNotFinal, got %v", err)
	}
}

func TestCheckFinality_AcceptsPassedHeightLock(t *testing.T) {
	v := NewValidator(nil)
	v.SetChainTip(fakeChainTip{height: 800_000}) // next block = 800_001, > 800_000
	tx := &sdkTx.Transaction{LockTime: 800_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if err := v.checkFinality(context.Background(), tx); err != nil {
		t.Errorf("expected acceptance, got %v", err)
	}
}

func TestCheckFinality_RejectsFutureTimeLock(t *testing.T) {
	v := NewValidator(nil)
	v.SetNowFunc(func() int64 { return 1_699_999_999 })
	tx := &sdkTx.Transaction{LockTime: 1_700_000_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if err := v.checkFinality(context.Background(), tx); !errors.Is(err, ErrTxNotFinal) {
		t.Errorf("expected ErrTxNotFinal, got %v", err)
	}
}

func TestCheckFinality_ChainTipErrorAccepts(t *testing.T) {
	// A chain-tip read failure must not block submission.
	v := NewValidator(nil)
	v.SetChainTip(fakeChainTip{err: errors.New("boom")})
	tx := &sdkTx.Transaction{LockTime: 800_000, Inputs: []*sdkTx.TransactionInput{nonFinalInput()}}
	if err := v.checkFinality(context.Background(), tx); err != nil {
		t.Errorf("chain-tip error must accept, got %v", err)
	}
}
