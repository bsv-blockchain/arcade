package finality

import (
	"errors"
	"strings"
	"testing"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// txWith builds a transaction with the given locktime and one input per
// sequence number.
func txWith(lockTime uint32, sequences ...uint32) *sdkTx.Transaction {
	tx := &sdkTx.Transaction{Version: 2, LockTime: lockTime}
	for _, seq := range sequences {
		tx.Inputs = append(tx.Inputs, &sdkTx.TransactionInput{SequenceNumber: seq})
	}
	return tx
}

func TestIsTransactionFinal(t *testing.T) {
	const (
		finalSeq    = uint32(0xffffffff)
		nonFinalSeq = uint32(0xfffffffe)
	)

	tests := []struct {
		name            string
		tx              *sdkTx.Transaction
		nextBlockHeight uint32
		medianTimePast  uint32
		wantFinal       bool
		wantHeightBased bool
	}{
		{
			// Regression vector from bsv-blockchain/arcade#245: timestamp
			// locktime ahead of MTP with a non-final input sequence.
			name:            "issue 245 timestamp locktime ahead of MTP is not final",
			tx:              txWith(1783806110, nonFinalSeq, finalSeq),
			nextBlockHeight: 957458,
			medianTimePast:  1783805456,
			wantFinal:       false,
			wantHeightBased: false,
		},
		{
			name:            "all input sequences final ignores future locktime",
			tx:              txWith(1783806110, finalSeq, finalSeq),
			nextBlockHeight: 957458,
			medianTimePast:  1783805456,
			wantFinal:       true,
		},
		{
			name:            "locktime zero is final despite non-final sequence",
			tx:              txWith(0, nonFinalSeq),
			nextBlockHeight: 100,
			medianTimePast:  100,
			wantFinal:       true,
		},
		{
			name:            "one non-final sequence among final ones enforces locktime",
			tx:              txWith(1783806110, finalSeq, nonFinalSeq, finalSeq),
			nextBlockHeight: 957458,
			medianTimePast:  1783805456,
			wantFinal:       false,
			wantHeightBased: false,
		},
		{
			name:            "timestamp locktime equal to MTP is not final (strict)",
			tx:              txWith(1783806110, nonFinalSeq),
			nextBlockHeight: 957458,
			medianTimePast:  1783806110,
			wantFinal:       false,
			wantHeightBased: false,
		},
		{
			name:            "timestamp locktime below MTP is final",
			tx:              txWith(1783806110, nonFinalSeq),
			nextBlockHeight: 957458,
			medianTimePast:  1783806111,
			wantFinal:       true,
		},
		{
			name:            "height locktime below next height is final",
			tx:              txWith(900100, nonFinalSeq),
			nextBlockHeight: 900101,
			medianTimePast:  0,
			wantFinal:       true,
		},
		{
			name:            "height locktime equal to next height is not final (strict)",
			tx:              txWith(900101, nonFinalSeq),
			nextBlockHeight: 900101,
			medianTimePast:  0,
			wantFinal:       false,
			wantHeightBased: true,
		},
		{
			name:            "locktime just below threshold uses height branch",
			tx:              txWith(499999999, nonFinalSeq),
			nextBlockHeight: 100,
			medianTimePast:  4000000000,
			wantFinal:       false,
			wantHeightBased: true,
		},
		{
			name:            "locktime at threshold uses timestamp branch",
			tx:              txWith(500000000, nonFinalSeq),
			nextBlockHeight: 4000000000,
			medianTimePast:  100,
			wantFinal:       false,
			wantHeightBased: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IsTransactionFinal(tt.tx, tt.nextBlockHeight, tt.medianTimePast)
			if tt.wantFinal {
				if err != nil {
					t.Fatalf("IsTransactionFinal() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("IsTransactionFinal() = nil, want NotFinalError")
			}
			var nfe *NotFinalError
			if !errors.As(err, &nfe) {
				t.Fatalf("IsTransactionFinal() = %T (%v), want *NotFinalError", err, err)
			}
			if nfe.HeightBased != tt.wantHeightBased {
				t.Errorf("HeightBased = %v, want %v", nfe.HeightBased, tt.wantHeightBased)
			}
			if nfe.LockTime != tt.tx.LockTime {
				t.Errorf("LockTime = %d, want %d", nfe.LockTime, tt.tx.LockTime)
			}
		})
	}
}

func TestIsTransactionFinalNoInputs(t *testing.T) {
	err := IsTransactionFinal(&sdkTx.Transaction{Version: 2, LockTime: 1}, 100, 100)
	if err == nil {
		t.Fatal("IsTransactionFinal() = nil for zero-input tx, want error")
	}
	var nfe *NotFinalError
	if errors.As(err, &nfe) {
		t.Fatalf("zero-input tx should not yield *NotFinalError, got %v", err)
	}
}

func TestNotFinalErrorMessageTimestamp(t *testing.T) {
	err := IsTransactionFinal(txWith(1783806110, 0xfffffffe), 957458, 1783805456)
	if err == nil {
		t.Fatal("want error")
	}
	msg := err.Error()
	for _, want := range []string{
		"transaction is not final", // greps consistently with teranode's UTXO_NON_FINAL
		"1783806110",               // raw locktime
		"2026-07-11T21:41:50Z",     // RFC3339 render of the locktime
		"1783805456",               // current median-time-past
		"~11m",                     // ETA until final (654s rounds up to 11m)
		"0xffffffff",               // remediation hint
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}

func TestNotFinalErrorMessageHeight(t *testing.T) {
	err := IsTransactionFinal(txWith(900123, 0xfffffffe), 900101, 0)
	if err == nil {
		t.Fatal("want error")
	}
	msg := err.Error()
	for _, want := range []string{
		"transaction is not final",
		"900123",     // locktime as height
		"900101",     // next block height
		"23 blocks",  // 900123+1-900101 blocks remaining
		"0xffffffff", // remediation hint
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}
