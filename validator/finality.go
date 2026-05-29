package validator

import (
	"context"
	"errors"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// lockTimeThreshold is the boundary (teranode util.ValidLockTime) below which
// nLockTime is interpreted as a block height and at or above which it is
// interpreted as a Unix timestamp.
const lockTimeThreshold = 500_000_000

// ErrTxNotFinal is returned when a transaction is non-final: its nLockTime has
// not yet been reached and at least one input has a non-final sequence number,
// so the transaction could still be replaced before its locktime elapses and
// is therefore ineligible for the next block.
var ErrTxNotFinal = errors.New("transaction is not final")

// ChainTip supplies the current best block height, used to evaluate
// height-based nLockTime finality. The store satisfies this interface. It is
// optional: when no ChainTip is wired the height-locked case cannot be
// evaluated and is accepted rather than rejected (graceful degradation), which
// matches arcade's intake stance of not owning a chaintracker.
type ChainTip interface {
	GetActiveTipBlockHeight(ctx context.Context) (uint64, error)
}

// allInputsFinal reports whether every input has a final sequence number
// (0xffffffff), which makes the transaction final regardless of its nLockTime.
func allInputsFinal(tx *sdkTx.Transaction) bool {
	for _, in := range tx.Inputs {
		if in.SequenceNumber != sdkTx.MaxTxInSequenceNum {
			return false
		}
	}
	return true
}

// validLockTime mirrors teranode util.ValidLockTime: an nLockTime below
// lockTimeThreshold is satisfied when blockHeight >= lockTime; at or above the
// threshold it is a timestamp satisfied when blockTime >= lockTime.
// blockHeight and blockTime are the values of the block in which the
// transaction would be mined. Since BIP113 the time-based comparison is
// against the 11-block median-time-past, not the block timestamp itself.
func validLockTime(lockTime, blockHeight, blockTime uint32) bool {
	if lockTime < lockTimeThreshold {
		return blockHeight >= lockTime
	}
	return blockTime >= lockTime
}

// IsFinal mirrors teranode util.IsTransactionFinal (consensus rule TNJ-13): a
// transaction is final when either of the following holds:
//   - the sequence number of every input is final (0xffffffff), or
//   - nLockTime is zero, or below lockTimeThreshold and not greater than
//     blockHeight, or at/above lockTimeThreshold and not greater than blockTime.
//
// blockHeight and blockTime are the values of the block in which the
// transaction would be mined.
func IsFinal(tx *sdkTx.Transaction, blockHeight, blockTime uint32) bool {
	if allInputsFinal(tx) {
		return true
	}
	if tx.LockTime == 0 {
		return true
	}
	return validLockTime(tx.LockTime, blockHeight, blockTime)
}

// checkFinality enforces transaction finality at submit time, sourcing the
// chain height (for height-based locktimes) from the configured ChainTip and
// the wall clock (an upper bound on median-time-past) for time-based
// locktimes. Cases that need chain state we cannot obtain are accepted rather
// than rejected so a transient chain-tip read failure never blocks a
// submission. Returns ErrTxNotFinal for a provably non-final transaction.
func (v *Validator) checkFinality(ctx context.Context, tx *sdkTx.Transaction) error {
	// Decidable without chain state: all-inputs-final or nLockTime == 0.
	if allInputsFinal(tx) || tx.LockTime == 0 {
		return nil
	}

	if tx.LockTime < lockTimeThreshold {
		// Height-based locktime: needs the current chain height.
		if v.chainTip == nil {
			return nil
		}
		h, err := v.chainTip.GetActiveTipBlockHeight(ctx)
		if err != nil {
			return nil
		}
		// A submitted tx targets the next block, so evaluate against height+1.
		if validLockTime(tx.LockTime, uint32(h+1), 0) {
			return nil
		}
		return ErrTxNotFinal
	}

	// Time-based locktime: wall clock is >= median-time-past, so a lock that
	// has elapsed against now() has certainly elapsed against MTP.
	now := v.now
	if now == nil {
		now = func() int64 { return time.Now().Unix() }
	}
	if validLockTime(tx.LockTime, 0, uint32(now())) {
		return nil
	}
	return ErrTxNotFinal
}
