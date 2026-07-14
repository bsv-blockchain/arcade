// Package finality implements the nLockTime/nSequence transaction-finality
// check (BIP113) that teranode applies at mempool admission but arcade's
// embedded BDK validator does not: teranode enforces it one layer above the
// TxValidator arcade calls (teranode services/validator/Validator.go
// validateInternal → util.IsTransactionFinal), and its propagation surface
// strips the UTXO_NON_FINAL reason to a generic "failed to validate
// transaction". Running the same check at arcade intake turns that opaque
// terminal rejection into an immediate, actionable error (issue #245).
//
// The check is deliberately a pure-Go sibling of the validator package, which
// needs CGO/BDK to build; keeping it separate lets it be tested everywhere.
//
// Semantics mirror teranode util/lock_time.go exactly, including the strict
// inequalities: a transaction is final when every input sequence is
// 0xffffffff, when nLockTime is zero, or when nLockTime is exceeded by the
// comparison value — the next block height for lock times below 500000000,
// the chain tip's median-time-past (BIP113, NOT wall-clock time, which runs
// 1–2.5h ahead of MTP on mainnet) for lock times at or above it.
package finality

import (
	"errors"
	"fmt"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// lockTimeThreshold is the nLockTime value below which the lock is a block
// height and at/above which it is a unix timestamp.
const lockTimeThreshold uint32 = 500_000_000

// targetBlockSeconds is the expected mainnet block interval, used only to
// humanize the ETA of height-based locks.
const targetBlockSeconds = 600

// NotFinalError reports a transaction that is provably non-final against the
// supplied chain context. It renders an actionable message telling the
// submitter when the transaction becomes broadcastable and how to opt out of
// locktime semantics entirely.
type NotFinalError struct {
	// LockTime is the transaction's nLockTime.
	LockTime uint32
	// HeightBased is true when LockTime is below lockTimeThreshold and was
	// compared against NextBlockHeight rather than MedianTimePast.
	HeightBased bool
	// NextBlockHeight is the height the transaction would be mined at
	// (chain tip + 1), the comparison value for height-based locks.
	NextBlockHeight uint32
	// MedianTimePast is the chain tip's BIP113 median-time-past, the
	// comparison value for timestamp-based locks.
	MedianTimePast uint32
}

const remediation = "resubmit then, or set all input sequence numbers to 0xffffffff (or nLockTime to 0) if immediate broadcast is intended"

func (e *NotFinalError) Error() string {
	if e.HeightBased {
		blocks := uint64(e.LockTime) + 1 - uint64(e.NextBlockHeight)
		return fmt.Sprintf(
			"transaction is not final: nLockTime %d is a block height and the next block height must exceed it before broadcast; the next block is %d, ~%d blocks (~%s) remaining; %s",
			e.LockTime, e.NextBlockHeight, blocks, humanizeSeconds(blocks*targetBlockSeconds), remediation,
		)
	}
	wait := uint64(e.LockTime) + 1 - uint64(e.MedianTimePast)
	return fmt.Sprintf(
		"transaction is not final: nLockTime %d (%s) is a timestamp and the chain's median-time-past must exceed it before broadcast; current median-time-past is %d (%s), expected to become final in ~%s; %s",
		e.LockTime, rfc3339(e.LockTime), e.MedianTimePast, rfc3339(e.MedianTimePast), humanizeSeconds(wait), remediation,
	)
}

// IsTransactionFinal mirrors teranode's util.IsTransactionFinal (consensus
// rule TNJ-13) on go-sdk types. nextBlockHeight is the height the transaction
// would be mined at (chain tip + 1); medianTimePast is the tip's BIP113 MTP.
// It returns nil for a final transaction and a *NotFinalError otherwise.
func IsTransactionFinal(tx *sdkTx.Transaction, nextBlockHeight, medianTimePast uint32) error {
	if len(tx.Inputs) == 0 {
		return errors.New("transactions with no inputs are not valid, and therefore not final")
	}

	allSequencesFinal := true
	for _, input := range tx.Inputs {
		allSequencesFinal = allSequencesFinal && input.SequenceNumber == sdkTx.DefaultSequenceNumber
	}
	if allSequencesFinal {
		return nil
	}

	if tx.LockTime == 0 {
		return nil
	}

	if tx.LockTime < lockTimeThreshold {
		if nextBlockHeight > tx.LockTime {
			return nil
		}
		return &NotFinalError{LockTime: tx.LockTime, HeightBased: true, NextBlockHeight: nextBlockHeight, MedianTimePast: medianTimePast}
	}

	if medianTimePast > tx.LockTime {
		return nil
	}
	return &NotFinalError{LockTime: tx.LockTime, NextBlockHeight: nextBlockHeight, MedianTimePast: medianTimePast}
}

func rfc3339(unix uint32) string {
	return time.Unix(int64(unix), 0).UTC().Format(time.RFC3339)
}

// humanizeSeconds renders a coarse duration ("11m", "3h50m", "2d4h"),
// rounding minutes up so an ETA never reads as already elapsed.
func humanizeSeconds(seconds uint64) string {
	minutes := (seconds + 59) / 60
	switch {
	case minutes < 60:
		return fmt.Sprintf("%dm", minutes)
	case minutes < 24*60:
		if minutes%60 == 0 {
			return fmt.Sprintf("%dh", minutes/60)
		}
		return fmt.Sprintf("%dh%dm", minutes/60, minutes%60)
	default:
		days := minutes / (24 * 60)
		hours := (minutes % (24 * 60)) / 60
		if hours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd%dh", days, hours)
	}
}
