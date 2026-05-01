// Package models contains data structures for transaction and status management.
package models

import (
	"encoding/hex"
	"time"
)

// HexBytes is a byte slice that marshals to/from hex strings in JSON
type HexBytes []byte

// MarshalJSON converts HexBytes to a JSON-encoded hex string.
func (h HexBytes) MarshalJSON() ([]byte, error) {
	if h == nil {
		return []byte("null"), nil
	}
	return []byte(`"` + hex.EncodeToString(h) + `"`), nil
}

// UnmarshalJSON decodes a JSON-encoded hex string into HexBytes.
func (h *HexBytes) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*h = nil
		return nil
	}
	// Remove quotes
	if len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"' {
		data = data[1 : len(data)-1]
	}
	decoded, err := hex.DecodeString(string(data))
	if err != nil {
		return err
	}
	*h = decoded
	return nil
}

// TransactionStatus represents the current status of a transaction.
// RawTx and NextRetryAt are set only while the tx is in PENDING_RETRY and
// cleared on any terminal transition; they drive the durable retry reaper.
type TransactionStatus struct {
	TxID         string    `json:"txid"`
	Status       Status    `json:"txStatus"`
	StatusCode   int       `json:"status,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
	BlockHash    string    `json:"blockHash,omitempty"`
	BlockHeight  uint64    `json:"blockHeight,omitempty"`
	MerklePath   HexBytes  `json:"merklePath,omitempty"`
	ExtraInfo    string    `json:"extraInfo,omitempty"`
	CompetingTxs []string  `json:"competingTxs,omitempty"`
	RawTx        HexBytes  `json:"rawTx,omitempty"`
	RetryCount   int       `json:"retryCount,omitempty"`
	NextRetryAt  time.Time `json:"nextRetryAt,omitempty"`
	CreatedAt    time.Time `json:"-"`
}

// Status represents the various states a transaction can be in
type Status string

const (
	// StatusUnknown indicates the transaction status is unknown.
	StatusUnknown = Status("UNKNOWN")
	// StatusReceived indicates the transaction was received.
	StatusReceived = Status("RECEIVED")
	// StatusSentToNetwork indicates the transaction was sent to the network.
	StatusSentToNetwork = Status("SENT_TO_NETWORK")
	// StatusAcceptedByNetwork indicates the transaction was accepted by the network.
	StatusAcceptedByNetwork = Status("ACCEPTED_BY_NETWORK")
	// StatusSeenOnNetwork indicates the transaction was seen on the network.
	StatusSeenOnNetwork = Status("SEEN_ON_NETWORK")
	// StatusSeenMultipleNodes indicates the transaction was seen by multiple independent miners.
	StatusSeenMultipleNodes = Status("SEEN_MULTIPLE_NODES")
	// StatusDoubleSpendAttempted indicates a double spend was attempted.
	StatusDoubleSpendAttempted = Status("DOUBLE_SPEND_ATTEMPTED")
	// StatusRejected indicates the transaction was rejected.
	StatusRejected = Status("REJECTED")
	// StatusPendingRetry indicates the transaction broadcast failed with a retryable error and will be retried.
	StatusPendingRetry = Status("PENDING_RETRY")
	// StatusStumpProcessing indicates the transaction has a STUMP and is building the BUMP
	StatusStumpProcessing = Status("STUMP_PROCESSING")
	// StatusMined indicates the transaction was mined.
	StatusMined = Status("MINED")
	// StatusImmutable indicates the transaction is immutable.
	StatusImmutable = Status("IMMUTABLE")
)

// terminalStatuses lists statuses that represent a final outcome for a
// transaction. A row already in one of these states must never be overwritten
// by a lower-priority update (e.g. a late SEEN_ON_NETWORK callback arriving
// after MINED). The only allowed transition out of a terminal status is the
// MINED → IMMUTABLE promotion handled explicitly by CanTransitionFrom.
var terminalStatuses = map[Status]struct{}{
	StatusRejected:             {},
	StatusDoubleSpendAttempted: {},
	StatusMined:                {},
	StatusImmutable:            {},
}

// IsTerminal reports whether this status is a final outcome that should not be
// overwritten by later, lower-priority updates.
func (s Status) IsTerminal() bool {
	_, ok := terminalStatuses[s]
	return ok
}

// DisallowedPreviousStatuses returns statuses that CANNOT transition to this status.
// Used by stores' UpdateStatus implementations (and CanTransitionFrom) to prevent
// invalid status transitions — most importantly, to prevent terminal statuses
// (MINED, IMMUTABLE, REJECTED, DOUBLE_SPEND_ATTEMPTED) from being silently
// overwritten by a later, lower-priority update such as SEEN_ON_NETWORK.
func (s Status) DisallowedPreviousStatuses() []Status {
	switch s {
	case StatusUnknown, StatusReceived:
		// Going back to UNKNOWN/RECEIVED is never valid once the tx has any
		// further status — only an initial insert should set these.
		return []Status{
			StatusSentToNetwork, StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusPendingRetry, StatusStumpProcessing,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSentToNetwork:
		// PENDING_RETRY → SENT_TO_NETWORK is the reaper republishing a retry-
		// queued tx, so it must be allowed; everything to the right of
		// SENT_TO_NETWORK in the forward order is a regression.
		return []Status{
			StatusSentToNetwork, StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusAcceptedByNetwork:
		return []Status{
			StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSeenOnNetwork:
		// SEEN_ON_NETWORK is a forward step from earlier states only — once a
		// tx has progressed to SEEN_MULTIPLE_NODES, a terminal state, or is in
		// the retry side-branch it must not regress to SEEN_ON_NETWORK.
		return []Status{
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusPendingRetry,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSeenMultipleNodes:
		return []Status{
			StatusSeenMultipleNodes,
			StatusPendingRetry,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusPendingRetry:
		// PENDING_RETRY is only valid for txs that are still in flight — never
		// from a terminal state, and never from an already-confirmed state.
		return []Status{
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusStumpProcessing:
		// STUMP_PROCESSING relates to mined-block bump building. It is only
		// meaningful for in-flight txs; once a tx is terminal it must not be
		// pushed back into STUMP_PROCESSING.
		return []Status{
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusRejected, StatusDoubleSpendAttempted:
		// Rejection paths can override any non-terminal in-flight state, but
		// must not be able to clobber an already-confirmed (MINED/IMMUTABLE)
		// transaction.
		return []Status{StatusMined, StatusImmutable}
	case StatusMined:
		// MINED can be set from any in-flight state, but a transient miner-
		// reorg-style regression must not pull a tx out of IMMUTABLE.
		return []Status{StatusImmutable}
	case StatusImmutable:
		// IMMUTABLE is the highest-priority sink — reachable from anything.
		return []Status{}
	default:
		return []Status{}
	}
}

// CanTransitionFrom reports whether moving from prev → s is allowed by the
// status lattice. An empty prev (i.e. no existing row) is always allowed —
// the very first write for a txid bypasses the lattice. Re-asserting the same
// status (prev == s) is also allowed: it is an idempotent no-op for callers
// that may receive duplicate callbacks.
func (s Status) CanTransitionFrom(prev Status) bool {
	if prev == "" || prev == s {
		return true
	}
	for _, banned := range s.DisallowedPreviousStatuses() {
		if banned == prev {
			return false
		}
	}
	return true
}

// Submission represents a client's submission and subscription preferences
type Submission struct {
	SubmissionID        string
	TxID                string
	CallbackURL         string
	CallbackToken       string
	FullStatusUpdates   bool
	LastDeliveredStatus Status
	RetryCount          int
	NextRetryAt         *time.Time
	CreatedAt           time.Time
}

// BlockReorg represents a notification that a block was orphaned due to a chain reorganization
type BlockReorg struct {
	BlockHash   string
	BlockHeight uint64
	Timestamp   time.Time
}

// SubmitOptions contains options for transaction submission
type SubmitOptions struct {
	CallbackURL          string // Webhook URL for status callbacks
	CallbackToken        string // Token for SSE event filtering
	FullStatusUpdates    bool   // Send all status updates (not just final)
	SkipFeeValidation    bool   // Skip fee validation
	SkipScriptValidation bool   // Skip script validation
}

// Policy represents the transaction policy configuration
type Policy struct {
	MaxScriptSizePolicy     uint64 `json:"maxscriptsizepolicy"`
	MaxTxSigOpsCountsPolicy uint64 `json:"maxtxsigopscountspolicy"`
	MaxTxSizePolicy         uint64 `json:"maxtxsizepolicy"`
	MiningFeeBytes          uint64 `json:"miningFeeBytes"`
	MiningFeeSatoshis       uint64 `json:"miningFeeSatoshis"`
}
