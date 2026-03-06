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

// TransactionStatus represents the current status of a transaction
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
	// StatusDoubleSpendAttempted indicates a double spend was attempted.
	StatusDoubleSpendAttempted = Status("DOUBLE_SPEND_ATTEMPTED")
	// StatusRejected indicates the transaction was rejected.
	StatusRejected = Status("REJECTED")
	// StatusMined indicates the transaction was mined.
	StatusMined = Status("MINED")
	// StatusImmutable indicates the transaction is immutable.
	StatusImmutable = Status("IMMUTABLE")
)

// DisallowedPreviousStatuses returns statuses that CANNOT transition to this status
// Used in UPDATE queries to prevent invalid status transitions
func (s Status) DisallowedPreviousStatuses() []Status {
	switch s {
	case StatusUnknown, StatusReceived:
		return []Status{}
	case StatusSentToNetwork:
		return []Status{StatusSentToNetwork, StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusRejected, StatusDoubleSpendAttempted, StatusMined}
	case StatusAcceptedByNetwork:
		return []Status{StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusRejected, StatusDoubleSpendAttempted, StatusMined}
	case StatusSeenOnNetwork, StatusRejected, StatusDoubleSpendAttempted, StatusMined, StatusImmutable:
		return []Status{}
	default:
		return []Status{}
	}
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
