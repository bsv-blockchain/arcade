package models

import (
	"encoding/hex"
	"time"
)

// HexBytes is a byte slice that marshals to/from hex strings in JSON
type HexBytes []byte

func (h HexBytes) MarshalJSON() ([]byte, error) {
	if h == nil {
		return []byte("null"), nil
	}
	return []byte(`"` + hex.EncodeToString(h) + `"`), nil
}

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
	StatusUnknown              Status = "UNKNOWN"
	StatusReceived             Status = "RECEIVED"
	StatusSentToNetwork        Status = "SENT_TO_NETWORK"
	StatusAcceptedByNetwork    Status = "ACCEPTED_BY_NETWORK"
	StatusSeenOnNetwork        Status = "SEEN_ON_NETWORK"
	StatusDoubleSpendAttempted Status = "DOUBLE_SPEND_ATTEMPTED"
	StatusRejected             Status = "REJECTED"
	StatusMined                Status = "MINED"
	StatusImmutable            Status = "IMMUTABLE"
)

// DisallowedPreviousStatuses returns statuses that CANNOT transition to this status
// Used in UPDATE queries to prevent invalid status transitions
func (s Status) DisallowedPreviousStatuses() []Status {
	switch s {
	case StatusSentToNetwork:
		return []Status{StatusSentToNetwork, StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusRejected, StatusDoubleSpendAttempted, StatusMined}
	case StatusAcceptedByNetwork:
		return []Status{StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusRejected, StatusDoubleSpendAttempted, StatusMined}
	case StatusSeenOnNetwork, StatusRejected, StatusDoubleSpendAttempted, StatusMined:
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

// NetworkState tracks the current network block height
type NetworkState struct {
	CurrentHeight uint64
	LastBlockHash string
	LastBlockTime time.Time
}
