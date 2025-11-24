package models

import "time"

// TransactionStatus represents the current status of a transaction
type TransactionStatus struct {
	TxID         string
	Status       Status
	Timestamp    time.Time
	BlockHash    string
	BlockHeight  uint64
	MerklePath   string
	ExtraInfo    string
	CompetingTxs []string
	CreatedAt    time.Time
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

// StatusUpdate represents a notification that a transaction status has changed
type StatusUpdate struct {
	TxID      string
	Status    Status
	Timestamp time.Time
}

// NetworkState tracks the current network block height
type NetworkState struct {
	CurrentHeight uint64
	LastBlockHash string
	LastBlockTime time.Time
}
