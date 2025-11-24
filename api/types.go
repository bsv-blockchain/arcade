package api

import "time"

// TransactionResponse is the Arc-compatible transaction response
type TransactionResponse struct {
	Timestamp    time.Time `json:"timestamp"`
	TxID         string    `json:"txid"`
	TxStatus     string    `json:"txStatus"`
	BlockHash    string    `json:"blockHash,omitempty"`
	BlockHeight  uint64    `json:"blockHeight,omitempty"`
	MerklePath   string    `json:"merklePath,omitempty"`
	ExtraInfo    string    `json:"extraInfo,omitempty"`
	CompetingTxs []string  `json:"competingTxs,omitempty"`
}

// TransactionRequest represents a transaction submission request
type TransactionRequest struct {
	RawTx string `json:"rawTx"`
}

// PolicyResponse represents the policy configuration
type PolicyResponse struct {
	MaxScriptSizePolicy     uint64    `json:"maxscriptsizepolicy"`
	MaxTxSigOpsCountsPolicy uint64    `json:"maxtxsigopscountspolicy"`
	MaxTxSizePolicy         uint64    `json:"maxtxsizepolicy"`
	MiningFee               FeeAmount `json:"miningFee"`
}

// FeeAmount represents fee amount in bytes and satoshis
type FeeAmount struct {
	Bytes    uint64 `json:"bytes"`
	Satoshis uint64 `json:"satoshis"`
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Healthy bool   `json:"healthy"`
	Reason  string `json:"reason,omitempty"`
	Version string `json:"version,omitempty"`
}

// RequestHeaders contains Arc-compatible request headers
type RequestHeaders struct {
	CallbackURL        string
	CallbackToken      string
	CallbackBatch      bool
	FullStatusUpdates  bool
	WaitFor            string
	MaxTimeout         int
	SkipFeeValidation  bool
	SkipScriptValidation bool
}
