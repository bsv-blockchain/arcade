package models

// CallbackType represents the type of callback message from Merkle Service
type CallbackType string

const (
	// CallbackSeenOnNetwork indicates the transaction was seen in a subtree
	CallbackSeenOnNetwork = CallbackType("SEEN_ON_NETWORK")
	// CallbackSeenMultipleNodes indicates the transaction was seen by multiple miners
	CallbackSeenMultipleNodes = CallbackType("SEEN_MULTIPLE_NODES")
	// CallbackStump indicates a STUMP (subtree merkle path) for a mined transaction
	CallbackStump = CallbackType("STUMP")
	// CallbackBlockProcessed indicates all STUMPs for a block have been delivered
	CallbackBlockProcessed = CallbackType("BLOCK_PROCESSED")
)

// CallbackMessage is the payload received from Merkle Service on the callback endpoint
type CallbackMessage struct {
	Type         CallbackType `json:"type"`
	TxID         string       `json:"txid,omitempty"`
	TxIDs        []string     `json:"txids,omitempty"`
	BlockHash    string       `json:"blockHash,omitempty"`
	SubtreeIndex int          `json:"subtreeIndex,omitempty"`
	Stump        HexBytes     `json:"stump,omitempty"`
}

// ResolveSeenTxIDs returns the list of txids from either the batched TxIDs
// field or the scalar TxID field, for backward compatibility.
func (msg *CallbackMessage) ResolveSeenTxIDs() []string {
	if len(msg.TxIDs) > 0 {
		return msg.TxIDs
	}
	if msg.TxID != "" {
		return []string{msg.TxID}
	}
	return nil
}

// Stump represents a stored STUMP (Subtree Unified Merkle Path), keyed by subtree.
type Stump struct {
	BlockHash    string
	SubtreeIndex int
	StumpData    []byte
}
