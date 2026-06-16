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

	// The following fields enrich BLOCK_PROCESSED so a consumer can build and
	// validate a compound BUMP without fetching the block from a datahub. They
	// are populated by merkle-service >= v0.2.4 and are all additive +
	// omitempty: an older producer (or one that couldn't build them) omits
	// them, and bump-builder falls back to the datahub fetch. See issue #195.

	// MerkleRoot is the canonical block-header merkle root in DISPLAY-order hex
	// (same convention as BlockHash). Decode with chainhash.NewHashFromHex,
	// which reverses display->internal order — NOT HexBytes, which does not
	// reverse.
	MerkleRoot string `json:"merkleRoot,omitempty"`
	// SubtreeCount is the canonical number of subtrees in the block.
	SubtreeCount int `json:"subtreeCount,omitempty"`
	// SubtreeHashes are the canonical, coinbase-placeholder-based subtree roots
	// in subtree-index order, DISPLAY-order hex (same decoding as MerkleRoot).
	// subtreeHashes[0] is still corrected from the coinbase BUMP inside
	// BuildCompoundBUMP.
	SubtreeHashes []string `json:"subtreeHashes,omitempty"`
	// CoinbaseBUMP is a BRC-74 merkle path of the coinbase transaction up to the
	// block merkle root — a drop-in replacement for the coinbase BUMP arcade
	// otherwise parses out of the datahub binary response. It is the hex of the
	// raw BRC-74 bytes, so HexBytes (a plain, non-reversing hex decode) is the
	// correct type here.
	CoinbaseBUMP HexBytes `json:"coinbaseBump,omitempty"`
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
	BlockHash    string `json:"block_hash"`
	SubtreeIndex int    `json:"subtree_index"`
	StumpData    []byte `json:"stump_data"`
}
