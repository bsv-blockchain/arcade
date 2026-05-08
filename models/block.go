package models

import "time"

type BlockHeader struct {
	Version    uint32 `json:"version"`
	PrevHash   string `json:"prev_hash"`
	MerkleRoot string `json:"merkle_root"`
	Timestamp  uint32 `json:"timestamp"`
	Bits       uint32 `json:"bits"`
	Nonce      uint32 `json:"nonce"`
}

type Block struct {
	Hash       string      `json:"hash"`
	Header     BlockHeader `json:"header"`
	Height     uint64      `json:"height"`
	CoinbaseTx []byte      `json:"coinbase_tx"`
	TxCount    uint64      `json:"tx_count"`
	ReceivedAt time.Time   `json:"received_at"`
}

// BlockProcessingStatusValue tracks whether a block tracked by chaintracks
// is still on the active chain or has been orphaned by a reorg. Stored as a
// short string for backend portability (Postgres TEXT, Aerospike string bin,
// Pebble byte slice).
type BlockProcessingStatusValue string

const (
	BlockStatusActive   BlockProcessingStatusValue = "active"
	BlockStatusOrphaned BlockProcessingStatusValue = "orphaned"
)

// BlockProcessingStatus records the milestones we've reached for one block:
// when chaintracks first observed the header, when the merkle service
// delivered BLOCK_PROCESSED, and when the bump-builder finished assembling
// the compound BUMP. Pointer time fields stay nil until the corresponding
// milestone fires so JSON renders `null` and backends can distinguish
// "not yet" from "at the zero time".
type BlockProcessingStatus struct {
	BlockHash    string                     `json:"blockHash"`
	BlockHeight  uint64                     `json:"blockHeight"`
	HeaderSeenAt time.Time                  `json:"headerSeenAt"`
	ProcessedAt  *time.Time                 `json:"processedAt,omitempty"`
	BUMPBuiltAt  *time.Time                 `json:"bumpBuiltAt,omitempty"`
	Status       BlockProcessingStatusValue `json:"status"`
	OrphanedAt   *time.Time                 `json:"orphanedAt,omitempty"`
}
