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
// is still on the active chain, has been orphaned by a reorg, or has been
// parked by the watchdog (reprocess caps exhausted — needs explicit triage,
// no longer re-driven). Stored as a short string for backend portability
// (Postgres TEXT, Aerospike string bin, Pebble byte slice).
type BlockProcessingStatusValue string

const (
	BlockStatusActive   BlockProcessingStatusValue = "active"
	BlockStatusOrphaned BlockProcessingStatusValue = "orphaned"
	// BlockStatusParked marks an on-chain block the watchdog gave up
	// re-driving (MaxReprocessAttempts / MaxStaleAge crossed). Unlike
	// orphaned it IS on the active chain — its compound BUMP is genuinely
	// missing. Parked rows leave the watchdog's stale scan (status='active'
	// predicate) so they surface as an explicit triage backlog instead of
	// silent processed_at=NULL churn; a fresh header arrival for the same
	// hash resets status='active' (UpsertBlockHeaderSeen conflict path) and
	// revives recovery.
	BlockStatusParked BlockProcessingStatusValue = "parked"
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
