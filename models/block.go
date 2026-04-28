package models

import "time"

type BlockHeader struct {
	Version       uint32 `json:"version"`
	PrevHash      string `json:"prev_hash"`
	MerkleRoot    string `json:"merkle_root"`
	Timestamp     uint32 `json:"timestamp"`
	Bits          uint32 `json:"bits"`
	Nonce         uint32 `json:"nonce"`
}

type Block struct {
	Hash        string      `json:"hash"`
	Header      BlockHeader `json:"header"`
	Height      uint64      `json:"height"`
	CoinbaseTx  []byte      `json:"coinbase_tx"`
	TxCount     uint64      `json:"tx_count"`
	ReceivedAt  time.Time   `json:"received_at"`
}
