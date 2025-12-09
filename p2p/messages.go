package p2p

// NodeStatusMessage represents a node status update message.
// These are broadcast by Teranode nodes to announce their current state.
type NodeStatusMessage struct {
	PeerID              string   `json:"peer_id"`
	ClientName          string   `json:"client_name"`
	Type                string   `json:"type"`
	BaseURL             string   `json:"base_url"`
	Version             string   `json:"version"`
	CommitHash          string   `json:"commit_hash"`
	BestBlockHash       string   `json:"best_block_hash"`
	BestHeight          uint32   `json:"best_height"`
	TxCount             uint64   `json:"tx_count,omitempty"`
	SubtreeCount        uint32   `json:"subtree_count,omitempty"`
	FSMState            string   `json:"fsm_state"`
	StartTime           int64    `json:"start_time"`
	Uptime              float64  `json:"uptime"`
	MinerName           string   `json:"miner_name"`
	ListenMode          string   `json:"listen_mode"`
	ChainWork           string   `json:"chain_work"`
	SyncPeerID          string   `json:"sync_peer_id,omitempty"`
	SyncPeerHeight      uint32   `json:"sync_peer_height,omitempty"`
	SyncPeerBlockHash   string   `json:"sync_peer_block_hash,omitempty"`
	SyncConnectedAt     int64    `json:"sync_connected_at,omitempty"`
	MinMiningTxFee      *float64 `json:"min_mining_tx_fee,omitempty"`
	ConnectedPeersCount int      `json:"connected_peers_count,omitempty"`
	Storage             string   `json:"storage,omitempty"`
}

// BlockMessage announces the availability of a new block to the P2P network.
// It contains block metadata and a reference to where full block data can be retrieved.
type BlockMessage struct {
	PeerID     string `json:"PeerID"`
	ClientName string `json:"ClientName"`
	DataHubURL string `json:"DataHubURL"`
	Hash       string `json:"Hash"`
	Height     uint32 `json:"Height"`
	Header     string `json:"Header"`
	Coinbase   string `json:"Coinbase"`
}

// SubtreeMessage announces the availability of a subtree (transaction batch) to the network.
// Subtrees represent collections of validated transactions ready for inclusion in a block.
type SubtreeMessage struct {
	PeerID     string `json:"PeerID"`
	ClientName string `json:"ClientName"`
	DataHubURL string `json:"DataHubURL"`
	Hash       string `json:"Hash"`
}

// RejectedTxMessage notifies peers about a rejected transaction.
// This helps prevent unnecessary retransmissions of invalid transactions.
type RejectedTxMessage struct {
	PeerID     string `json:"PeerID"`
	ClientName string `json:"ClientName"`
	TxID       string `json:"TxID"`
	Reason     string `json:"Reason"`
}
