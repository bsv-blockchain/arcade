package chaintracks

import (
	"math/big"

	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// BlockHeader extends the base block.Header with additional chain-specific metadata
type BlockHeader struct {
	*block.Header

	Height    uint32         `json:"height"` // Block height in the chain
	Hash      chainhash.Hash `json:"hash"`
	ChainWork *big.Int       `json:"-"` // Cumulative chain work up to and including this block
}

// CDNMetadata represents the JSON metadata file structure
type CDNMetadata struct {
	RootFolder     string         `json:"rootFolder"`
	JSONFilename   string         `json:"jsonFilename"`
	HeadersPerFile int            `json:"headersPerFile"`
	Files          []CDNFileEntry `json:"files"`
}

// CDNFileEntry represents a single file entry in the metadata
type CDNFileEntry struct {
	Chain         string         `json:"chain"`
	Count         int            `json:"count"`
	FileHash      string         `json:"fileHash"`
	FileName      string         `json:"fileName"`
	FirstHeight   uint32         `json:"firstHeight"`
	LastChainWork string         `json:"lastChainWork"`
	LastHash      chainhash.Hash `json:"lastHash"`
	PrevChainWork string         `json:"prevChainWork"`
	PrevHash      chainhash.Hash `json:"prevHash"`
	SourceURL     string         `json:"sourceUrl"`
}

// PeerInfo contains information about a connected peer
type PeerInfo struct {
	ID    string
	Name  string
	Addrs []string
}
