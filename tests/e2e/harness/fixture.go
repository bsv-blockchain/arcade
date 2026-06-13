//go:build e2e

package harness

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/util"
)

// RealBlockFixture is the on-disk artifact set the e2e tests consume
// to drive a real-mainnet-block scenario without depending on the
// public teranode datahubs (whose subtree endpoints are pruned for
// most blocks). The fixture is generated once via
// tools/fetch-block-fixture and committed under tests/e2e/fixtures/.
type RealBlockFixture struct {
	// BlockHash is the block's hash in internal byte order. matches
	// the bytes embedded in the block header.
	BlockHash chainhash.Hash
	// BlockBin is the binary served at /block/<hash>. Identical to
	// teranode's served bytes — header + subtree-hash list +
	// coinbase tx + height + coinbase BUMP, the format both arcade
	// and merkle-service expect.
	BlockBin []byte
	// Subtrees lists every subtree in the block in declaration order.
	Subtrees []SubtreeFixture
	// PickedTxIDs is the ordered list of 10 non-coinbase txids the
	// test will register with merkle-service. Display-order hex.
	PickedTxIDs []string
	// PickedRawTxs maps each picked txid to its raw bytes (suitable
	// for POST /tx). Same set of keys as PickedTxIDs.
	PickedRawTxs map[string][]byte
	// Height + MerkleRoot + CoinbaseTxID + TxCount echoed from
	// meta.json so tests can write descriptive assertions without
	// re-parsing the block binary.
	Height       uint32
	MerkleRoot   string
	CoinbaseTxID string
	TxCount      uint64

	// HeaderHex (80-byte block header) and CoinbaseHex (coinbase tx) are
	// parsed out of BlockBin. teranode's p2p BlockMessage carries both, and
	// merkle-service builds the BLOCK_PROCESSED enrichment (merkleRoot +
	// coinbaseBump) from them — NOT from a datahub fetch. Tests must feed
	// these into LibP2P.PublishBlock so merkle-service can emit the
	// enrichment; otherwise arcade falls back to a datahub fetch (issue #195).
	HeaderHex   string
	CoinbaseHex string
}

// SubtreeFixture is one entry in RealBlockFixture.Subtrees: the
// subtree's hash plus the bytes the harness datahub serves at
// /subtree/<hash>.
type SubtreeFixture struct {
	Hash chainhash.Hash
	Bin  []byte
}

// LoadBlockFixture reads tests/e2e/fixtures/blocks/<blockHash>/* into
// a RealBlockFixture. Falls back to scanning relative paths from the
// caller file's directory upward, so the loader works whether `go
// test` runs from the repo root or from inside tests/e2e/.
//
// On any missing file the test fails fatally with a clear pointer to
// the regenerator command.
func LoadBlockFixture(t *testing.T, blockHash string) *RealBlockFixture {
	t.Helper()
	root, err := findFixtureRoot(blockHash)
	if err != nil {
		t.Fatalf("fixture not found for block %s: %v\n  regenerate via:\n    go run ./tools/fetch-block-fixture --block %s --out tests/e2e/fixtures/blocks/%s",
			blockHash, err, blockHash, blockHash)
	}

	metaBytes, err := os.ReadFile(filepath.Join(root, "meta.json"))
	if err != nil {
		t.Fatalf("read meta.json: %v", err)
	}
	var meta struct {
		BlockHash    string   `json:"blockHash"`
		Height       uint32   `json:"height"`
		MerkleRoot   string   `json:"merkleRoot"`
		TxCount      uint64   `json:"txCount"`
		Subtrees     []string `json:"subtrees"`
		CoinbaseTxID string   `json:"coinbaseTxID"`
		PickedTxIDs  []string `json:"pickedTxIDs"`
	}
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatalf("decode meta.json: %v", err)
	}
	if meta.BlockHash != blockHash {
		t.Fatalf("meta.json blockHash %s != requested %s", meta.BlockHash, blockHash)
	}

	blockBin, err := os.ReadFile(filepath.Join(root, "block.bin"))
	if err != nil {
		t.Fatalf("read block.bin: %v", err)
	}

	subtrees := make([]SubtreeFixture, 0, len(meta.Subtrees))
	for _, hashHex := range meta.Subtrees {
		hash, err := decodeInternalHash(hashHex)
		if err != nil {
			t.Fatalf("decode subtree hash %s: %v", hashHex, err)
		}
		bin, err := os.ReadFile(filepath.Join(root, "subtrees", hashHex+".bin"))
		if err != nil {
			t.Fatalf("read subtree %s: %v", hashHex, err)
		}
		subtrees = append(subtrees, SubtreeFixture{Hash: *hash, Bin: bin})
	}

	rawTxs := make(map[string][]byte, len(meta.PickedTxIDs))
	for _, id := range meta.PickedTxIDs {
		bin, err := os.ReadFile(filepath.Join(root, "txs", id+".bin"))
		if err != nil {
			t.Fatalf("read tx %s: %v", id, err)
		}
		rawTxs[id] = bin
	}

	blockHashH, err := decodeBlockHashFromHeader(blockBin, blockHash)
	if err != nil {
		t.Fatalf("decode block hash from header: %v", err)
	}

	headerHex, coinbaseHex, coinbaseTxID, err := extractHeaderAndCoinbase(blockBin)
	if err != nil {
		t.Fatalf("extract header/coinbase from block.bin: %v", err)
	}
	if coinbaseTxID != meta.CoinbaseTxID {
		t.Fatalf("parsed coinbase txid %s != meta.json coinbaseTxID %s", coinbaseTxID, meta.CoinbaseTxID)
	}

	picked := append([]string(nil), meta.PickedTxIDs...)
	sort.Strings(picked)

	return &RealBlockFixture{
		BlockHash:    *blockHashH,
		BlockBin:     blockBin,
		Subtrees:     subtrees,
		PickedTxIDs:  picked,
		PickedRawTxs: rawTxs,
		Height:       meta.Height,
		MerkleRoot:   meta.MerkleRoot,
		CoinbaseTxID: meta.CoinbaseTxID,
		TxCount:      meta.TxCount,
		HeaderHex:    headerHex,
		CoinbaseHex:  coinbaseHex,
	}
}

// extractHeaderAndCoinbase pulls the 80-byte block header and the coinbase
// transaction out of the teranode datahub block binary (the bytes served at
// /block/<hash>). Layout:
//
//	header(80) | txCount(varint) | sizeBytes(varint) | subtreeCount(varint) |
//	subtreeHashes(N*32) | coinbaseTx(variable) | ...
//
// Both are returned as hex, matching teranode's p2p BlockMessage.Header /
// .Coinbase fields that merkle-service consumes to build the BLOCK_PROCESSED
// enrichment. The coinbase txid is returned too so the caller can verify the
// parse against meta.json.
func extractHeaderAndCoinbase(blockBin []byte) (headerHex, coinbaseHex, coinbaseTxID string, err error) {
	if len(blockBin) < 80 {
		return "", "", "", fmt.Errorf("block.bin too short for header: %d bytes", len(blockBin))
	}
	headerHex = hex.EncodeToString(blockBin[:80])

	r := bytes.NewReader(blockBin[80:])
	var txCount, sizeBytes, subtreeCount util.VarInt
	if _, e := txCount.ReadFrom(r); e != nil {
		return "", "", "", fmt.Errorf("read tx count: %w", e)
	}
	if _, e := sizeBytes.ReadFrom(r); e != nil {
		return "", "", "", fmt.Errorf("read size bytes: %w", e)
	}
	if _, e := subtreeCount.ReadFrom(r); e != nil {
		return "", "", "", fmt.Errorf("read subtree count: %w", e)
	}
	if _, e := io.CopyN(io.Discard, r, int64(subtreeCount)*32); e != nil {
		return "", "", "", fmt.Errorf("skip %d subtree hashes: %w", uint64(subtreeCount), e)
	}

	remaining := blockBin[len(blockBin)-r.Len():]
	tx, used, e := transaction.NewTransactionFromStream(remaining)
	if e != nil {
		return "", "", "", fmt.Errorf("parse coinbase tx: %w", e)
	}
	coinbaseHex = hex.EncodeToString(remaining[:used])
	coinbaseTxID = tx.TxID().String()
	return headerHex, coinbaseHex, coinbaseTxID, nil
}

// findFixtureRoot walks up from the test file's directory until it
// finds tests/e2e/fixtures/blocks/<blockHash>/. Lets `go test` run
// from anywhere in the module without callers having to thread paths
// in by hand.
func findFixtureRoot(blockHash string) (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller: not available")
	}
	dir := filepath.Dir(file)
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, "fixtures", "blocks", blockHash)
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return candidate, nil
		}
		// Walk up.
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("no fixtures/blocks/%s/ found relative to harness package", blockHash)
}

// decodeInternalHash parses a hex hash that was written in INTERNAL
// byte order (i.e., direct hex encode of the bytes — what
// tools/fetch-block-fixture writes for subtree hashes since they live
// that way in the block binary).
func decodeInternalHash(hashHex string) (*chainhash.Hash, error) {
	if len(hashHex) != 64 {
		return nil, fmt.Errorf("expected 64 hex chars, got %d", len(hashHex))
	}
	b, err := hex.DecodeString(hashHex)
	if err != nil {
		return nil, err
	}
	return chainhash.NewHash(b)
}

// decodeBlockHashFromHeader recomputes the block's hash by SHA256d-ing
// the 80-byte header, instead of trusting the meta.json's display-order
// string. Lets the fixture self-verify: a corrupted block.bin produces
// a hash that doesn't match the directory name.
func decodeBlockHashFromHeader(blockBin []byte, expectedDisplayHex string) (*chainhash.Hash, error) {
	if len(blockBin) < 80 {
		return nil, fmt.Errorf("block.bin too short: %d bytes", len(blockBin))
	}
	h := chainhash.DoubleHashH(blockBin[:80])
	if h.String() != expectedDisplayHex {
		return nil, fmt.Errorf("block hash mismatch: header hashes to %s but fixture is named %s",
			h.String(), expectedDisplayHex)
	}
	return &h, nil
}
