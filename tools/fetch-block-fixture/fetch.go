package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// run is the top-level pipeline. Pulled out of main() so a test can
// drive it with synthetic config.
func run(cfg config) error {
	if err := os.MkdirAll(filepath.Join(cfg.outDir, "subtrees"), 0o750); err != nil {
		return fmt.Errorf("mkdir subtrees: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.outDir, "txs"), 0o750); err != nil {
		return fmt.Errorf("mkdir txs: %w", err)
	}

	// 1. Fetch block bytes from teranode datahub.
	teranodeBlock, err := httpGetBytes(cfg.datahubURL+"/block/"+cfg.blockHash, 10*1024*1024)
	if err != nil {
		return fmt.Errorf("fetch block: %w", err)
	}
	parsed, err := parseTeranodeBlock(teranodeBlock)
	if err != nil {
		return fmt.Errorf("parse block: %w", err)
	}
	log.Printf("block %s: height=%d txCount=%d subtreeCount=%d coinbaseTxID=%s",
		cfg.blockHash, parsed.height, parsed.txCount, len(parsed.subtreeHashes), parsed.coinbaseTxID)

	// 2. Fetch txid list from WhatsOnChain. The first txid must equal
	//    the coinbase txid we extracted from the block binary —
	//    otherwise the WoC and teranode views of the block disagree
	//    and our reconstruction won't match the header root.
	txids, err := fetchBlockTxIDs(cfg.wocURL, cfg.blockHash)
	if err != nil {
		return fmt.Errorf("fetch txids: %w", err)
	}
	if uint64(len(txids)) != parsed.txCount {
		return fmt.Errorf("txid count mismatch: woc=%d teranode=%d", len(txids), parsed.txCount)
	}
	if txids[0] != parsed.coinbaseTxID {
		return fmt.Errorf("coinbase txid mismatch: woc=%s teranode=%s", txids[0], parsed.coinbaseTxID)
	}

	// 3. Reconstruct subtrees. For subtreeCount==1 the answer is the
	//    whole txid list (with coinbase replaced by the placeholder
	//    at index 0). For subtreeCount > 1 we recover boundaries by
	//    matching subtree merkle roots.
	subtreeBins, err := buildSubtreeBinaries(txids, parsed.subtreeHashes)
	if err != nil {
		return fmt.Errorf("build subtrees: %w", err)
	}

	// 4. Self-check: rebuilding subtree[0] with the real coinbase
	//    must produce the block header's merkle root. If this fails
	//    the txid list and the header disagree and the fixture would
	//    be useless — better to fail loudly here than later in the
	//    e2e test.
	if vErr := verifyHeaderMatchesReconstruction(parsed, txids); vErr != nil {
		return fmt.Errorf("header self-check: %w", vErr)
	}

	// 5. Use teranode's served bytes for block.bin verbatim. The
	//    block already includes a real coinbase BUMP (~hundreds of
	//    bytes for typical mainnet blocks); recomputing here would
	//    risk byte-level drift from what arcade's bump-builder
	//    expects when it parses /block/<hash>.
	blockBin := teranodeBlock

	// 7. Pick N random non-coinbase txids and fetch their raw bytes.
	//    Pace fetches at one every ~300ms so we stay below WoC's free-
	//    tier rate limit for sequential requests (the retry loop handles
	//    sporadic 429s on top of this).
	picked := pickRandomTxIDs(txids[1:], cfg.pickN, cfg.rng)
	rawTxs := make(map[string][]byte, len(picked))
	for i, id := range picked {
		if i > 0 {
			time.Sleep(300 * time.Millisecond)
		}
		raw, fErr := fetchRawTx(cfg.wocURL, id)
		if fErr != nil {
			return fmt.Errorf("fetch raw tx %s: %w", id, fErr)
		}
		rawTxs[id] = raw
	}

	// 8. Write everything to disk.
	if wErr := os.WriteFile(filepath.Join(cfg.outDir, "block.bin"), blockBin, 0o644); wErr != nil { //nolint:gosec // test fixtures committed alongside code; readable like any tracked file
		return fmt.Errorf("write block.bin: %w", wErr)
	}
	for hash, bin := range subtreeBins {
		path := filepath.Join(cfg.outDir, "subtrees", hash+".bin")
		if wErr := os.WriteFile(path, bin, 0o644); wErr != nil { //nolint:gosec // test fixtures committed alongside code; readable like any tracked file
			return fmt.Errorf("write %s: %w", path, wErr)
		}
	}
	for id, raw := range rawTxs {
		path := filepath.Join(cfg.outDir, "txs", id+".bin")
		if wErr := os.WriteFile(path, raw, 0o644); wErr != nil { //nolint:gosec // test fixtures committed alongside code; readable like any tracked file
			return fmt.Errorf("write %s: %w", path, wErr)
		}
	}
	meta := metaFile{
		BlockHash:    cfg.blockHash,
		Height:       parsed.height,
		MerkleRoot:   parsed.merkleRootHex,
		TxCount:      parsed.txCount,
		SubtreeCount: len(parsed.subtreeHashes),
		Subtrees:     parsed.subtreeHashes,
		CoinbaseTxID: parsed.coinbaseTxID,
		PickedTxIDs:  picked,
		Provenance: provenance{
			DatahubURL:  cfg.datahubURL,
			WOCURL:      cfg.wocURL,
			Seed:        0, // tool config-driven; intentionally not encoded since tool consumers re-run with their own seed
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("encode meta: %w", err)
	}
	if wErr := os.WriteFile(filepath.Join(cfg.outDir, "meta.json"), append(metaBytes, '\n'), 0o644); wErr != nil { //nolint:gosec // test fixtures committed alongside code; readable like any tracked file
		return fmt.Errorf("write meta.json: %w", wErr)
	}

	log.Printf("wrote fixture to %s (block.bin=%dB, %d subtree(s), %d picked txs)",
		cfg.outDir, len(blockBin), len(subtreeBins), len(rawTxs))
	return nil
}

type metaFile struct {
	BlockHash    string     `json:"blockHash"`
	Height       uint32     `json:"height"`
	MerkleRoot   string     `json:"merkleRoot"`
	TxCount      uint64     `json:"txCount"`
	SubtreeCount int        `json:"subtreeCount"`
	Subtrees     []string   `json:"subtrees"`
	CoinbaseTxID string     `json:"coinbaseTxID"`
	PickedTxIDs  []string   `json:"pickedTxIDs"`
	Provenance   provenance `json:"provenance"`
}

type provenance struct {
	DatahubURL  string `json:"datahubURL"`
	WOCURL      string `json:"wocURL"`
	Seed        int64  `json:"seed,omitempty"`
	GeneratedAt string `json:"generatedAt"`
}

// teranodeBlock holds the fields parsed out of the raw block binary.
// merkleRootHex is the display-order merkle-root string
// (chainhash.Hash.String()).
type teranodeBlock struct {
	merkleRootHex string
	height        uint32
	txCount       uint64
	subtreeHashes []string // hex strings, internal byte order matching the binary
	coinbaseTxID  string   // hex string in display (reverse) order
}

// parseTeranodeBlock decodes a raw block binary the way both arcade and
// merkle-service expect:
//
//	header(80) | txCount(varint) | sizeBytes(varint) | subtreeCount(varint) |
//	subtreeHashes(N×32) | coinbaseTx(variable) | height(varint) |
//	coinbaseBUMPLen(varint) | coinbaseBUMP
//
// Returns offsets so callers can splice in a non-empty coinbase BUMP.
func parseTeranodeBlock(data []byte) (*teranodeBlock, error) {
	if len(data) < 80 {
		return nil, fmt.Errorf("block data too short: %d bytes", len(data))
	}
	mr := make([]byte, 32)
	copy(mr, data[36:68])
	mh, err := chainhash.NewHash(mr)
	if err != nil {
		return nil, fmt.Errorf("parse merkle root: %w", err)
	}

	r := newCursor(data, 80)
	txCount, err := r.varint()
	if err != nil {
		return nil, fmt.Errorf("read txCount: %w", err)
	}
	if _, vErr := r.varint(); vErr != nil { // sizeBytes — we don't store it
		return nil, fmt.Errorf("read sizeBytes: %w", vErr)
	}
	subtreeCount, err := r.varint()
	if err != nil {
		return nil, fmt.Errorf("read subtreeCount: %w", err)
	}
	subtrees := make([]string, 0, subtreeCount)
	for i := uint64(0); i < subtreeCount; i++ {
		buf, rErr := r.read(32)
		if rErr != nil {
			return nil, fmt.Errorf("read subtree[%d]: %w", i, rErr)
		}
		subtrees = append(subtrees, hex.EncodeToString(buf))
	}

	// Coinbase tx — variable length. Use the SDK to parse + report length.
	tx, used, err := sdkTx.NewTransactionFromStream(data[r.pos:])
	if err != nil {
		return nil, fmt.Errorf("parse coinbase tx: %w", err)
	}
	r.pos += used

	height64, err := r.varint()
	if err != nil {
		return nil, fmt.Errorf("read height: %w", err)
	}

	if height64 > 1<<32-1 {
		return nil, fmt.Errorf("height %d does not fit in uint32", height64)
	}

	return &teranodeBlock{
		merkleRootHex: mh.String(),
		height:        uint32(height64),
		txCount:       txCount,
		subtreeHashes: subtrees,
		coinbaseTxID:  tx.TxID().String(),
	}, nil
}

// buildSubtreeBinaries assembles each subtree's wire-format binary:
// concatenated 32-byte tx hashes (internal byte order). subtree[0] uses
// the coinbase placeholder (zero hash) at index 0, so merkle-service's
// STUMP construction correctly anchors the coinbase position.
//
// For multi-subtree blocks WoC doesn't expose subtree boundaries — we
// recover them by computing the merkle root over progressively-larger
// candidate batches and matching against parsed.subtreeHashes.
func buildSubtreeBinaries(txids, subtreeHashesHex []string) (map[string][]byte, error) {
	if len(subtreeHashesHex) == 0 {
		return nil, fmt.Errorf("block has no subtrees")
	}
	if len(subtreeHashesHex) == 1 {
		// Whole tx list, with coinbase replaced by placeholder at 0.
		leaves := parseHashes(txids)
		leaves[0] = chainhash.Hash{}
		bin := concatHashes(leaves)
		return map[string][]byte{subtreeHashesHex[0]: bin}, nil
	}
	return buildMultiSubtreeBinaries(txids, subtreeHashesHex)
}

// intnRNG abstracts the math/rand/v2.Rand IntN method so tests can
// inject a deterministic stub without depending on the concrete type.
type intnRNG interface {
	IntN(n int) int
}

// pickRandomTxIDs returns n distinct entries from txids using rng. Sorts
// the result for stable output.
func pickRandomTxIDs(txids []string, n int, rng intnRNG) []string {
	if n > len(txids) {
		n = len(txids)
	}
	pool := make([]string, len(txids))
	copy(pool, txids)
	for i := 0; i < n; i++ {
		j := i + rng.IntN(len(pool)-i)
		pool[i], pool[j] = pool[j], pool[i]
	}
	picked := pool[:n]
	sort.Strings(picked)
	return picked
}

// httpGetBytes is a small wrapper that GETs a URL with a generous
// timeout, a body cap, and a polite retry on 429 (WhatsOnChain's free
// tier rate-limits when fetching many raw txs in quick succession).
func httpGetBytes(url string, maxBytes int64) ([]byte, error) {
	c := &http.Client{Timeout: 60 * time.Second}
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff capped at 16s. Generous because
			// the tool runs once per fixture refresh, not on the
			// hot path.
			delay := time.Duration(1<<attempt) * time.Second
			if delay > 16*time.Second {
				delay = 16 * time.Second
			}
			time.Sleep(delay)
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		resp, err := c.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("GET %s: 429 (attempt %d/%d)", url, attempt+1, maxAttempts)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GET %s: status %d body=%s", url, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if readErr != nil {
			return nil, readErr
		}
		return body, nil
	}
	return nil, lastErr
}

// fetchBlockTxIDs returns the in-block-order txid list (display order)
// for a block. WhatsOnChain caps the inline `tx` field at the first 100
// txids and surfaces the rest behind pagination links in `pages.uri`.
// We follow every page so blocks of any size return their full txid
// list.
func fetchBlockTxIDs(wocBase, blockHash string) ([]string, error) {
	body, err := httpGetBytes(wocBase+"/v1/bsv/main/block/hash/"+blockHash, 32*1024*1024)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Tx    []string `json:"tx"`
		Pages struct {
			URI []string `json:"uri"`
		} `json:"pages"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode block JSON: %w", err)
	}
	if len(resp.Tx) == 0 {
		return nil, fmt.Errorf("WhatsOnChain returned no txids for block %s", blockHash)
	}
	all := resp.Tx
	for _, uri := range resp.Pages.URI {
		// Page URIs in the response are server-relative
		// ("/block/hash/<h>/page/<n>") and don't include the
		// /v1/bsv/main prefix; rebuild against wocBase.
		pageURL := wocBase + "/v1/bsv/main" + uri
		pageBody, err := httpGetBytes(pageURL, 32*1024*1024)
		if err != nil {
			return nil, fmt.Errorf("fetch page %s: %w", uri, err)
		}
		var pageTxs []string
		if err := json.Unmarshal(pageBody, &pageTxs); err != nil {
			return nil, fmt.Errorf("decode page %s: %w", uri, err)
		}
		all = append(all, pageTxs...)
	}
	return all, nil
}

// fetchRawTx returns the raw tx bytes for a txid. WhatsOnChain serves
// hex at /v1/bsv/main/tx/<txid>/hex.
func fetchRawTx(wocBase, txid string) ([]byte, error) {
	body, err := httpGetBytes(wocBase+"/v1/bsv/main/tx/"+txid+"/hex", 4*1024*1024)
	if err != nil {
		return nil, err
	}
	hexStr := strings.TrimSpace(string(body))
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		preview := hexStr
		if len(preview) > 64 {
			preview = preview[:64]
		}
		return nil, fmt.Errorf("decode hex: %w (preview=%q)", err, preview)
	}
	return raw, nil
}
