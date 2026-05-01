package bump

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/util"
	"go.uber.org/zap"
)

var httpClient = &http.Client{Timeout: 5 * time.Minute}

// DefaultMaxBlockBytes caps a single /block/<hash> binary response from a
// DataHub endpoint. The endpoint serves block metadata: 80-byte header,
// transaction-count varints, the 32-byte subtree-hash list, the coinbase
// transaction, and the coinbase BUMP. Even on a Teranode network with
// millions of subtrees per block this stays well under a hundred MiB; 1 GiB
// is two-plus orders of magnitude of headroom while still bounding memory
// against a hostile or malfunctioning DataHub. See finding F-007.
const DefaultMaxBlockBytes int64 = 1 * 1024 * 1024 * 1024 // 1 GiB

// maxErrorBodyBytes caps how much of a non-200 response body we read into the
// returned error string. The HTTP status itself carries the diagnostic
// signal — body is just for human debugging — so a small cap is fine and
// stops a hostile server from inflating arcade's log lines.
const maxErrorBodyBytes int64 = 512

// FetchBlockDataForBUMP fetches subtree hashes, coinbase BUMP, and the block's
// header merkle root from the binary block endpoint, trying all DataHub URLs.
// Each attempt emits a Debug-level log line so operators can see which URLs
// were tried, the HTTP status, elapsed time, and per-URL error. logger may be
// nil — in that case the per-attempt logs are silently dropped.
//
// Response bodies are read through an io.LimitReader and Content-Length is
// inspected before reading, so a hostile or malfunctioning DataHub cannot
// exhaust process memory by returning an unbounded body. The cap is the
// package-level default (DefaultMaxBlockBytes); use FetchBlockDataForBUMPWithCap
// to override from configuration.
//
// Binary format: header (80) | txCount (varint) | sizeBytes (varint) |
// subtreeCount (varint) | subtreeHashes (N×32) | coinbaseTx (variable) |
// blockHeight (varint) | coinbaseBUMPLen (varint) | coinbaseBUMP (variable)
func FetchBlockDataForBUMP(ctx context.Context, datahubURLs []string, blockHash string, logger *zap.Logger) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, headerMerkleRoot *chainhash.Hash, err error) {
	return FetchBlockDataForBUMPWithCap(ctx, datahubURLs, blockHash, DefaultMaxBlockBytes, logger)
}

// FetchBlockDataForBUMPWithCap is the cap-aware variant of FetchBlockDataForBUMP.
// maxBlockBytes <= 0 selects DefaultMaxBlockBytes — passing zero/negative does
// not silently disable the protection.
func FetchBlockDataForBUMPWithCap(ctx context.Context, datahubURLs []string, blockHash string, maxBlockBytes int64, logger *zap.Logger) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, headerMerkleRoot *chainhash.Hash, err error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if maxBlockBytes <= 0 {
		maxBlockBytes = DefaultMaxBlockBytes
	}
	if len(datahubURLs) == 0 {
		return nil, nil, nil, fmt.Errorf("no DataHub URLs configured (static + discovered list is empty) for block %s", blockHash)
	}
	var urlErrors []string
	for i, dataHubURL := range datahubURLs {
		start := time.Now()
		hashes, cbBUMP, root, status, fetchErr := fetchBlockBinary(ctx, dataHubURL, blockHash, maxBlockBytes)
		logger.Debug("datahub fetch attempt",
			zap.Int("idx", i),
			zap.String("url", dataHubURL),
			zap.Int("status", status),
			zap.Duration("elapsed", time.Since(start)),
			zap.Int64("max_block_bytes", maxBlockBytes),
			zap.Error(fetchErr),
		)
		if fetchErr == nil {
			return hashes, cbBUMP, root, nil
		}
		urlErrors = append(urlErrors, fmt.Sprintf("url[%d] %q: %v", i, dataHubURL, fetchErr))
	}
	return nil, nil, nil, fmt.Errorf("all DataHub URLs failed for block %s:\n  %s", blockHash, strings.Join(urlErrors, "\n  "))
}

// fetchBlockBinary fetches a block from the binary endpoint and parses
// subtree hashes, coinbase BUMP, and the header merkle root from the response.
// Returns the HTTP status code (0 on transport error before a response was
// received) so the caller can include it in its per-attempt log line.
//
// maxBlockBytes bounds the response body size: Content-Length is rejected
// before reading when advertised oversize, and the body itself is read
// through an io.LimitReader of maxBlockBytes+1 so we can distinguish
// "exactly at cap" (allowed) from "exceeded cap" (rejected) without ever
// buffering more than the cap.
func fetchBlockBinary(ctx context.Context, baseURL, blockHash string, maxBlockBytes int64) ([]chainhash.Hash, []byte, *chainhash.Hash, int, error) {
	url := fmt.Sprintf("%s/block/%s", baseURL, blockHash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return nil, nil, nil, resp.StatusCode, fmt.Errorf("GET %s: status %d (body: %s)", url, resp.StatusCode, string(body))
	}

	// Reject advertised oversize responses without reading any of the body.
	// resp.ContentLength is -1 when the server omits the header or uses
	// chunked encoding; in that case we fall through to the LimitReader
	// check below, which still bounds memory.
	if resp.ContentLength >= 0 && resp.ContentLength > maxBlockBytes {
		return nil, nil, nil, resp.StatusCode, fmt.Errorf("GET %s: Content-Length %d exceeds cap of %d bytes", url, resp.ContentLength, maxBlockBytes)
	}

	// Read entire response into memory so we can use NewTransactionFromStream.
	// Read maxBlockBytes+1 so a body that lands exactly at the cap is
	// accepted but anything larger is detected and rejected.
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBlockBytes+1))
	if err != nil {
		return nil, nil, nil, resp.StatusCode, fmt.Errorf("failed to read response body: %w", err)
	}
	if int64(len(data)) > maxBlockBytes {
		return nil, nil, nil, resp.StatusCode, fmt.Errorf("GET %s: response body exceeds %d bytes", url, maxBlockBytes)
	}

	hashes, cb, root, parseErr := parseBlockBinary(data)
	return hashes, cb, root, resp.StatusCode, parseErr
}

// parseBlockBinary parses the binary block format:
// header (80) | txCount (varint) | sizeBytes (varint) |
// subtreeCount (varint) | subtreeHashes (N×32) | coinbaseTx (variable) |
// blockHeight (varint) | coinbaseBUMPLen (varint) | coinbaseBUMP (variable)
//
// The 80-byte header layout is: version (4) | prevBlockHash (32) |
// merkleRoot (32) | time (4) | bits (4) | nonce (4).
func parseBlockBinary(data []byte) ([]chainhash.Hash, []byte, *chainhash.Hash, error) {
	if len(data) < 80 {
		return nil, nil, nil, fmt.Errorf("block data too short for header: %d bytes", len(data))
	}

	headerMerkleRoot, err := chainhash.NewHash(data[36:68])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse header merkle root: %w", err)
	}

	r := bytes.NewReader(data[80:]) // skip block header

	// Read transaction count (varint)
	var txCount util.VarInt
	if _, rErr := txCount.ReadFrom(r); rErr != nil {
		return nil, nil, nil, fmt.Errorf("failed to read transaction count: %w", rErr)
	}

	// Read size in bytes (varint)
	var sizeBytes util.VarInt
	if _, rErr := sizeBytes.ReadFrom(r); rErr != nil {
		return nil, nil, nil, fmt.Errorf("failed to read size in bytes: %w", rErr)
	}

	// Read subtree count (varint)
	var subtreeCount util.VarInt
	if _, rErr := subtreeCount.ReadFrom(r); rErr != nil {
		return nil, nil, nil, fmt.Errorf("failed to read subtree count: %w", rErr)
	}

	// Read subtree hashes (32 bytes each)
	hashes := make([]chainhash.Hash, 0, uint64(subtreeCount))
	hashBuf := make([]byte, 32)
	for i := uint64(0); i < uint64(subtreeCount); i++ {
		if _, rErr := io.ReadFull(r, hashBuf); rErr != nil {
			return nil, nil, nil, fmt.Errorf("failed to read subtree hash %d: %w", i, rErr)
		}
		hash, hErr := chainhash.NewHash(hashBuf)
		if hErr != nil {
			return nil, nil, nil, fmt.Errorf("failed to create hash: %w", hErr)
		}
		hashes = append(hashes, *hash)
	}

	// Parse coinbase transaction (variable length) to skip past it
	remaining := data[len(data)-r.Len():]
	_, txBytesUsed, err := transaction.NewTransactionFromStream(remaining)
	if err != nil {
		// Coinbase tx parsing failed — return subtree hashes without coinbase BUMP
		return hashes, nil, headerMerkleRoot, nil
	}

	r = bytes.NewReader(remaining[txBytesUsed:])

	// Read block height (varint)
	var blockHeight util.VarInt
	if _, err := blockHeight.ReadFrom(r); err != nil {
		return hashes, nil, headerMerkleRoot, nil
	}

	// Read coinbase BUMP length (varint)
	var cbBUMPLen util.VarInt
	if _, err := cbBUMPLen.ReadFrom(r); err != nil {
		return hashes, nil, headerMerkleRoot, nil
	}

	if uint64(cbBUMPLen) == 0 {
		return hashes, nil, headerMerkleRoot, nil
	}

	// Read coinbase BUMP data
	coinbaseBUMP := make([]byte, uint64(cbBUMPLen))
	if _, err := io.ReadFull(r, coinbaseBUMP); err != nil {
		return hashes, nil, headerMerkleRoot, nil
	}

	return hashes, coinbaseBUMP, headerMerkleRoot, nil
}
