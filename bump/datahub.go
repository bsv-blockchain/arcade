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

// FetchBlockDataForBUMP fetches subtree hashes, coinbase BUMP, and the block's
// header merkle root from the binary block endpoint, trying all DataHub URLs.
// Each attempt emits a Debug-level log line so operators can see which URLs
// were tried, the HTTP status, elapsed time, and per-URL error. logger may be
// nil — in that case the per-attempt logs are silently dropped.
//
// Binary format: header (80) | txCount (varint) | sizeBytes (varint) |
// subtreeCount (varint) | subtreeHashes (N×32) | coinbaseTx (variable) |
// blockHeight (varint) | coinbaseBUMPLen (varint) | coinbaseBUMP (variable)
func FetchBlockDataForBUMP(ctx context.Context, datahubURLs []string, blockHash string, logger *zap.Logger) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, headerMerkleRoot *chainhash.Hash, err error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if len(datahubURLs) == 0 {
		return nil, nil, nil, fmt.Errorf("no DataHub URLs configured (static + discovered list is empty) for block %s", blockHash)
	}
	var urlErrors []string
	for i, dataHubURL := range datahubURLs {
		start := time.Now()
		hashes, cbBUMP, root, status, fetchErr := fetchBlockBinary(ctx, dataHubURL, blockHash)
		logger.Debug("datahub fetch attempt",
			zap.Int("idx", i),
			zap.String("url", dataHubURL),
			zap.Int("status", status),
			zap.Duration("elapsed", time.Since(start)),
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
func fetchBlockBinary(ctx context.Context, baseURL, blockHash string) ([]chainhash.Hash, []byte, *chainhash.Hash, int, error) {
	url := fmt.Sprintf("%s/block/%s", baseURL, blockHash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, nil, nil, resp.StatusCode, fmt.Errorf("GET %s: status %d (body: %s)", url, resp.StatusCode, string(body))
	}

	// Read entire response into memory so we can use NewTransactionFromStream
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, nil, resp.StatusCode, fmt.Errorf("failed to read response body: %w", err)
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
	if _, err := txCount.ReadFrom(r); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read transaction count: %w", err)
	}

	// Read size in bytes (varint)
	var sizeBytes util.VarInt
	if _, err := sizeBytes.ReadFrom(r); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read size in bytes: %w", err)
	}

	// Read subtree count (varint)
	var subtreeCount util.VarInt
	if _, err := subtreeCount.ReadFrom(r); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read subtree count: %w", err)
	}

	// Read subtree hashes (32 bytes each)
	hashes := make([]chainhash.Hash, 0, uint64(subtreeCount))
	hashBuf := make([]byte, 32)
	for i := uint64(0); i < uint64(subtreeCount); i++ {
		if _, err := io.ReadFull(r, hashBuf); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to read subtree hash %d: %w", i, err)
		}
		hash, err := chainhash.NewHash(hashBuf)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create hash: %w", err)
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
