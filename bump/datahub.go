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

// BlockDataValidator is an optional response-acceptance predicate run after a
// successful datahub fetch. Returning a non-nil error causes the fetch loop
// to discard that peer's response (logging the reason into urlErrors) and try
// the next URL — turning the otherwise-greedy "first 200 wins" into a
// "first 200 that passes validation wins" policy.
//
// Validators are stateless and pure functions of the response; they receive
// what was parsed from one peer and decide whether it's plausible. Callers
// use this to reject obviously-truncated responses (a pruned peer returning
// fewer subtrees than the STUMPs reference) and to cross-check the header
// merkle root against an out-of-band canonical value (chaintracks).
type BlockDataValidator func(subtreeHashes []chainhash.Hash, headerMerkleRoot *chainhash.Hash) error

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
	return FetchBlockDataForBUMPWithOptions(ctx, datahubURLs, blockHash, DefaultMaxBlockBytes, nil, logger)
}

// FetchBlockDataForBUMPWithCap is the cap-aware variant of FetchBlockDataForBUMP.
// maxBlockBytes <= 0 selects DefaultMaxBlockBytes — passing zero/negative does
// not silently disable the protection.
//
// Retained for backwards compatibility with callers that don't need to plug
// in a content validator. Equivalent to FetchBlockDataForBUMPWithOptions with
// validator=nil.
func FetchBlockDataForBUMPWithCap(ctx context.Context, datahubURLs []string, blockHash string, maxBlockBytes int64, logger *zap.Logger) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, headerMerkleRoot *chainhash.Hash, err error) {
	return FetchBlockDataForBUMPWithOptions(ctx, datahubURLs, blockHash, maxBlockBytes, nil, logger)
}

// FetchBlockDataForBUMPWithOptions adds an optional BlockDataValidator that
// runs after each successful HTTP+parse. A non-nil error from the validator
// is treated the same as a transport error: log it, append to urlErrors, and
// try the next URL. This turns the loop into "first 200-that-passes-validation
// wins" — defending against datahubs that return self-consistent-but-wrong
// block representations (pruned peers, stale caches).
func FetchBlockDataForBUMPWithOptions(ctx context.Context, datahubURLs []string, blockHash string, maxBlockBytes int64, validator BlockDataValidator, logger *zap.Logger) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, headerMerkleRoot *chainhash.Hash, err error) {
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
		logger.Debug(
			"datahub fetch attempt",
			zap.Int("idx", i),
			zap.String("url", dataHubURL),
			zap.Int("status", status),
			zap.Duration("elapsed", time.Since(start)),
			zap.Int64("max_block_bytes", maxBlockBytes),
			zap.Error(fetchErr),
		)
		if fetchErr != nil {
			urlErrors = append(urlErrors, fmt.Sprintf("url[%d] %q: %v", i, dataHubURL, fetchErr))
			continue
		}
		if validator != nil {
			if vErr := validator(hashes, root); vErr != nil {
				logger.Warn(
					"datahub response rejected by validator",
					zap.Int("idx", i),
					zap.String("url", dataHubURL),
					zap.Int("subtree_count", len(hashes)),
					zap.Error(vErr),
				)
				urlErrors = append(urlErrors, fmt.Sprintf("url[%d] %q: validator rejected: %v", i, dataHubURL, vErr))
				continue
			}
		}
		// Reject endpoints whose coinbase BUMP does not reconcile to the header
		// merkle root they served. The coinbase tx sits at level 0 offset 0 of
		// the block merkle tree, so its BUMP MUST compute that header root; an
		// endpoint that serves a coinbase BUMP computing a different root is
		// internally inconsistent (stale/pruned/buggy peer) and would only
		// produce a compound BUMP that fails the post-build ValidateCompoundRoot.
		// Without this, a bad peer that sorts ahead of a good one is selected on
		// every fetch and every retry, deterministically blocking the block from
		// ever building a valid BUMP even when a consistent peer is healthy.
		// Skipping it here lets the loop fall through to that consistent peer.
		if cbErr := coinbaseBUMPReconciles(cbBUMP, root); cbErr != nil {
			logger.Warn(
				"datahub coinbase BUMP does not reconcile to header merkle root",
				zap.Int("idx", i),
				zap.String("url", dataHubURL),
				zap.Error(cbErr),
			)
			urlErrors = append(urlErrors, fmt.Sprintf("url[%d] %q: coinbase BUMP mismatch: %v", i, dataHubURL, cbErr))
			continue
		}
		return hashes, cbBUMP, root, nil
	}
	return nil, nil, nil, fmt.Errorf("all DataHub URLs failed for block %s:\n  %s", blockHash, strings.Join(urlErrors, "\n  "))
}

// SubtreeCountValidator returns a BlockDataValidator that rejects responses
// with fewer than minSubtrees subtree hashes. Pass max(stump.SubtreeIndex)+1
// when STUMPs are already in hand — a peer whose response can't index every
// STUMP we've collected is provably wrong, no expensive cryptographic check
// required.
//
// minSubtrees <= 0 returns nil — no validation, which preserves the old
// "first 200 wins" behavior for callers that haven't yet collected STUMPs.
func SubtreeCountValidator(minSubtrees int) BlockDataValidator {
	if minSubtrees <= 0 {
		return nil
	}
	return func(subtreeHashes []chainhash.Hash, _ *chainhash.Hash) error {
		if len(subtreeHashes) < minSubtrees {
			return fmt.Errorf("subtree_count %d < required %d (STUMPs reference subtree indexes up to %d)",
				len(subtreeHashes), minSubtrees, minSubtrees-1)
		}
		return nil
	}
}

// coinbaseBUMPReconciles verifies that the coinbase BUMP computes the expected
// block-header merkle root. The coinbase transaction is leaf 0 of the block
// merkle tree, so folding it up its own BUMP must yield the header merkle root.
//
// Returns nil when there is nothing to check: a nil expected root, or an empty
// coinbase BUMP (some peers legitimately omit it; downstream BUMP construction
// handles a missing coinbase BUMP separately, so we must not reject those here).
func coinbaseBUMPReconciles(coinbaseBUMP []byte, headerMerkleRoot *chainhash.Hash) error {
	if headerMerkleRoot == nil || len(coinbaseBUMP) == 0 {
		return nil
	}
	cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
	if err != nil {
		return fmt.Errorf("parse coinbase BUMP: %w", err)
	}
	cbTxID := extractCoinbaseTxID(coinbaseBUMP)
	if cbTxID == nil {
		return fmt.Errorf("coinbase BUMP has no coinbase txid at level 0 offset 0")
	}
	got, err := cbPath.ComputeRoot(cbTxID)
	if err != nil {
		return fmt.Errorf("compute coinbase BUMP root: %w", err)
	}
	if !got.IsEqual(headerMerkleRoot) {
		return fmt.Errorf("coinbase BUMP root %s != header merkle root %s", got, headerMerkleRoot)
	}
	return nil
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
