package chaintracks

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
)

const (
	maxHeadersPerRequest = 1000
	headerSize           = 80
)

// SyncFromRemoteTip walks backwards from a remote tip to find common ancestor,
// then imports the entire branch in one operation. This is used for both
// bootstrap sync and P2P block messages with unknown parents.
//
//nolint:gocyclo // Complex sync and ancestor finding logic
func (cm *ChainManager) SyncFromRemoteTip(ctx context.Context, remoteTipHash chainhash.Hash, baseURL string) error {
	// Check if we already have the remote tip
	if _, err := cm.GetHeaderByHash(ctx, &remoteTipHash); err == nil {
		log.Printf("Already have block %s", remoteTipHash.String())
		return nil
	}

	// Walk backwards from remote tip to find common ancestor
	log.Printf("Walking backwards from remote tip %s to find common ancestor", remoteTipHash.String())
	branch := make([]*block.Header, 0, 10000)
	currentHash := remoteTipHash
	var commonAncestor *BlockHeader

	startTime := time.Now()
	for {
		// Check if we have this block in our chain
		existingHeader, err := cm.GetHeaderByHash(ctx, &currentHash)
		if err == nil {
			// Found common ancestor!
			commonAncestor = existingHeader
			log.Printf("Found common ancestor at height %d after %v", commonAncestor.Height, time.Since(startTime))
			break
		}

		// Fetch batch of headers walking backwards
		startFetch := time.Now()
		headers, err := fetchHeadersBackward(baseURL, currentHash.String(), maxHeadersPerRequest)
		fetchDuration := time.Since(startFetch)
		if err != nil {
			return fmt.Errorf("failed to fetch headers walking backward from %s: %w", currentHash.String(), err)
		}

		if len(headers) == 0 {
			return fmt.Errorf("%w from %s/headers/%s?n=%d", ErrNoHeadersReturned, baseURL, currentHash.String(), maxHeadersPerRequest)
		}

		log.Printf("Fetched %d headers in %v from %s/headers/%s?n=%d", len(headers), fetchDuration, baseURL, currentHash.String(), maxHeadersPerRequest)

		// Add headers to branch (they're in reverse order - newest first)
		branch = append(branch, headers...)

		// Check if any of the fetched headers exist in our chain
		found := false
		for i, header := range headers {
			hash := header.Hash()
			if existingHeader, err := cm.GetHeaderByHash(ctx, &hash); err == nil {
				// Found common ancestor!
				commonAncestor = existingHeader
				// Trim the branch to only include headers after the common ancestor
				branch = branch[:len(branch)-len(headers)+i]
				found = true
				log.Printf("Found common ancestor at height %d after %v", commonAncestor.Height, time.Since(startTime))
				break
			}
		}

		if found {
			break
		}

		// Continue from the last header's parent
		currentHash = headers[len(headers)-1].PrevHash
	}

	if commonAncestor == nil {
		return ErrCommonAncestorNotFound
	}

	if len(branch) == 0 {
		log.Printf("No new headers to sync")
		return nil
	}

	log.Printf("Found %d new headers to import", len(branch))

	// Reverse branch (it's currently newest to oldest, we need oldest to newest)
	for i := 0; i < len(branch)/2; i++ {
		branch[i], branch[len(branch)-1-i] = branch[len(branch)-1-i], branch[i]
	}

	// Calculate heights and chainwork for the entire branch
	startConvert := time.Now()
	blockHeaders := make([]*BlockHeader, len(branch))
	currentHeight := commonAncestor.Height + 1
	currentChainWork := commonAncestor.ChainWork

	for i, header := range branch {
		work := CalculateWork(header.Bits)
		currentChainWork = new(big.Int).Add(currentChainWork, work)

		blockHeaders[i] = &BlockHeader{
			Header:    header,
			Height:    currentHeight,
			Hash:      header.Hash(),
			ChainWork: new(big.Int).Set(currentChainWork),
		}
		currentHeight++
	}
	log.Printf("Calculated chainwork for %d headers in %v", len(blockHeaders), time.Since(startConvert))

	// Import entire branch in one operation
	startSetTip := time.Now()
	if err := cm.SetChainTip(ctx, blockHeaders); err != nil {
		return fmt.Errorf("failed to set chain tip: %w", err)
	}
	log.Printf("SetChainTip took %v", time.Since(startSetTip))

	newTip := cm.GetTip(ctx)
	log.Printf("Sync complete. New chain tip: %s at height %d (added %d headers)",
		newTip.Header.Hash().String(), newTip.Height, len(blockHeaders))

	return nil
}

// FetchLatestBlock gets the latest block hash from the node's bestblockheader endpoint
func FetchLatestBlock(ctx context.Context, baseURL string) (chainhash.Hash, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/bestblockheader", baseURL), nil)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to fetch best block header: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return chainhash.Hash{}, fmt.Errorf("%w: status %d", ErrBestBlockHeaderFailed, resp.StatusCode)
	}

	headerBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to read response: %w", err)
	}

	if len(headerBytes) != headerSize {
		return chainhash.Hash{}, fmt.Errorf("%w: expected %d, got %d", ErrInvalidHeaderSize, headerSize, len(headerBytes))
	}

	header, err := block.NewHeaderFromBytes(headerBytes)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("failed to parse header: %w", err)
	}

	return header.Hash(), nil
}

// fetchHeadersBackward fetches headers walking backwards from a starting hash
// Uses the /headers/:hash endpoint which traverses backwards (child -> parent)
// Returns headers in reverse chronological order (newest first)
func fetchHeadersBackward(baseURL, startHash string, count int) ([]*block.Header, error) {
	// Use binary endpoint for efficiency (80 bytes per header vs 160 for hex)
	url := fmt.Sprintf("%s/headers/%s?n=%d", baseURL, startHash, count)

	resp, err := http.Get(url) //nolint:noctx,gosec // URL is constructed from validated inputs
	if err != nil {
		return nil, fmt.Errorf("failed to fetch headers: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrServerRequestFailed, resp.StatusCode)
	}

	headerBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if len(headerBytes)%headerSize != 0 {
		return nil, fmt.Errorf("%w: %d bytes", ErrInvalidHeaderDataLength, len(headerBytes))
	}

	numHeaders := len(headerBytes) / headerSize
	headers := make([]*block.Header, numHeaders)

	for i := 0; i < numHeaders; i++ {
		start := i * headerSize
		end := start + headerSize
		headerData := headerBytes[start:end]

		header, err := block.NewHeaderFromBytes(headerData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse header %d: %w", i, err)
		}

		headers[i] = header
	}

	return headers, nil
}
