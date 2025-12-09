package chaintracks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// loadHeadersFromFile reads a binary .headers file and returns a slice of headers
// This function performs no validation - just parsing
func loadHeadersFromFile(path string) ([]*block.Header, error) {
	data, err := os.ReadFile(path) //nolint:gosec // Path is constructed internally, not from user input
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	if len(data)%80 != 0 {
		return nil, fmt.Errorf("%w: %d bytes (not multiple of 80)", ErrInvalidFileSize, len(data))
	}

	headerCount := len(data) / 80
	headers := make([]*block.Header, 0, headerCount)

	for i := 0; i < headerCount; i++ {
		headerBytes := data[i*80 : (i+1)*80]
		header, err := block.NewHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse header at index %d: %w", i, err)
		}
		headers = append(headers, header)
	}

	return headers, nil
}

// parseMetadata reads and parses the metadata JSON file
func parseMetadata(path string) (*CDNMetadata, error) {
	data, err := os.ReadFile(path) //nolint:gosec // Path is constructed internally, not from user input
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata CDNMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}

	return &metadata, nil
}

// loadFromLocalFiles restores the chain from local header files
// First checks the storage path, then falls back to embedded checkpoint files
// No validation is performed - we trust our own checkpoint and exported files
func (cm *ChainManager) loadFromLocalFiles(ctx context.Context) error {
	// First try storage path (user's saved state)
	metadataPath := filepath.Join(cm.localStoragePath, cm.network+"NetBlockHeaders.json")
	basePath := cm.localStoragePath

	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// Fall back to embedded checkpoint files
		checkpointPath := filepath.Join("data", "headers")
		checkpointMetadata := filepath.Join(checkpointPath, cm.network+"NetBlockHeaders.json")

		if _, err := os.Stat(checkpointMetadata); os.IsNotExist(err) {
			log.Printf("No checkpoint files found at %s or %s, starting with empty chain", metadataPath, checkpointMetadata)
			return nil
		}

		log.Printf("Loading from embedded checkpoint: %s", checkpointMetadata)
		metadataPath = checkpointMetadata
		basePath = checkpointPath
	} else {
		log.Printf("Loading from storage path: %s", metadataPath)
	}

	metadata, err := parseMetadata(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	log.Printf("Found %d checkpoint files to load", len(metadata.Files))

	for _, fileEntry := range metadata.Files {
		filePath := filepath.Join(basePath, fileEntry.FileName)
		headers, err := loadHeadersFromFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to load file %s: %w", fileEntry.FileName, err)
		}

		blockHeaders := make([]*BlockHeader, 0, len(headers))

		// Calculate chainwork incrementally
		var prevChainWork *big.Int
		if fileEntry.FirstHeight == 0 {
			prevChainWork = big.NewInt(0)
		} else {
			// Get the chainwork from the previous block (last block of previous file)
			prevHeader, err := cm.GetHeaderByHeight(ctx, fileEntry.FirstHeight-1)
			if err != nil {
				return fmt.Errorf("failed to get previous header at height %d: %w", fileEntry.FirstHeight-1, err)
			}
			prevChainWork = prevHeader.ChainWork
		}

		for i, header := range headers {
			height := fileEntry.FirstHeight + uint32(i) //nolint:gosec // Loop index bounded by slice length

			var chainWork *big.Int
			if height == 0 {
				chainWork = big.NewInt(0)
			} else {
				work := CalculateWork(header.Bits)
				chainWork = new(big.Int).Add(prevChainWork, work)
				prevChainWork = chainWork
			}

			blockHeader := &BlockHeader{
				Header:    header,
				Height:    height,
				Hash:      header.Hash(),
				ChainWork: chainWork,
			}

			blockHeaders = append(blockHeaders, blockHeader)
		}

		if err := cm.SetChainTip(ctx, blockHeaders); err != nil {
			return fmt.Errorf("failed to set chain tip for file %s: %w", fileEntry.FileName, err)
		}
	}

	return nil
}

// SetChainTip updates the chain tip with a new branch of headers
// branchHeaders should be ordered from oldest to newest
// The parent of branchHeaders[0] must exist in our current chain
//
//nolint:gocyclo // Complex validation and reorganization logic
func (cm *ChainManager) SetChainTip(ctx context.Context, branchHeaders []*BlockHeader) error {
	if len(branchHeaders) == 0 {
		return nil
	}

	// Update in-memory chain
	cm.mu.Lock()

	// Update byHeight for all blocks in the new branch
	for _, header := range branchHeaders {
		// hash := header.Hash()

		// Ensure slice is large enough
		for uint32(len(cm.byHeight)) <= header.Height { //nolint:gosec // Height is validated before storage
			cm.byHeight = append(cm.byHeight, chainhash.Hash{})
		}

		// Update byHeight and byHash
		cm.byHeight[header.Height] = header.Hash
		cm.byHash[header.Hash] = header
	}

	// Clear any blocks after the new tip (handles reorg to shorter chain)
	newTipHeight := branchHeaders[len(branchHeaders)-1].Height
	if uint32(len(cm.byHeight)) > newTipHeight+1 { //nolint:gosec // Length comparison for slice truncation
		cm.byHeight = cm.byHeight[:newTipHeight+1]
	}

	// Always set tip to the last header in the branch
	cm.tip = branchHeaders[len(branchHeaders)-1]

	// Prune orphaned headers older than 100 blocks
	cm.pruneOrphans()

	// Get channel reference before unlocking
	msgChan := cm.msgChan
	cm.mu.Unlock()

	// Publish tip change event outside the lock (non-blocking)
	if msgChan != nil {
		// Drain any old tip (we only care about the latest)
		select {
		case <-msgChan:
		default:
		}

		// Send the new tip (non-blocking)
		select {
		case msgChan <- cm.tip:
		default:
			// Channel full after drain shouldn't happen, but skip if it does
		}
	}

	// Write headers to files
	startWrite := time.Now()
	if err := cm.writeHeadersToFiles(branchHeaders); err != nil {
		return fmt.Errorf("failed to write headers to files: %w", err)
	}
	writeDuration := time.Since(startWrite)

	// Update metadata
	startMeta := time.Now()
	if err := cm.updateMetadataForTip(ctx); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}
	metaDuration := time.Since(startMeta)

	if writeDuration > 100*time.Millisecond || metaDuration > 100*time.Millisecond {
		log.Printf("SetChainTip timing: write=%v meta=%v", writeDuration, metaDuration)
	}

	return nil
}

// writeHeadersToFiles writes headers to the appropriate .headers files
func (cm *ChainManager) writeHeadersToFiles(headers []*BlockHeader) error {
	if cm.localStoragePath == "" {
		return nil
	}

	if err := os.MkdirAll(cm.localStoragePath, 0o750); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Group headers by file
	fileHeaders := make(map[uint32][]*BlockHeader)
	for _, header := range headers {
		fileIndex := header.Height / 100000
		fileHeaders[fileIndex] = append(fileHeaders[fileIndex], header)
	}

	// Write to each file
	for fileIndex, hdrs := range fileHeaders {
		fileName := fmt.Sprintf("%sNet_%d.headers", cm.network, fileIndex)
		filePath := filepath.Join(cm.localStoragePath, fileName)

		// Open file for read/write (create if doesn't exist)
		f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o600) //nolint:gosec // Path is constructed internally
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", fileName, err)
		}

		// Write each header at its position
		for _, header := range hdrs {
			positionInFile := (header.Height % 100000) * 80
			if _, err := f.Seek(int64(positionInFile), 0); err != nil {
				_ = f.Close()
				return fmt.Errorf("failed to seek in file: %w", err)
			}

			headerBytes := header.Bytes()
			if _, err := f.Write(headerBytes); err != nil {
				_ = f.Close()
				return fmt.Errorf("failed to write header: %w", err)
			}
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
	}

	return nil
}

// updateMetadataForTip updates the metadata JSON with current chain tip info
func (cm *ChainManager) updateMetadataForTip(ctx context.Context) error {
	if cm.localStoragePath == "" {
		return nil
	}

	metadataPath := filepath.Join(cm.localStoragePath, cm.network+"NetBlockHeaders.json")

	// Read existing metadata or create new
	var metadata *CDNMetadata
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		metadata = &CDNMetadata{
			RootFolder:     "",
			JSONFilename:   cm.network + "NetBlockHeaders.json",
			HeadersPerFile: 100000,
			Files:          []CDNFileEntry{},
		}
	} else {
		metadata, err = parseMetadata(metadataPath)
		if err != nil {
			return fmt.Errorf("failed to parse existing metadata: %w", err)
		}
	}

	// Update file entries based on current chain
	tip := cm.GetTip(ctx)
	if tip == nil {
		return nil
	}

	fileIndex := tip.Height / 100000

	// Ensure we have entries for all files up to the current tip
	for i := uint32(len(metadata.Files)); i <= fileIndex; i++ { //nolint:gosec // Files length is bounded by storage capacity
		metadata.Files = append(metadata.Files, CDNFileEntry{
			Chain:         cm.network,
			Count:         0,
			FileHash:      "",
			FileName:      fmt.Sprintf("%sNet_%d.headers", cm.network, i),
			FirstHeight:   i * 100000,
			LastChainWork: "0000000000000000000000000000000000000000000000000000000000000000",
			PrevChainWork: "0000000000000000000000000000000000000000000000000000000000000000",
			SourceURL:     "",
		})
	}

	// Update the last file entry with current tip info
	lastFileEntry := &metadata.Files[fileIndex]
	lastFileEntry.Count = int((tip.Height % 100000) + 1)
	lastFileEntry.LastChainWork = ChainWorkToHex(tip.ChainWork)
	lastFileEntry.LastHash = tip.Hash

	// Get previous header for prevChainWork and prevHash
	if tip.Height > 0 {
		prevHeader, err := cm.GetHeaderByHeight(ctx, tip.Height-1)
		if err == nil {
			lastFileEntry.PrevChainWork = ChainWorkToHex(prevHeader.ChainWork)
			lastFileEntry.PrevHash = prevHeader.Hash
		}
	}

	// Write updated metadata
	return cm.writeLocalMetadata(metadata)
}

// writeLocalMetadata writes the metadata JSON to local storage
func (cm *ChainManager) writeLocalMetadata(metadata *CDNMetadata) error {
	if cm.localStoragePath == "" {
		return nil
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metadataPath := filepath.Join(cm.localStoragePath, cm.network+"NetBlockHeaders.json")
	if err := os.WriteFile(metadataPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

