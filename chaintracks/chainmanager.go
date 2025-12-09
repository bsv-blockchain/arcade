// Package chaintracks provides blockchain header tracking and management functionality.
package chaintracks

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/bsv-blockchain/arcade/p2p"
	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// ChainManager is the main orchestrator for chain management
type ChainManager struct {
	mu sync.RWMutex

	byHeight []chainhash.Hash                // Main chain hashes indexed by height
	byHash   map[chainhash.Hash]*BlockHeader // Hash → Header (all headers: main + orphans)
	tip      *BlockHeader                    // Current chain tip

	localStoragePath string
	network          string

	// P2P fields
	p2pClient *p2p.Client       // P2P client for network communication
	msgChan   chan *BlockHeader // Channel for broadcasting tip changes to consumers
}

// NewChainManager creates a new ChainManager and restores from local files if present
// If p2pClient is provided, it will use that instead of creating its own
// If bootstrapURL is provided, it will sync from a remote teranode before returning
func NewChainManager(ctx context.Context, network, localStoragePath string, p2pClient *p2p.Client, bootstrapURL ...string) (*ChainManager, error) {
	// Default to ~/.chaintracks if no path provided
	if localStoragePath == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get user home directory: %w", err)
		}
		localStoragePath = filepath.Join(homeDir, ".chaintracks")
	} else if localStoragePath[0] == '~' {
		// Expand ~ to home directory
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get user home directory: %w", err)
		}
		localStoragePath = filepath.Join(homeDir, localStoragePath[1:])
	}

	cm := &ChainManager{
		byHeight:         make([]chainhash.Hash, 0, 1000000),
		byHash:           make(map[chainhash.Hash]*BlockHeader),
		network:          network,
		localStoragePath: localStoragePath,
		p2pClient:        p2pClient,
	}

	log.Printf("ChainManager initializing: network=%s, path=%s", network, localStoragePath)

	// Auto-restore from local files (checks storage path first, then embedded checkpoints)
	if err := cm.loadFromLocalFiles(ctx); err != nil {
		return nil, fmt.Errorf("failed to load checkpoint files: %w", err)
	}

	// Run bootstrap sync if configured (optional parameter)
	if len(bootstrapURL) > 0 && bootstrapURL[0] != "" {
		cm.runBootstrapSync(ctx, bootstrapURL[0])
	}

	return cm, nil
}

// runBootstrapSync performs initial sync from a bootstrap node
func (cm *ChainManager) runBootstrapSync(ctx context.Context, url string) {
	log.Printf("Bootstrap URL configured: %s", url)

	// Get the latest block hash from the bootstrap node
	remoteTipHash, err := FetchLatestBlock(ctx, url)
	if err != nil {
		log.Printf("Failed to get bootstrap node tip: %v (will continue with P2P sync)", err)
		return
	}

	log.Printf("Bootstrap node tip: %s", remoteTipHash.String())
	if err := cm.SyncFromRemoteTip(ctx, remoteTipHash, url); err != nil {
		log.Printf("Bootstrap sync failed: %v (will continue with P2P sync)", err)
		return
	}

	// Log updated chain state after bootstrap
	if tip := cm.GetTip(ctx); tip != nil {
		log.Printf("Chain tip after bootstrap: %s at height %d", tip.Header.Hash().String(), tip.Height)
	}
}

// GetHeaderByHeight retrieves a header by height
func (cm *ChainManager) GetHeaderByHeight(_ context.Context, height uint32) (*BlockHeader, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if len(cm.byHeight) > 0xFFFFFFFF {
		return nil, ErrIntegerOverflow
	}
	if height >= uint32(len(cm.byHeight)) { //nolint:gosec // Overflow check performed above
		return nil, ErrHeaderNotFound
	}

	hash := cm.byHeight[height]
	header, ok := cm.byHash[hash]
	if !ok {
		return nil, ErrHeaderNotFound
	}

	return header, nil
}

// GetHeaderByHash retrieves a header by hash
func (cm *ChainManager) GetHeaderByHash(_ context.Context, hash *chainhash.Hash) (*BlockHeader, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	header, ok := cm.byHash[*hash]
	if !ok {
		return nil, ErrHeaderNotFound
	}

	return header, nil
}

// GetTip returns the current chain tip
func (cm *ChainManager) GetTip(_ context.Context) *BlockHeader {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.tip
}

// GetHeight returns the current chain height
func (cm *ChainManager) GetHeight(_ context.Context) uint32 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.tip == nil {
		return 0
	}
	return cm.tip.Height
}

// AddHeader adds a header to byHash for lookups without modifying the chain tip
func (cm *ChainManager) AddHeader(header *BlockHeader) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.byHash[header.Hash] = header

	return nil
}

// GetNetwork returns the network name
func (cm *ChainManager) GetNetwork(_ context.Context) (string, error) {
	return cm.network, nil
}

// pruneOrphans removes old orphaned headers (must be called with lock held)
func (cm *ChainManager) pruneOrphans() {
	if cm.tip == nil {
		return
	}

	pruneHeight := uint32(0)
	if cm.tip.Height > 100 {
		pruneHeight = cm.tip.Height - 100
	}

	// Remove headers that are not in byHeight (orphans) and too old
	for hash, header := range cm.byHash {
		// Check if it's in the main chain
		chainLen := len(cm.byHeight)
		if chainLen <= 0xFFFFFFFF && header.Height < uint32(chainLen) && cm.byHeight[header.Height] == hash {
			continue
		}
		// It's an orphan, check if too old
		if header.Height < pruneHeight {
			delete(cm.byHash, hash)
		}
	}
}
