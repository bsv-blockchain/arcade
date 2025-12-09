package p2p

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/libp2p/go-libp2p/core/crypto"
)

//go:embed bootstrap_peers.json
var bootstrapPeersJSON []byte

// bootstrapPeers holds the parsed bootstrap peers by network
var bootstrapPeers map[string][]string

func init() {
	bootstrapPeers = make(map[string][]string)
	if err := json.Unmarshal(bootstrapPeersJSON, &bootstrapPeers); err != nil {
		panic(fmt.Sprintf("failed to parse embedded bootstrap_peers.json: %v", err))
	}
}

// BootstrapPeers returns the bootstrap peer multiaddrs for the given network.
// Valid networks: "main", "test", "stn"
// Returns nil if the network is not found.
func BootstrapPeers(network string) []string {
	return bootstrapPeers[network]
}

// LoadOrGeneratePrivateKey loads a P2P private key from the given storage path,
// or generates a new one if it doesn't exist. The key is stored as a hex string
// in a file named "p2p_key.hex" within the storage directory.
//
// This ensures consistent peer identity across restarts.
func LoadOrGeneratePrivateKey(storagePath string) (crypto.PrivKey, error) {
	keyPath := filepath.Join(storagePath, "p2p_key.hex")

	// Try to load existing key
	if data, err := os.ReadFile(keyPath); err == nil {
		privKey, err := msgbus.PrivateKeyFromHex(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key from %s: %w", keyPath, err)
		}
		return privKey, nil
	}

	// Generate new key
	privKey, err := msgbus.GeneratePrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	// Save to file
	keyHex, err := msgbus.PrivateKeyToHex(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encode private key: %w", err)
	}

	if err := os.MkdirAll(storagePath, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	if err := os.WriteFile(keyPath, []byte(keyHex), 0o600); err != nil {
		return nil, fmt.Errorf("failed to write key file: %w", err)
	}

	return privKey, nil
}

// GeneratePrivateKey generates a new Ed25519 private key for P2P identity.
func GeneratePrivateKey() (crypto.PrivKey, error) {
	return msgbus.GeneratePrivateKey()
}

// PrivateKeyToHex converts a private key to its hex-encoded string representation.
func PrivateKeyToHex(key crypto.PrivKey) (string, error) {
	return msgbus.PrivateKeyToHex(key)
}

// PrivateKeyFromHex parses a hex-encoded private key string.
func PrivateKeyFromHex(keyHex string) (crypto.PrivKey, error) {
	return msgbus.PrivateKeyFromHex(keyHex)
}
