package p2p

import "fmt"

// Protocol prefix for Teranode P2P topics
const protocolPrefix = "teranode/bitcoin/1.0.0"

// Topic type constants
const (
	TopicBlock      = "block"
	TopicSubtree    = "subtree"
	TopicRejectedTx = "rejected-tx"
	TopicNodeStatus = "node_status"
)

// Network constants for use with TopicName
const (
	NetworkMainnet     = "mainnet"
	NetworkTestnet     = "testnet"
	NetworkSTN         = "stn"
	NetworkTeratestnet = "teratestnet"
)

// TopicName constructs a full topic name for subscribing to Teranode P2P messages.
// Example: TopicName("mainnet", TopicBlock) returns "teranode/bitcoin/1.0.0/mainnet-block"
func TopicName(network, topic string) string {
	return fmt.Sprintf("%s/%s-%s", protocolPrefix, network, topic)
}

// AllTopics returns all topic names for a given network.
func AllTopics(network string) []string {
	return []string{
		TopicName(network, TopicBlock),
		TopicName(network, TopicSubtree),
		TopicName(network, TopicRejectedTx),
		TopicName(network, TopicNodeStatus),
	}
}
