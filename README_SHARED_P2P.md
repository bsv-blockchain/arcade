# Shared P2P Connection Architecture

This document describes how ChainTracks and Arcade can share a single P2P connection to reduce resource usage and network overhead.

## Problem

Previously, both ChainTracks and Arcade would create their own independent P2P connections to the Bitcoin network, resulting in:
- Duplicate network connections
- Increased resource consumption
- Potential network congestion

## Solution

We've implemented a shared P2P connection pattern that allows multiple services to use the same underlying P2P client.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Shared P2P Manager                                       │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                        P2P Client                                   │  │
│  │                                                                     │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │  │  Consumer   │  │  Consumer   │  │  Consumer   │  │  Consumer   │  │
│  │  │   (Chain)   │  │   (Arcade)  │  │   (Other)   │  │   (Other)   │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Implementation Details

### 1. SharedP2PManager

The `SharedP2PManager` is responsible for:
- Creating and managing a single P2P client
- Handling multiple consumers that want to subscribe to different topics
- Managing the lifecycle of the shared connection
- Forwarding messages to appropriate consumers

### 2. Consumer Registration

Each service (ChainTracks, Arcade, etc.) registers with the manager using a unique consumer ID:
- ChainTracks registers with ID "chaintracks"
- Arcade registers with ID "arcade"

### 3. Topic Subscription

Each consumer can subscribe to specific topics:
- ChainTracks: `teranode/bitcoin/1.0.0/mainnet-block`
- Arcade: `teranode/bitcoin/1.0.0/mainnet-subtree`, `teranode/bitcoin/1.0.0/mainnet-rejected-tx`

## Usage Example

```go
// Create shared P2P manager
p2pConfig := &p2p.Config{
    ProcessName:    "shared-node",
    BootstrapPeers: []string{"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"},
    TopicPrefix:   "mainnet",
}

sharedP2P, err := p2p.NewSharedP2PManager(p2pConfig, logger)
if err != nil {
    // handle error
}

// Start shared connection
err = sharedP2P.Start(ctx)
if err != nil {
    // handle error
}

// Initialize ChainTracks with shared connection
chaintracksManager, err := chaintracks.NewChainManager(ctx, "mainnet", "./chaintracks_data", sharedP2P.GetClient())

// Initialize Arcade with shared connection  
arcadeSubscriber, err := p2p.NewSubscriber(
    ctx,
    p2pConfig,
    statusStore,
    networkStore,
    eventPublisher,
    logger,
    sharedP2P.GetClient(), // Pass shared client
)
```

## Benefits

1. **Reduced Network Overhead** - Single connection to the network instead of multiple
2. **Lower Resource Usage** - Less memory and CPU consumption
3. **Better Network Utilization** - More efficient use of bandwidth
4. **Simplified Management** - Single point of connection management
5. **Improved Reliability** - Shared connection health monitoring

## Future Enhancements

1. **Dynamic Topic Management** - Allow services to dynamically add/remove topic subscriptions
2. **Connection Health Monitoring** - Monitor and report on shared connection health
3. **Load Balancing** - Distribute message processing across multiple consumers
4. **Graceful Degradation** - Handle partial failures in shared connection gracefully

## Migration Notes

Existing applications that create their own P2P connections will continue to work, but for optimal performance, we recommend migrating to use the shared connection pattern.