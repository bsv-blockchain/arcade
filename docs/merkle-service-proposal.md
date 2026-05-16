# Merkle Service Proposal

## Overview

The Merkle Service provides on-demand Merkle path generation for any transaction that has been mined on the BSV blockchain. It receives block and subtree announcements from Teranode (via P2P or Kafka), progressively indexes transactions as subtrees are announced (before blocks are mined), and builds Merkle paths on-the-fly when requested via API.

## Goals

- **Progressive Processing**: Index transactions as subtrees are announced, not at block time
- **On-Demand Path Building**: Build Merkle paths when requested, not pre-computed
- **Pluggable Storage**: Support BadgerDB/RocksDB initially, allow migration to distributed stores
- **Pluggable Message Source**: Support Teranode P2P or Kafka
- **Canonical Chain Awareness**: Track only the current canonical chain, clean up orphaned data
- **Scalable**: Handle billions of transactions with bounded storage growth

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    Message Source (Pluggable)                  │
│  ┌──────────────┐  ┌──────────────┐                           │
│  │ P2P Client   │  │ Kafka        │                           │
│  │ (Teranode)   │  │ Consumer     │                           │
│  └──────────────┘  └──────────────┘                           │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│                      Merkle Service                            │
├───────────────────────────────────────────────────────────────┤
│  Ingestion Engine                                              │
│  ├─ Message Parser                                             │
│  ├─ Subtree Fetcher (from Teranode)                           │
│  └─ Indexer                                                    │
│                                                                 │
│  Query Engine                                                  │
│  ├─ Merkle Path Builder                                        │
│  └─ HTTP API (GET /merkle/{txid})                              │
│                                                                 │
│  Maintenance                                                   │
│  ├─ ChainTracks Client                                         │
│  └─ Cleanup Worker                                             │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│                        Storage Layer                           │
│  ┌───────────────────────────────────────────────────────┐     │
│  │ blocks: block_hash → raw P2P msg                       │     │
│  │ subtrees: subtree_hash → tx_hashes                     │     │
│  │ subtree_to_block: (subtree, block) → index            │     │
│  │ tx_index: (txid, subtree) → position                  │     │
│  └───────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────┘
```

### Message Source Interface

The service uses a pluggable interface for receiving block and subtree announcements:

```go
// MessageSource defines the interface for receiving P2P messages
type MessageSource interface {
    // Subscribe to block announcements
    SubscribeBlocks(ctx context.Context) <-chan BlockMessage
    
    // Subscribe to subtree announcements
    SubscribeSubtrees(ctx context.Context) <-chan SubtreeMessage
    
    // Close the source connection
    Close() error
}
```

**Implementations:**
- **P2PSource**: Direct connection to Teranode P2P network using `go-teranode-p2p-client`
- **KafkaSource**: Consumer for Teranode Kafka topics (better scalability, replay capability)

## Data Model

### Block Storage

Stores the raw P2P BlockMessage received from the network:

```
Key: [32 bytes: block_hash]
Value: [raw P2P BlockMessage bytes]
```

The BlockMessage contains:
- Block hash
- Block height
- Subtree count
- Array of subtree hashes (32 bytes each)
- Coinbase transaction (raw bytes)
- DataHub URL for fetching full block data

**Rationale**: We need the coinbase transaction to replace the placeholder in subtree 0, position 0. We also need the complete list of subtree hashes to build the upper layers of the Merkle path.

### Subtree Storage

Stores transaction hashes for each subtree:

```
Key: [32 bytes: subtree_hash]
Value: [raw P2P SubtreeMessage or just tx_hashes array]
```

The subtree data contains:
- Array of transaction hashes (32 bytes each)
- Position 0 in subtree 0 is a placeholder (all zeros)

**Rationale**: The complete list of tx hashes in a subtree is required to build layer 0 of the Merkle path.

### Subtree-to-Block Mapping

Maps subtrees to the blocks containing them (a subtree may be in multiple competing/orphaned blocks):

```
Key: [32 bytes: subtree_hash][32 bytes: block_hash]
Value: [4 bytes: subtree_index]
```

Total: 68 bytes per (subtree, block) entry

**Rationale**:
- A subtree may exist in multiple blocks during reorgs
- Composite key allows lookup of all blocks for a given subtree
- Subtree index stored for efficient Merkle path building (avoids scanning)
- Block height not stored here (available in block record)

### Transaction Index

Maps transaction ID to all subtrees containing it (a transaction may be in multiple competing subtrees before mining):

```
Key: [32 bytes: txid][32 bytes: subtree_hash]
Value: [4 bytes: position_in_subtree]
```

Total: 68 bytes per (txid, subtree) entry

**Rationale**: 
- A transaction may appear in multiple subtrees from different miners before being mined
- Composite key allows efficient lookup of all subtrees for a given txid
- Position is uint32 (max 4 billion transactions per subtree, sufficient)
- Block height not stored (available via block lookup)
- Using subtree reference allows progressive indexing before block is mined

## Processing Flow

### 1. Subtree Announcement

When a `SubtreeMessage` is received (via P2P or Kafka):

1. **Download subtree data**:
   - Try DataHub URL from message
   - If unavailable: Try configured Teranode(s) in order
   - Fetch: Array of transaction hashes for this subtree

2. **Store locally**:
   - Store: `subtree_hash → tx_hashes`
   - For each txid in tx_hashes:
     - Store: `(txid, subtree_hash) → position`

3. **Send subtree callbacks**:
   - Check if any txids in this subtree are being watched (subscriptions)
   - For each matching subscription:
     - Send `subtree_seen` callback with the subtree hash and matching txids

**Note**: Subtrees are announced before blocks are mined. The service downloads and stores all data locally - no runtime dependency on external DataHubs for queries.

### 2. Block Announcement

When a `BlockMessage` is received (via P2P or Kafka):

1. **Download block data** (if not already cached):
   - Try DataHub URL from message
   - If unavailable: Try configured Teranode(s) in order
   - Fetch: Full block binary (header + subtree hashes + coinbase)

2. **Store locally**:
   - Store: `block_hash → raw_block_data`

3. **Link subtrees to block**:
   - For each subtree hash in block's subtree list (with index):
     - Store: `(subtree_hash, block_hash) → subtree_index`

4. **Send block callbacks**:
   - For each subscription with watched txids in this block:
     - Build BUMP structure containing all watched txids
     - Send `block_mined` callback with the BUMP, block hash, and height

**Note**: Block data includes the coinbase transaction needed for subtree 0, and the complete list of subtree hashes for building Merkle paths.

### 3. Merkle Path Request

When `GET /merkle/{txid}` is requested:

1. **Find all subtrees for txid**:
   - Scan `tx_index` with prefix `txid + *`
   - Returns: list of `(subtree_hash, position)` pairs

2. **Find canonical block for each subtree**:
   - For each `subtree_hash`:
     - Scan `subtree_to_block` with prefix `subtree_hash + *`
     - Returns: list of `(block_hash, subtree_index)` pairs
     - Check each `block_hash` via ChainTracks for canonical status
     - Return first match (exactly one canonical, or none during reorgs)

3. **Build and return path**:
   - **If canonical block found**:
     - Fetch block → get height, subtree_hashes array, coinbase_tx
     - Fetch subtree → get tx_hashes array
     - Build Merkle path:
       - Create `transaction.MerklePath` with block height
       - Add all tx_hashes from subtree as leaves at layer 0
       - If subtree index is 0, replace position 0 with actual coinbase txid
       - Add all subtree_hashes at appropriate layer
       - Call `ComputeMissingHashes()`
       - Extract minimal path using `extractMinimalPath()`
     - Return: `200 OK` with binary Merkle path
   
   - **If no canonical block found but subtrees exist**:
     - Return: `202 Accepted` with JSON listing pending subtrees
   
   - **If transaction not found**:
     - Return: `404 Not Found`
   
   - **If transaction only in orphaned blocks** (not on canonical chain):
   - Return: `410 Gone`

## Storage Interface

```go
// Store defines the interface for Merkle Service storage
// Implementations: BadgerDB, RocksDB, etc.
type Store interface {
    // Block operations
    PutBlock(blockHash []byte, blockData []byte) error
    GetBlock(blockHash []byte) ([]byte, error)
    DeleteBlock(blockHash []byte) error
    
    // Subtree operations
    PutSubtree(subtreeHash []byte, txHashes [][]byte) error
    GetSubtree(subtreeHash []byte) ([][]byte, error)
    DeleteSubtree(subtreeHash []byte) error
    
    // Subtree → Block mapping
    // Stores: (subtree_hash, block_hash) -> subtree_index
    LinkSubtreeToBlock(subtreeHash, blockHash []byte, index uint32) error
    
    // Scan all blocks for a given subtree
    // Returns iterator over all (block_hash, subtree_index) entries
    ScanSubtreeBlocks(subtreeHash []byte) (SubtreeBlockIterator, error)
    
    // Delete all subtree_to_block entries for a block (used during cleanup)
    DeleteSubtreeBlockEntries(blockHash []byte) error
    
    // Transaction index
    // Stores: (txid, subtree_hash) -> position
    IndexTransaction(txid, subtreeHash []byte, position uint32) error
    
    // Scan all subtrees for a given txid
    // Returns iterator over all (subtree_hash, position) entries
    ScanTransactions(txid []byte) (TxIndexIterator, error)
    
    // Delete all tx_index entries for a subtree (used during cleanup)
    DeleteTransactionsBySubtree(subtreeHash []byte) error
    
    // Maintenance
    NewIterator(opts IterOptions) Iterator
    Close() error
}

// Iterator for cleanup operations
type Iterator interface {
    First() bool
    Next() bool
    Valid() bool
    Key() []byte
    Value() []byte
    Close() error
}

// TxIndexIterator for scanning transaction entries
type TxIndexIterator interface {
    Next() bool
    SubtreeHash() []byte
    Position() uint32
    Close() error
}

// SubtreeBlockIterator for scanning subtree-to-block mappings
type SubtreeBlockIterator interface {
    Next() bool
    BlockHash() []byte
    SubtreeIndex() uint32
    Close() error
}
```

## Merkle Path Construction

Based on Arcade's implementation, building a Merkle path requires:

### Inputs:
- `txid`: Transaction to build path for
- `subtree_hash`: Hash of the subtree containing txid
- `position`: Index of txid within subtree's tx_hashes
- `subtree_index`: Which subtree within the block (0-indexed)
- `tx_hashes`: All transaction hashes in the subtree (from subtree storage)
- `subtree_hashes`: All subtree root hashes in the block (from block storage)
- `coinbase_tx`: Coinbase transaction bytes (from block storage)
- `block_height`: Block height (from block storage)

### Algorithm:

1. **Create MerklePath structure**:
   ```go
   mp := &transaction.MerklePath{
       BlockHeight: blockHeight,
       Path: make([][]*transaction.PathElement, totalHeight),
   }
   ```

2. **Build layer 0** (transactions in subtree):
   - Add all tx_hashes from subtree
   - If position 0 in subtree 0: replace placeholder with actual coinbase txid
   - If subtree size is odd: add duplicate marker at end

3. **Build upper layers** (subtree roots):
   - Calculate internal height: `ceil(log2(subtree_size))`
   - Calculate subtree root layer: `ceil(log2(num_subtrees))`
   - Add all subtree_hashes at the internal_height layer
   - Skip subtree 0 hash (computed from tx_hashes, not stored)

4. **Compute tree**:
   - Call `mp.ComputeMissingHashes()` to calculate all intermediate hashes

5. **Extract minimal path**:
   - For each level, keep only the leaf at tx_offset and its sibling
   - Walk up: `offset = offset >> 1`

6. **Return** binary encoded minimal path

## Canonical Chain Management

### Chain Tracking

The service connects to ChainTracks to receive:
- Current chain tip updates
- New block headers
- Reorg notifications

### Cleanup Strategy

We cleanup **only orphaned data** (blocks that were reorged out of the canonical chain). Canonical data is retained permanently and grows with the blockchain.

**Reorg Handling**:
1. When ChainTracks detects a reorg, identify orphaned blocks
2. For each orphaned block:
   - Delete block record
   - Delete all subtrees associated with that block
   - Delete all tx_index entries pointing to those subtrees
3. Keep competing subtrees from the 100-block buffer period
4. After 100 confirmations, blocks are considered immutable and permanent

**Important**: 
- Canonical block data is **never deleted**
- Storage grows linearly with the blockchain
- At current BSV rates (~4M tx/day): ~272 MB/day growth
- Annual growth: ~100 GB/year

### Cleanup Process

```go
func (s *Service) handleReorg(ctx context.Context, orphanedBlocks []Block) error {
    for _, block := range orphanedBlocks {
        // Get all subtrees for this orphaned block
        subtrees := s.store.GetBlockSubtrees(block.Hash)
        
        // Delete tx_index entries for each subtree
        for _, subtreeHash := range subtrees {
            s.store.DeleteTransactionsBySubtree(subtreeHash)
            s.store.DeleteSubtree(subtreeHash)
        }
        
        // Delete the orphaned block
        s.store.DeleteBlock(block.Hash)
    }
    
    return nil
}
```

**Note**: During the 100-block buffer period, a transaction may exist in multiple subtrees (from different miners or competing chains). Query logic filters to return only the canonical one.

## API Specification

### GET /merkle/{txid}

Returns the Merkle path for a transaction.

**Request:**
```
GET /merkle/{txid}
```

**Response:**
- `200 OK`: Binary Merkle path (application/octet-stream)
- `202 Accepted`: Transaction found in pending subtree(s), awaiting block confirmation
- `404 Not Found`: Transaction not found
- `410 Gone`: Transaction was in an orphaned block (no longer valid)

**200 OK Response Body (binary):**
Binary encoded `transaction.MerklePath` from go-sdk.

**202 Accepted Response Body (JSON):**
```json
{
  "status": "pending",
  "subtrees": [
    {
      "subtree_hash": "abc123...",
      "position": 123
    }
  ],
  "message": "Transaction found in pending subtree(s), awaiting block confirmation"
}
```

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "tip_height": 1234567,
  "tip_hash": "000000000000000001...",
  "indexed_transactions": 1234567890
}
```

### Subscription API

For services like Arcade that need to watch many transactions and receive push notifications:

```
POST /watch
{
  "txids": ["abc123...", "def456..."],
  "webhook_url": "https://arcade.example.com/merkle-proofs",
  "callback_token": "arcade_instance_1"
}
→ Returns: { sse_endpoint: "/watch/arcade_instance_1/events" }
```

**Behavior:**
- `callback_token` acts as the subscription ID (upsert semantics - multiple calls append txids)
- When watched txids are seen in subtrees: "subtree_seen" callback sent
- When watched txids are mined in a block: "block_mined" callback with BUMP sent
- SSE endpoint provides real-time stream of all callbacks

**Webhook Payloads:**

**1. Subtree Seen:**
```json
{
  "type": "subtree_seen",
  "subtree_hash": "abc123...",
  "txids": ["txid1...", "txid2..."],
  "callback_token": "arcade_instance_1"
}
```
Sent when watched transactions are first seen in a pending subtree (before block mining).

**2. Block Mined:**
```json
{
  "type": "block_mined",
  "bump": "<base64_encoded_bump_structure>",
  "block_hash": "000000...",
  "block_height": 1234567,
  "callback_token": "arcade_instance_1"
}
```
Sent when a block is mined containing watched transactions. The BUMP contains the merkle proofs.

## Storage Growth Analysis

### Data Sizes

- **Transaction index**: 68 bytes per (txid, subtree) entry
  - Key: 64 bytes (txid + subtree_hash)
  - Value: 4 bytes (position)
  
- **Subtree-to-block mapping**: 68 bytes per (subtree, block) entry
  - Key: 64 bytes (subtree_hash + block_hash)
  - Value: 4 bytes (subtree_index)
  
- **Subtree storage**: ~100-500 KB per subtree (depends on transaction count)
  
- **Block storage**: ~4 MB per block (raw P2P message with coinbase)

### With Cleanup (100 block buffer)

**Transaction index** (dominant storage):
- ~4M tx/day × 100 blocks × 68 bytes = ~27 GB
- This is the primary storage cost and grows linearly with transaction volume

**Storage Structure**:
- **Block storage**: Block header messages (small, ~5-10 KB per block)
  - Contains: 80-byte header + subtree hashes array + coinbase transaction
- **Subtree storage**: Transaction hash arrays (variable size based on subtree capacity)
  - Each subtree stores: [32 bytes × number of transactions in subtree]
  - Subtree sizes are powers of 2 (e.g., 65,536 txs = 2 MB of hashes)
- **Transaction index**: 68 bytes per (txid, subtree) entry
  - Composite key: [txid][subtree_hash] → [position]
- **Subtree-to-block mapping**: 68 bytes per (subtree, block) entry
  - Composite key: [subtree_hash][block_hash] → [subtree_index]

**Growth Characteristics**:
- Storage grows linearly with the blockchain size
- We cleanup only orphaned data (reorged blocks), not canonical data
- Storage is proportional to total transaction count × 68 bytes for the index
- Subtree data adds 32 bytes per transaction stored
- Current BSV rates vary widely; blocks can be very small or very large
- Future: As BSV scales to larger blocks, storage requirements will grow accordingly

**Example** (for illustration only, actual sizes vary):
- Transaction index: ~68 bytes per transaction
- Subtree data: ~32 bytes per transaction
- Combined: ~100 bytes per transaction in canonical blocks
- A block with 4 million transactions: ~400 MB of storage
- Annual growth depends on block sizes and frequency

## Implementation Phases

### Phase 1: Core Service
- P2P listener for subtree/block messages
- BadgerDB storage implementation
- HTTP API for Merkle path requests
- Basic indexing pipeline

### Phase 2: Chain Integration
- ChainTracks integration for canonical chain awareness
- Cleanup worker for orphaned data
- Reorg handling

### Phase 3: Optimization
- RocksDB implementation option
- Caching layer for recent queries
- Metrics and monitoring
- Bulk query API

## Dependencies

- `github.com/bsv-blockchain/go-teranode-p2p-client` - P2P network client
- `github.com/bsv-blockchain/go-chaintracks` - Chain tracking
- `github.com/bsv-blockchain/go-sdk` - MerklePath construction
- BadgerDB or RocksDB - Storage engine

## Design Decisions

### Initial Sync

Initial sync must happen from a Teranode to ensure complete data. The service cannot operate with incomplete historical data. On startup:
1. Connect to configured Teranode
2. Backfill from current tip minus buffer (100 blocks)
3. Continue with live P2P/Kafka ingestion

### Data Fetching Fallback

When downloading subtree or block data:
1. Try DataHub URL from the P2P message first
2. If unavailable: Fall back to configured Teranode(s) in order
3. Store locally once downloaded - no retry needed (immutable data)

### Duplicate Subtree Handling

Duplicate subtree announcements are not a concern:
- Subtrees are content-addressed (hash of transaction list)
- Storage operations are idempotent (same key, same value)
- If already processed, store operation is a no-op

## Open Questions

1. **ChainTracks Dependency**: What happens if ChainTracks is temporarily unavailable?

## Success Criteria

- Can build valid Merkle paths for any transaction in current canonical chain
- Subtrees indexed progressively (within seconds of P2P announcement)
- Cleanup removes only orphaned data; canonical data grows with the blockchain
- Query latency under 100ms for cached data, under 1s for cold lookups
- Handles reorgs correctly (no invalid Merkle paths served)

## Appendix: Merkle Path Binary Format

The Merkle path is returned in the binary format defined by `github.com/bsv-blockchain/go-sdk/transaction.MerklePath`:

```
[4 bytes: block_height]
[1 byte: num_levels]
For each level:
    [1 byte: num_elements]
    For each element:
        [8 bytes: offset]
        [1 byte: flags] (bit 0: is_txid, bit 1: is_duplicate)
        [32 bytes: hash] (omitted if is_duplicate)
```

This format can be used directly with go-sdk's `transaction` package for validation.

## Appendix: P2P Message Formats

### BlockMessage (from Teranode)

```
[32 bytes: block_hash]
[4 bytes: height]
[4 bytes: subtree_count]
[subtree_count × 32 bytes: subtree_hashes]
[varint: coinbase_tx_length]
[coinbase_tx_length bytes: coinbase_tx]
[string: DataHubURL]
```

### SubtreeMessage (from Teranode)

```
[32 bytes: subtree_hash]
[string: DataHubURL]
[string: block_hash] (may be empty if not yet mined)
```

Note: Full subtree data (tx_hashes array) is fetched separately from DataHub URL.

## Appendix: References

- Arcade implementation: `/arcade/arcade.go` lines 437-532 (buildMerklePathsForSubtree)
- go-sdk MerklePath: `github.com/bsv-blockchain/go-sdk/transaction`
- Teranode P2P messages: `github.com/bsv-blockchain/teranode/services/p2p`
