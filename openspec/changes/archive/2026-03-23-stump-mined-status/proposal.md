## Why

When a STUMP callback is received from Merkle Service, the transaction's status remains unchanged until the full BUMP construction completes (triggered by BLOCK_PROCESSED). This means there's a delay between knowing a transaction is mined (STUMP received) and reflecting that in the status. Updating the status to MINED immediately upon receiving a STUMP provides faster feedback to clients.

## What Changes

- When a STUMP callback is received for a transaction, update its status to `MINED` immediately
- The existing BUMP construction flow (triggered by BLOCK_PROCESSED) continues to work as before for merkle path storage
- Status events are published so downstream subscribers (webhooks, SSE) are notified of the MINED status when the STUMP arrives

## Capabilities

### New Capabilities
- `stump-mined-status`: Update transaction status to MINED when a STUMP callback is received, before full BUMP construction

### Modified Capabilities

## Impact

- `routes/fiber/callback.go`: The `handleStump` method needs to update transaction status and publish events
- `store` layer: Uses existing `UpdateStatus` method - no store changes needed
- Event subscribers receive MINED status earlier (at STUMP time rather than BLOCK_PROCESSED time)
- The `SetMinedByBlockHash` call in `ConstructBUMPsForBlock` becomes a no-op for transactions already marked MINED, which is safe due to existing status transition guards
