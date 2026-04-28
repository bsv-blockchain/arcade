## MODIFIED Requirements

### Requirement: Transaction propagation handles batches
The propagation service SHALL support processing multiple transactions as a batch. Instead of processing each Kafka message independently (parse → register → broadcast → update), the propagator SHALL accumulate messages into micro-batches and process them together: register all txids with merkle service concurrently, broadcast all raw transactions to each teranode endpoint in a single HTTP call, then update all statuses.

#### Scenario: Batch of 100 transactions propagated
- **WHEN** 100 propagation messages arrive on the Kafka topic within the flush interval
- **THEN** all 100 txids are registered with merkle service concurrently (bounded parallelism)
- **THEN** all 100 raw transactions are broadcast to each teranode endpoint in one `POST /txs` call
- **THEN** `UpdateStatus` is called for each of the 100 transactions

#### Scenario: Merkle registration failure aborts batch broadcast
- **WHEN** a batch of 100 transactions is being processed and merkle registration fails for any txid
- **THEN** no transactions in the batch are broadcast to teranode
- **THEN** the batch processing returns an error to trigger Kafka retry for all messages

#### Scenario: Single transaction still works
- **WHEN** a single propagation message arrives and no others arrive within the flush interval
- **THEN** the single transaction is registered with merkle service and broadcast normally
- **THEN** behavior is identical to the pre-batch single-message flow

#### Scenario: Nil merkle client skips registration for batch
- **WHEN** merkle client is nil and a batch of transactions is processed
- **THEN** merkle registration is skipped entirely
- **THEN** all transactions are broadcast to teranode
