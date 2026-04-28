## MODIFIED Requirements

### Requirement: Validator batches propagation publishes
The tx-validator SHALL accumulate validated propagation messages and publish them in a batch using `SendBatch` instead of publishing each one individually via `Send`. Messages that fail validation are excluded from the batch (they are handled individually with status updates).

#### Scenario: 100 transactions validated and batch-published
- **WHEN** 100 transaction messages are consumed from the Kafka topic and all pass validation
- **THEN** 100 propagation messages are published to `arcade.propagation` in a single `SendBatch` call
- **THEN** each propagation message contains the txid and raw_tx

#### Scenario: Mixed validation results in batch
- **WHEN** 100 transaction messages are consumed and 95 pass validation while 5 fail
- **THEN** the 5 failed transactions have their status updated to REJECTED individually
- **THEN** the 95 valid transactions are published to `arcade.propagation` in a single `SendBatch` call

#### Scenario: Single transaction still works
- **WHEN** 1 transaction message is consumed and passes validation
- **THEN** 1 propagation message is published (batch of 1) via `SendBatch`
- **THEN** behavior is functionally identical to the pre-batch flow
