## MODIFIED Requirements

### Requirement: Batch endpoint uses batch Kafka publish
The `POST /txs` handler SHALL parse all transactions from the concatenated body first, then publish all messages to the `arcade.transaction` Kafka topic in a single batch call using `SendBatch`. The handler SHALL NOT publish messages one at a time in a loop.

#### Scenario: 100 transactions submitted in batch
- **WHEN** a client sends `POST /txs` with 100 concatenated raw transactions
- **THEN** all 100 transactions are parsed from the body
- **THEN** all 100 messages are published to Kafka in a single `SendBatch` call
- **THEN** the response is `200 OK` with `{"submitted": 100}`

#### Scenario: Parse failure mid-batch
- **WHEN** a client sends `POST /txs` and transaction 50 fails to parse
- **THEN** no messages are published to Kafka (parse-all-first semantics)
- **THEN** the response is `400 Bad Request` with the parse error and parsed count

#### Scenario: Kafka batch publish failure
- **WHEN** all 100 transactions parse successfully but `SendBatch` returns an error
- **THEN** the response is `500 Internal Server Error`
- **THEN** no partial delivery — either all are published or none
