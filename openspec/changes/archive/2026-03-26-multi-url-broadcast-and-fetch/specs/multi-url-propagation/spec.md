## ADDED Requirements

### Requirement: Broadcast single transaction to all endpoints
When submitting a single transaction, the system SHALL send the transaction to ALL configured DataHub endpoints concurrently, not just until the first success.

#### Scenario: All endpoints receive the transaction
- **WHEN** 3 DataHub URLs are configured and a single transaction is submitted
- **THEN** the system SHALL attempt submission to all 3 endpoints concurrently

#### Scenario: One endpoint accepts, others reject
- **WHEN** endpoint A returns ACCEPTED and endpoints B and C return REJECTED
- **THEN** the system SHALL return ACCEPTED_BY_NETWORK status to the caller

#### Scenario: All endpoints reject
- **WHEN** all configured endpoints reject the transaction
- **THEN** the system SHALL return REJECTED status to the caller

#### Scenario: Timeout before all endpoints respond
- **WHEN** the 15-second timeout fires before all endpoints have responded
- **THEN** the system SHALL return the best status received so far (ACCEPTED > SENT > REJECTED > timeout fallback)

### Requirement: Broadcast batch transactions to all endpoints
When submitting a batch of transactions, the system SHALL send the batch to ALL configured DataHub endpoints concurrently.

#### Scenario: Batch sent to all endpoints
- **WHEN** 3 DataHub URLs are configured and a batch of transactions is submitted
- **THEN** the system SHALL submit the batch to all 3 endpoints concurrently

#### Scenario: One endpoint succeeds for batch
- **WHEN** endpoint A accepts the batch and endpoint B rejects it
- **THEN** the system SHALL treat the batch as successfully broadcast (status ACCEPTED_BY_NETWORK)

### Requirement: Coinbase merkle proof fetched from first successful endpoint
When fetching the coinbase merkle proof for BUMP construction, the system SHALL try each DataHub URL in sequence and stop at the first successful response.

#### Scenario: First URL succeeds
- **WHEN** the first DataHub URL returns a valid coinbase merkle proof
- **THEN** the system SHALL use that proof and NOT query remaining URLs

#### Scenario: First URL fails, second succeeds
- **WHEN** the first DataHub URL fails and the second returns a valid proof
- **THEN** the system SHALL use the second URL's proof

#### Scenario: All URLs fail
- **WHEN** all DataHub URLs fail to return a coinbase merkle proof
- **THEN** the system SHALL return an error
