### Requirement: STUMP callback updates transaction status to MINED
When a STUMP callback is received from Merkle Service for a tracked transaction, the system SHALL update the transaction's status to MINED and publish a status event.

#### Scenario: Valid STUMP received for tracked transaction
- **WHEN** a STUMP callback is received with a valid txid, blockHash, and stump data
- **THEN** the system SHALL update the transaction status to MINED
- **AND** the system SHALL publish a MINED status event to the event publisher

#### Scenario: STUMP received with missing fields
- **WHEN** a STUMP callback is received with an empty txid, empty blockHash, or empty stump data
- **THEN** the system SHALL NOT attempt to update the transaction status
- **AND** the system SHALL log a warning (existing behavior)

#### Scenario: Status update fails
- **WHEN** a STUMP callback is received but the status update fails (e.g., transaction not in store)
- **THEN** the system SHALL log a warning
- **AND** the system SHALL still attempt to store the STUMP data
