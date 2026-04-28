## ADDED Requirements

### Requirement: In-memory transaction hash tracker
The system SHALL provide a `TxTracker` that maintains an in-memory concurrent map of tracked transaction hashes (as `chainhash.Hash`) with their current status, supporting O(1) lookup.

#### Scenario: Add and lookup tracked transaction
- **WHEN** `tracker.Add(txid, status)` is called with a valid TXID
- **THEN** `tracker.IsTracked(hash)` SHALL return true for the corresponding chainhash.Hash

#### Scenario: Filter tracked hashes from STUMP level-0
- **WHEN** `tracker.FilterTrackedHashes(hashes)` is called with a slice of chainhash.Hash values where 3 of 100 are tracked
- **THEN** it SHALL return exactly those 3 tracked hashes

### Requirement: Status updates in tracker
The system SHALL support updating the status of tracked transactions via `UpdateStatusHash(hash, newStatus)`.

#### Scenario: Update status of tracked transaction
- **WHEN** `tracker.UpdateStatusHash(hash, StatusMined)` is called for a tracked hash
- **THEN** the tracker's internal state for that hash SHALL reflect the new status

### Requirement: Thread-safe operations
All TxTracker operations SHALL be safe for concurrent access from multiple goroutines.

#### Scenario: Concurrent add and filter
- **WHEN** multiple goroutines simultaneously call Add and FilterTrackedHashes
- **THEN** no data races SHALL occur and all operations SHALL return consistent results
