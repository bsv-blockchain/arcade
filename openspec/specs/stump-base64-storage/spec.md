### Requirement: STUMP data stored as base64-encoded TEXT
The SQLite store SHALL encode STUMP data as standard base64 (RFC 4648) before writing to the `stump_data` column, and SHALL decode from base64 when reading.

#### Scenario: Insert and retrieve STUMP data
- **WHEN** a STUMP with binary data is inserted via `InsertStump`
- **AND** the same STUMP is retrieved via `GetStumpsByBlockHash`
- **THEN** the returned `StumpData` MUST be identical to the original binary data

#### Scenario: Stored format is base64 TEXT
- **WHEN** a STUMP is inserted via `InsertStump`
- **THEN** the `stump_data` column in SQLite MUST contain a valid base64-encoded string, not raw binary

### Requirement: Migration converts existing BLOB data to base64
The system SHALL include a migration that converts any existing STUMP BLOB data to base64-encoded TEXT.

#### Scenario: Existing database with BLOB STUMPs
- **WHEN** the migration runs on a database with existing BLOB STUMP data
- **THEN** all `stump_data` values MUST be converted to base64-encoded TEXT
- **AND** the data MUST be decodable back to the original binary content

#### Scenario: Empty database migration
- **WHEN** the migration runs on a database with no STUMP records
- **THEN** the migration MUST complete successfully with the column type changed to TEXT
