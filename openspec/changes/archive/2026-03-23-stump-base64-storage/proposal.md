## Why

STUMP data is currently stored as a `BLOB` in SQLite, making it opaque when inspecting the database directly. Storing it as base64-encoded `TEXT` improves debuggability and makes the data portable across tools that handle text better than binary (e.g., JSON exports, logs, database viewers).

## What Changes

- **BREAKING**: Change the `stump_data` column in the `stumps` table from `BLOB` to `TEXT`, storing base64-encoded data
- Encode STUMP data to base64 before writing to SQLite in `InsertStump`
- Decode STUMP data from base64 after reading from SQLite in `GetStumpsByBlockHash`
- Add a migration to convert existing BLOB data to base64 TEXT
- The `models.Stump.StumpData` field remains `[]byte` — encoding is an internal storage concern

## Capabilities

### New Capabilities
- `stump-base64-storage`: Store STUMP data as base64-encoded TEXT in SQLite instead of raw BLOB

### Modified Capabilities

## Impact

- `store/sqlite/sqlite.go`: `InsertStump` and `GetStumpsByBlockHash` methods need base64 encode/decode
- `store/sqlite/sqlite.go`: `createStumpsTable` DDL changes column type from BLOB to TEXT
- `store/migrations/000004_stumps.up.sql`: Migration needs updating
- New migration needed to convert existing data
- No changes to the `store.Store` interface or `models.Stump` — encoding is internal to the SQLite implementation
