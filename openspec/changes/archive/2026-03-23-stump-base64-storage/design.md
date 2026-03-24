## Context

The `stumps` table currently stores STUMP data as `BLOB` (`stump_data BLOB NOT NULL`). The SQLite store writes raw `[]byte` directly and reads it back with `rows.Scan`. This works correctly but the binary data is opaque in database viewers and log output.

The `models.Stump.StumpData` field is `[]byte` throughout the codebase — callers pass raw binary and expect raw binary back. The encoding change is purely a storage-layer concern within `store/sqlite/sqlite.go`.

## Goals / Non-Goals

**Goals:**
- Store STUMP data as standard base64 (RFC 4648) TEXT in SQLite
- Keep the `store.Store` interface and `models.Stump` unchanged — encoding is internal to SQLite store
- Migrate existing BLOB data to base64 TEXT

**Non-Goals:**
- Changing how other binary fields are stored (e.g., `merkle_path` remains BLOB)
- Changing the `store.Store` interface signature
- Supporting mixed BLOB/base64 reads (migration converts all existing data)

## Decisions

**Use `encoding/base64.StdEncoding`**: Standard base64 encoding (with padding). This is the most widely supported encoding and works well in TEXT columns, JSON, and logs.

*Alternative considered*: `base64.RawStdEncoding` (no padding). Rejected because padding is conventional and the size overhead is negligible.

**Encode/decode in `InsertStump` and `GetStumpsByBlockHash` only**: These are the only two methods that write/read `stump_data`. The encode happens just before the SQL exec; the decode happens just after the row scan. This keeps the change minimal.

**Add migration 000005**: A new migration converts existing BLOB data in-place. SQLite doesn't support `ALTER COLUMN`, so the migration creates a new table, copies data with base64 encoding, drops the old table, and renames. The inline DDL in `createStumpsTable` also changes from BLOB to TEXT.

## Risks / Trade-offs

[~33% storage increase] → Base64 encoding increases data size by approximately 33%. STUMP data is temporary (deleted after BUMP construction) and small, so this is acceptable.

[Migration on existing data] → The migration must handle the BLOB→base64 conversion. If the database is empty (common — STUMPs are ephemeral), the migration is a no-op effectively. For non-empty databases, the table rebuild is safe within a transaction.
