## 1. Schema Changes

- [x] 1.1 Update `createStumpsTable` constant in `store/sqlite/sqlite.go` to use `TEXT` instead of `BLOB` for `stump_data`
- [x] 1.2 Add migration `000005_stumps_base64.up.sql` that rebuilds the stumps table with `TEXT` column and converts existing data to base64
- [x] 1.3 Add migration `000005_stumps_base64.down.sql` to revert back to BLOB

## 2. Encode/Decode Logic

- [x] 2.1 In `InsertStump`, base64-encode `stump.StumpData` before passing to SQL exec
- [x] 2.2 In `GetStumpsByBlockHash`, base64-decode the `stump_data` TEXT value after scanning

## 3. Testing

- [x] 3.1 Update or add test for `InsertStump`/`GetStumpsByBlockHash` round-trip verifying binary data survives base64 encode/decode
- [x] 3.2 Run existing tests to verify no regressions
