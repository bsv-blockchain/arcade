# Agents Guide

## Build & Test Commands
- **Build:** `go build ./cmd/arcade`
- **Test all:** `go test ./...`
- **Single test:** `go test -run TestName ./path/to/package`
- **Generate docs:** `swag init -g cmd/arcade/main.go -o docs --parseDependency --parseDependencyLevel 3`

## Code Style
- **Imports:** Group in order: stdlib, third-party, local (`github.com/bsv-blockchain/arcade/...`), separated by blank lines
- **Naming:** PascalCase for exported, camelCase for private; error vars use `Err` prefix (e.g., `ErrTxOutputInvalid`)
- **Error handling:** Wrap with `fmt.Errorf("context: %w", err)`, use `errors.Join` for combining, define sentinel errors as package vars
- **Logging:** Use `slog` with structured fields: `logger.Error("msg", slog.String("key", val))`
- **Struct tags:** JSON uses camelCase (`json:"txid"`), mapstructure uses snake_case (`mapstructure:"storage_path"`)
- **Interfaces:** Define where consumed, use compile-time checks: `var _ Interface = (*Impl)(nil)`

## Testing Patterns
- Standard `testing` package with `t.Helper()` for helpers
- Use `t.Fatalf` for fatal errors, `t.Errorf` for non-fatal assertions
- Table-driven tests for multiple cases
