// Package e2e holds the end-to-end smoke test suite for the
// arcade ↔ merkle-service integration.
//
// All files in this tree carry the `e2e` build tag so the suite stays out
// of the default `go test ./...` run. Invoke with:
//
//	go test -tags=e2e -timeout=15m ./tests/e2e/...
//
// The reusable test fixtures live in tests/e2e/harness; smoke_test.go is
// the first consumer.

//go:build e2e

package e2e
