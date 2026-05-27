// Package smoke holds fast, in-process integration tests that exercise
// arcade's hot paths without spinning up Docker containers. The default
// `go test ./...` run skips this directory; invoke with:
//
//	go test -tags=smoke -timeout=2m ./tests/smoke/...
//
// Unlike tests/e2e (which boots Postgres + Redpanda + merkle-service in
// containers), this suite stays in-process: memory Kafka, Pebble in a
// temp dir, no merkle-service, no libp2p, no chaintracks. Each test
// stands up a fake teranode (recording_teranode.go) so the assertions
// can inspect what arcade actually sent on the wire.

//go:build smoke

package smoke
