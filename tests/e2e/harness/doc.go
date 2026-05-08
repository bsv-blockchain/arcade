// Package harness provides reusable fixtures for arcade end-to-end tests.
//
// The high-level entry point is harness.New(t), which boots Postgres +
// Redpanda + merkle-service via testcontainers-go, an in-process libp2p
// host that arcade and merkle-service treat as a bootstrap peer, an
// in-process datahub HTTP server that serves synthetic block/subtree
// binaries, and an in-process arcade instance configured to use all of
// the above.
//
// The harness is reusable across scenarios — smoke tests, reorg tests,
// callback-loss recovery, scale spikes — so each scenario file boots a
// Harness and drives it via its public methods rather than re-wiring
// the plumbing.
//
// All files carry the e2e build tag so this package stays out of the
// default test run.

//go:build e2e

package harness
