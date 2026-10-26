// Package version exposes the BSV Broadcaster build version.
package version

// Version is the BSV Broadcaster build version. It defaults to "dev" for local builds
// and `go run`, and is overridden at release/CI build time via
//
//	-ldflags "-X github.com/bsv-blockchain/broadcaster/version.Version=<git tag>"
//
// (see .github/workflows/build.yml and the Makefile docker-build target).
var Version = "dev"
