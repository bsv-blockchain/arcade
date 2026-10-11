// Package version exposes the arcade build version.
package version

// Version is the arcade build version. It defaults to "dev" for local builds
// and `go run`, and is overridden at release/CI build time via
//
//	-ldflags "-X github.com/bsv-blockchain/arcade/version.Version=<git tag>"
//
// The source commit and dirty-tree bit are not duplicated here. They come
// from Go buildinfo (`vcs.revision`, `vcs.modified`); see Read.
var Version = "dev"
