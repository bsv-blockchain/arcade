// Package config provides configuration types for arcade.
package config

// Mode specifies which arcade implementation to use.
type Mode string

const (
	// ModeEmbedded uses an in-process embedded implementation.
	ModeEmbedded Mode = "embedded"
	// ModeRemote uses a REST client to connect to a remote Arcade server.
	ModeRemote Mode = "remote"
)
