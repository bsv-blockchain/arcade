// Package logging provides log level utilities for slog.
package logging

import (
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds logging configuration
type Config struct {
	// Level is the default log level
	Level string `mapstructure:"level"`
}

// SetDefaults sets default logging configuration
func (c *Config) SetDefaults(v *viper.Viper, prefix string) {
	p := ""
	if prefix != "" {
		p = prefix + "."
	}
	v.SetDefault(p+"level", "info")
}

// ParseLevel converts a string to slog.Level
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogger creates a new text logger with the specified level.
func NewLogger(level string) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: ParseLevel(level),
	}))
}
