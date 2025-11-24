package p2p

import (
	"fmt"
	"log/slog"
)

// SlogAdapter adapts slog.Logger to the P2P library's logger interface.
type SlogAdapter struct {
	logger *slog.Logger
}

// NewSlogAdapter creates a new slog adapter.
func NewSlogAdapter(logger *slog.Logger) *SlogAdapter {
	return &SlogAdapter{logger: logger}
}

func (l *SlogAdapter) Debugf(format string, v ...any) {
	l.logger.Debug(fmt.Sprintf(format, v...))
}

func (l *SlogAdapter) Infof(format string, v ...any) {
	l.logger.Info(fmt.Sprintf(format, v...))
}

func (l *SlogAdapter) Warnf(format string, v ...any) {
	l.logger.Warn(fmt.Sprintf(format, v...))
}

func (l *SlogAdapter) Errorf(format string, v ...any) {
	l.logger.Error(fmt.Sprintf(format, v...))
}
