package bump_builder

import (
	"context"

	"go.uber.org/zap"
)

// tryShortCircuit is the pre-multi-source redelivery entry point, kept for
// the reorg-guard tests that drive the short-circuit path directly. It is
// loadStoredBUMP followed by shortCircuit; production goes through
// handleRedelivery, which also compares the retained STUMPs against the
// stored BUMP.
func (b *Builder) tryShortCircuit(ctx context.Context, logger *zap.Logger, blockHash string) (handled bool, mineFailed bool) {
	height, txids, ok, err := b.loadStoredBUMP(ctx, logger, blockHash)
	if err != nil || !ok {
		return false, false
	}
	mineFailed, _ = b.shortCircuit(ctx, logger, blockHash, height, txids)
	return true, mineFailed
}
