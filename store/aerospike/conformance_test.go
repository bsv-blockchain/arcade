//go:build integration

package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/arcade/store/storetest"
)

// Requires a live Aerospike on localhost:3200 (integrationStore skips
// otherwise). No CI workflow passes -tags=integration today, so run this by
// hand before merging changes to IterateTrackerRows:
//
//	go test -tags=integration -run TestIterateTrackerRows ./store/aerospike/...
func TestIterateTrackerRows_Conformance(t *testing.T) {
	storetest.RunTrackerRowsSuite(t, func(t *testing.T) storetest.Backend {
		t.Helper()
		return integrationStore(t)
	})
}

// The generation-checked block_processing writes (issue #339): ReactivateBlock,
// MarkBlocksOrphaned's applied-transition count, MarkBlockReconciled's CAS.
// The suite uses its own hash prefix and heights and reads its rows back
// directly, so it is safe in the shared namespace.
func TestBlockStatus_Conformance(t *testing.T) {
	storetest.RunBlockStatusSuite(t, func(t *testing.T) storetest.BlockStatusBackend {
		t.Helper()
		return integrationStore(t)
	})
}

// A double spend is discovered after the row exists, so the competing
// spender must persist through the status-update paths, not only on insert.
func TestCompetingTxs_Conformance(t *testing.T) {
	storetest.RunCompetingTxsSuite(t, func(t *testing.T) storetest.CompetingTxsBackend {
		t.Helper()
		return integrationStore(t)
	})
}

// The last network response must survive across durable retry attempts so
// the reaper's give-up reason can quote it.
func TestRetryReason_Conformance(t *testing.T) {
	storetest.RunRetryReasonSuite(t, func(t *testing.T) storetest.RetryReasonBackend {
		t.Helper()
		return integrationStore(t)
	})
}
