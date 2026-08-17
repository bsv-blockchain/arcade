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
