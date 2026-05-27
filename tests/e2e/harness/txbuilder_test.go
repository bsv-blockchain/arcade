//go:build e2e

package harness

import (
	"testing"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/validator"
)

// TestBuildValidatableTxs_PassesArcadePolicyValidator confirms our
// synthetic-tx builder produces transactions that survive arcade's
// structural validator. Without this assertion, smoke_test.go's
// BroadcastTx would silently route everything to REJECTED before
// merkle-service ever sees a watch registration.
func TestBuildValidatableTxs_PassesArcadePolicyValidator(t *testing.T) {
	v := validator.NewValidator(nil)
	txs := BuildValidatableTxs(5, 100)

	for i, tx := range txs {
		// arcade parses txs via go-sdk, not go-bt — round-trip via
		// raw bytes so we exercise the same path.
		raw := tx.Bytes()
		sdkParsed, err := sdkTx.NewTransactionFromBytes(raw)
		if err != nil {
			t.Fatalf("tx %d: sdk parse: %v (raw=%x)", i, err, raw)
		}
		if err := v.ValidatePolicy(sdkParsed); err != nil {
			t.Errorf("tx %d: ValidatePolicy: %v (size=%d)", i, err, sdkParsed.Size())
		}
	}
}

// TestBuildValidatableTxs_UniqueTxIDs confirms LockTime variation
// produces distinct txid hashes — the smoke test relies on this for
// per-tx status assertions.
func TestBuildValidatableTxs_UniqueTxIDs(t *testing.T) {
	txs := BuildValidatableTxs(50, 0)
	seen := make(map[string]struct{}, len(txs))
	for i, tx := range txs {
		id := tx.TxID()
		if _, dup := seen[id]; dup {
			t.Fatalf("tx %d: duplicate txid %s", i, id)
		}
		seen[id] = struct{}{}
	}
}
