//go:build e2e

package harness

import (
	"context"
	"testing"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/validator"
)

// TestBuildValidatableTxs_RejectedByGoBDKValidator documents that arcade's
// validator now delegates to go-bdk (the same consensus engine as Teranode),
// which performs full script execution rather than the old structural-only
// policy check. The synthetic txs from BuildValidatableTxs (fake prevout,
// dummy push unlocking script, non-extended) are NOT consensus-valid and are
// not in extended format, so go-bdk rejects them.
//
// CONSEQUENCE FOR THE SMOKE HARNESS: smoke_test.go's BroadcastTx must be
// updated to submit real, signed extended-format (EF/BEEF) transactions;
// synthetic txs will now be REJECTED at arcade intake. Tracked as follow-up to
// the go-bdk validation migration.
func TestBuildValidatableTxs_RejectedByGoBDKValidator(t *testing.T) {
	v, err := validator.NewValidator(nil)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	txs := BuildValidatableTxs(5, 100)

	for i, tx := range txs {
		// arcade parses txs via go-sdk, not go-bt — round-trip via
		// raw bytes so we exercise the same path.
		raw := tx.Bytes()
		sdkParsed, parseErr := sdkTx.NewTransactionFromBytes(raw)
		if parseErr != nil {
			t.Fatalf("tx %d: sdk parse: %v (raw=%x)", i, parseErr, raw)
		}
		if vErr := v.ValidateTransaction(context.Background(), sdkParsed, false); vErr == nil {
			t.Errorf("tx %d: expected go-bdk rejection of synthetic tx, got acceptance", i)
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
