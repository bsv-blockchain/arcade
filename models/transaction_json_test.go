package models

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestTransactionStatusJSON_OmitsZeroTimes pins the fix for GET /tx returning
// nextRetryAt: "0001-01-01T00:00:00Z" (and merkleRegisteredAt) on rows that
// never scheduled a retry or registered a proof: omitempty does not omit a
// zero struct, omitzero does.
func TestTransactionStatusJSON_OmitsZeroTimes(t *testing.T) {
	raw, err := json.Marshal(&TransactionStatus{TxID: "ab", Status: StatusRejected, Timestamp: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"nextRetryAt", "merkleRegisteredAt", "0001-01-01"} {
		if strings.Contains(string(raw), field) {
			t.Errorf("zero-value field %q serialized: %s", field, raw)
		}
	}

	when := time.Date(2026, 10, 28, 7, 59, 11, 0, time.UTC)
	raw, err = json.Marshal(&TransactionStatus{TxID: "ab", Status: StatusPendingRetry, Timestamp: when, NextRetryAt: when, MerkleRegisteredAt: when})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"nextRetryAt":"2026-10-28T07:59:11Z"`, `"merkleRegisteredAt":"2026-10-28T07:59:11Z"`} {
		if !strings.Contains(string(raw), field) {
			t.Errorf("set field missing: want %s in %s", field, raw)
		}
	}
}
