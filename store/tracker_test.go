package store

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/arcade/models"
)

func TestTxTracker_AddAndContains(t *testing.T) {
	tracker := NewTxTracker()
	tracker.Add("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)

	if !tracker.Contains("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f") {
		t.Error("expected tracker to contain added txid")
	}

	if tracker.Contains("ff01020304050607ff090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f") {
		t.Error("expected tracker not to contain unknown txid")
	}
}

func TestTxTracker_FilterTrackedHashes(t *testing.T) {
	tracker := NewTxTracker()

	hash1, _ := chainhash.NewHashFromHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	hash2, _ := chainhash.NewHashFromHex("ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	hash3, _ := chainhash.NewHashFromHex("aa0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")

	tracker.AddHash(*hash1, models.StatusReceived)
	tracker.AddHash(*hash3, models.StatusSeenOnNetwork)

	input := []chainhash.Hash{*hash1, *hash2, *hash3}
	tracked := tracker.FilterTrackedHashes(input)

	if len(tracked) != 2 {
		t.Fatalf("expected 2 tracked hashes, got %d", len(tracked))
	}
}

func TestTxTracker_UpdateStatus(t *testing.T) {
	tracker := NewTxTracker()
	tracker.Add("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)

	tracker.UpdateStatus("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusMined)

	status, ok := tracker.GetStatus("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	if !ok {
		t.Fatal("expected to find status")
	}
	if status != models.StatusMined {
		t.Errorf("expected MINED, got %s", status)
	}
}

func TestTxTracker_Count(t *testing.T) {
	tracker := NewTxTracker()
	if tracker.Count() != 0 {
		t.Errorf("expected 0, got %d", tracker.Count())
	}

	tracker.Add("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)
	tracker.Add("ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)

	if tracker.Count() != 2 {
		t.Errorf("expected 2, got %d", tracker.Count())
	}
}

func TestTxTracker_SetMinedAndPrune(t *testing.T) {
	tracker := NewTxTracker()
	txid := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	tracker.Add(txid, models.StatusReceived)

	tracker.SetMined(txid, 1000)

	// Not deep enough to prune
	pruned := tracker.PruneConfirmed(1050)
	if len(pruned) != 0 {
		t.Error("should not prune before 100 confirmations")
	}

	// Deep enough to prune
	pruned = tracker.PruneConfirmed(1101)
	if len(pruned) != 1 {
		t.Errorf("expected 1 pruned, got %d", len(pruned))
	}

	if tracker.Contains(txid) {
		t.Error("expected txid removed after pruning")
	}
}
