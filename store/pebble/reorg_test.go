package pebble

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/internal/synthblock"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Store-layer reorg semantics for issue #279: IMMUTABLE rows are never
// touched by block-scoped rewrites, superseded MINED anchors are preserved
// in the row's orphaned-anchor history (capped, dup-collapsed), and the
// history's proofs are re-derived at read time from each orphaned block's
// retained compound BUMP.

// TestSetMinedByTxIDs_SkipsImmutable pins the lattice hardening: a replayed
// BLOCK_PROCESSED must never demote a confirmation-depth promotion, so an
// IMMUTABLE row keeps its status AND its original anchor, and does not
// appear in the returned slices.
func TestSetMinedByTxIDs_SkipsImmutable(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "imm-tx"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusImmutable,
		BlockHash: "blk-A", BlockHeight: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	prevs, mined, err := s.SetMinedByTxIDs(ctx, "blk-B", 10, []string{txid})
	if err != nil {
		t.Fatalf("SetMinedByTxIDs: %v", err)
	}
	if len(prevs) != 0 || len(mined) != 0 {
		t.Fatalf("IMMUTABLE row must not be returned: prevs=%d mined=%d", len(prevs), len(mined))
	}

	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.StatusImmutable {
		t.Errorf("status = %s, want IMMUTABLE", got.Status)
	}
	if got.BlockHash != "blk-A" || got.BlockHeight != 10 {
		t.Errorf("anchor moved: hash=%q height=%d, want blk-A/10", got.BlockHash, got.BlockHeight)
	}
	if len(got.OrphanedProofs) != 0 {
		t.Errorf("IMMUTABLE row must not gain history: %+v", got.OrphanedProofs)
	}
}

// TestSetMinedByTxIDs_ReanchorAppendsOrphanedAnchor covers the same-height
// re-anchor path: MINED against a DIFFERENT block preserves the superseded
// anchor, idempotent same-block replays leave history untouched, and a
// first-time MINED appends nothing.
func TestSetMinedByTxIDs_ReanchorAppendsOrphanedAnchor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "reanchor-tx"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusMined,
		BlockHash: "blk-A", BlockHeight: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Re-anchor A → B at the same height.
	before := time.Now()
	if _, mined, err := s.SetMinedByTxIDs(ctx, "blk-B", 10, []string{txid}); err != nil {
		t.Fatalf("re-anchor: %v", err)
	} else if len(mined) != 1 {
		t.Fatalf("re-anchor returned %d rows, want 1", len(mined))
	}

	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.StatusMined || got.BlockHash != "blk-B" || got.BlockHeight != 10 {
		t.Fatalf("row after re-anchor: status=%s hash=%q height=%d, want MINED/blk-B/10",
			got.Status, got.BlockHash, got.BlockHeight)
	}
	if len(got.OrphanedProofs) != 1 {
		t.Fatalf("OrphanedProofs = %+v, want exactly 1 entry", got.OrphanedProofs)
	}
	entry := got.OrphanedProofs[0]
	if entry.BlockHash != "blk-A" || entry.BlockHeight != 10 {
		t.Errorf("orphaned anchor = {%s %d}, want {blk-A 10}", entry.BlockHash, entry.BlockHeight)
	}
	if entry.OrphanedAt.Before(before.Add(-time.Minute)) || entry.OrphanedAt.After(time.Now().Add(time.Minute)) {
		t.Errorf("OrphanedAt = %v, want ~now", entry.OrphanedAt)
	}

	// Idempotent replay against the SAME block must not grow history.
	if _, _, err := s.SetMinedByTxIDs(ctx, "blk-B", 10, []string{txid}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	got, _ = s.GetStatus(ctx, txid)
	if len(got.OrphanedProofs) != 1 {
		t.Fatalf("history grew on idempotent replay: %+v", got.OrphanedProofs)
	}

	// Re-anchor back to A: the current anchor (B) joins the history.
	if _, _, err := s.SetMinedByTxIDs(ctx, "blk-A", 10, []string{txid}); err != nil {
		t.Fatalf("re-anchor back: %v", err)
	}
	got, _ = s.GetStatus(ctx, txid)
	if got.BlockHash != "blk-A" {
		t.Errorf("hash after re-anchor back = %q, want blk-A", got.BlockHash)
	}
	if len(got.OrphanedProofs) != 2 ||
		got.OrphanedProofs[0].BlockHash != "blk-A" || got.OrphanedProofs[1].BlockHash != "blk-B" {
		t.Fatalf("history after flip back = %+v, want [blk-A blk-B]", got.OrphanedProofs)
	}

	// First-time MINED (from SEEN_ON_NETWORK) appends nothing.
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: "fresh-tx", Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SetMinedByTxIDs(ctx, "blk-B", 10, []string{"fresh-tx"}); err != nil {
		t.Fatal(err)
	}
	fresh, _ := s.GetStatus(ctx, "fresh-tx")
	if len(fresh.OrphanedProofs) != 0 {
		t.Errorf("first-time MINED must not append history: %+v", fresh.OrphanedProofs)
	}
}

// TestSetMinedByTxIDs_HistoryCap drives 7 alternating re-anchors and asserts
// the history caps at models.MaxOrphanedAnchors, keeping the newest entries.
func TestSetMinedByTxIDs_HistoryCap(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "cap-tx"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusMined,
		BlockHash: "cap-0", BlockHeight: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Alternate cap-A / cap-B so every re-anchor supersedes a different
	// block and no consecutive-dup collapse kicks in.
	var last string
	for i := 0; i < 7; i++ {
		next := "cap-A"
		if i%2 == 1 {
			next = "cap-B"
		}
		if _, _, err := s.SetMinedByTxIDs(ctx, next, 10, []string{txid}); err != nil {
			t.Fatalf("re-anchor %d → %s: %v", i, next, err)
		}
		last = next
	}

	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.BlockHash != last {
		t.Errorf("current anchor = %q, want %q", got.BlockHash, last)
	}
	if len(got.OrphanedProofs) != models.MaxOrphanedAnchors {
		t.Fatalf("history len = %d, want cap %d: %+v",
			len(got.OrphanedProofs), models.MaxOrphanedAnchors, got.OrphanedProofs)
	}
	// The oldest entries (the original cap-0 anchor) were evicted.
	for _, e := range got.OrphanedProofs {
		if e.BlockHash == "cap-0" {
			t.Errorf("oldest anchor cap-0 survived the cap: %+v", got.OrphanedProofs)
		}
	}
	// Newest entry is the anchor superseded by the last re-anchor: with the
	// A/B alternation that is the opposite block of the current anchor.
	newest := got.OrphanedProofs[len(got.OrphanedProofs)-1].BlockHash
	wantNewest := "cap-A"
	if last == "cap-A" {
		wantNewest = "cap-B"
	}
	if newest != wantNewest {
		t.Errorf("newest history entry = %q, want %q", newest, wantNewest)
	}
}

// TestSetStatusByBlockHash_RevertAppendsHistoryAndSkipsImmutable pins the
// reorg-revert path: MINED rows in the orphaned block are returned and
// reverted with block fields cleared plus the cleared anchor appended to
// history; IMMUTABLE rows are untouched and NOT returned.
func TestSetStatusByBlockHash_RevertAppendsHistoryAndSkipsImmutable(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for _, txid := range []string{"rv-1", "rv-2"} {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusMined,
			BlockHash: "blk-A", BlockHeight: 10, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", txid, err)
		}
	}
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: "rv-imm", Status: models.StatusImmutable,
		BlockHash: "blk-A", BlockHeight: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("seed rv-imm: %v", err)
	}

	updated, err := s.SetStatusByBlockHash(ctx, "blk-A", models.StatusSeenOnNetwork)
	if err != nil {
		t.Fatalf("SetStatusByBlockHash: %v", err)
	}
	sort.Strings(updated)
	if len(updated) != 2 || updated[0] != "rv-1" || updated[1] != "rv-2" {
		t.Fatalf("updated = %v, want [rv-1 rv-2]", updated)
	}

	for _, txid := range []string{"rv-1", "rv-2"} {
		got, err := s.GetStatus(ctx, txid)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != models.StatusSeenOnNetwork {
			t.Errorf("%s: status = %s, want SEEN_ON_NETWORK", txid, got.Status)
		}
		if got.BlockHash != "" || got.BlockHeight != 0 {
			t.Errorf("%s: block fields not cleared: hash=%q height=%d", txid, got.BlockHash, got.BlockHeight)
		}
		if len(got.OrphanedProofs) != 1 ||
			got.OrphanedProofs[0].BlockHash != "blk-A" || got.OrphanedProofs[0].BlockHeight != 10 {
			t.Errorf("%s: OrphanedProofs = %+v, want [{blk-A 10}]", txid, got.OrphanedProofs)
		}
	}

	imm, err := s.GetStatus(ctx, "rv-imm")
	if err != nil {
		t.Fatal(err)
	}
	if imm.Status != models.StatusImmutable || imm.BlockHash != "blk-A" || imm.BlockHeight != 10 {
		t.Errorf("IMMUTABLE row touched: status=%s hash=%q height=%d", imm.Status, imm.BlockHash, imm.BlockHeight)
	}
	if len(imm.OrphanedProofs) != 0 {
		t.Errorf("IMMUTABLE row gained history: %+v", imm.OrphanedProofs)
	}
}

// TestSetStatusByBlockHash_SkipsConcurrentlyReanchoredRow exercises the
// stale-index/hash-recheck guard: a row that moved to a different anchor
// between the index snapshot and the write must not be reverted. The stale
// index entry is injected by hand to simulate the race deterministically.
func TestSetStatusByBlockHash_SkipsConcurrentlyReanchoredRow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "cr-1"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusMined,
		BlockHash: "blk-A", BlockHeight: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Concurrent re-anchor to the canonical block.
	if _, _, err := s.SetMinedByTxIDs(ctx, "blk-B", 10, []string{txid}); err != nil {
		t.Fatalf("re-anchor: %v", err)
	}
	// Re-inject the now-stale blk-A index entry, as if the revert's index
	// snapshot had been taken before the re-anchor removed it.
	if err := s.db.Set(idxTxBlockKey("blk-A", txid), nil, s.writeOpts); err != nil {
		t.Fatalf("inject stale index: %v", err)
	}

	updated, err := s.SetStatusByBlockHash(ctx, "blk-A", models.StatusSeenOnNetwork)
	if err != nil {
		t.Fatalf("SetStatusByBlockHash: %v", err)
	}
	if len(updated) != 0 {
		t.Fatalf("re-anchored row must be skipped, got %v", updated)
	}

	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.StatusMined || got.BlockHash != "blk-B" {
		t.Errorf("row reverted despite re-anchor: status=%s hash=%q, want MINED/blk-B", got.Status, got.BlockHash)
	}
}

// TestGetTxIDsByBlockHash covers the reorg reconciler's affected-set read:
// only rows CURRENTLY anchored to the block are returned, so a re-anchored
// row leaves the set.
func TestGetTxIDsByBlockHash(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for _, txid := range []string{"ba-1", "ba-2", "ba-3"} {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusMined,
			BlockHash: "blk-A", BlockHeight: 10, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", txid, err)
		}
	}
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: "bb-1", Status: models.StatusMined,
		BlockHash: "blk-B", BlockHeight: 10, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetTxIDsByBlockHash(ctx, "blk-A")
	if err != nil {
		t.Fatalf("GetTxIDsByBlockHash: %v", err)
	}
	sort.Strings(got)
	if len(got) != 3 || got[0] != "ba-1" || got[1] != "ba-2" || got[2] != "ba-3" {
		t.Fatalf("blk-A txids = %v, want [ba-1 ba-2 ba-3]", got)
	}

	// Re-anchor one row to blk-B — it must leave blk-A's set.
	if _, _, err := s.SetMinedByTxIDs(ctx, "blk-B", 10, []string{"ba-2"}); err != nil {
		t.Fatalf("re-anchor: %v", err)
	}
	got, err = s.GetTxIDsByBlockHash(ctx, "blk-A")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != "ba-1" || got[1] != "ba-3" {
		t.Fatalf("blk-A txids after re-anchor = %v, want [ba-1 ba-3]", got)
	}
}

// TestDeleteBUMPByBlockHash: delete removes the stored compound and is
// idempotent — a second delete of the same (now missing) block is a no-op.
func TestDeleteBUMPByBlockHash(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.InsertBUMP(ctx, "blk-del", 42, []byte{0xde, 0xad, 0xbe, 0xef}); err != nil {
		t.Fatalf("InsertBUMP: %v", err)
	}
	if h, data, err := s.GetBUMP(ctx, "blk-del"); err != nil || h != 42 || len(data) != 4 {
		t.Fatalf("GetBUMP before delete: h=%d data=%x err=%v", h, data, err)
	}

	if err := s.DeleteBUMPByBlockHash(ctx, "blk-del"); err != nil {
		t.Fatalf("DeleteBUMPByBlockHash: %v", err)
	}
	if _, _, err := s.GetBUMP(ctx, "blk-del"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetBUMP after delete: err=%v, want ErrNotFound", err)
	}
	if err := s.DeleteBUMPByBlockHash(ctx, "blk-del"); err != nil {
		t.Fatalf("second delete must be a no-op, got %v", err)
	}
}

// TestMarkBlockReconciled_And_ListOrphanedBlocksToReconcile covers the
// reconciler's durable work queue: orphaned-and-unreconciled rows surface
// oldest-first, MarkBlockReconciled removes a row from the queue, and a
// resurrected block clears both orphaned_at and reconciled_at.
func TestMarkBlockReconciled_And_ListOrphanedBlocksToReconcile(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700000500, 0).UTC()

	for i, hash := range []string{"rb-1", "rb-2", "rb-3"} {
		if err := s.UpsertBlockHeaderSeen(ctx, hash, uint64(600+i), t0); err != nil {
			t.Fatalf("seed %s: %v", hash, err)
		}
	}
	// Orphan rb-2 first in wall-clock terms so the list orders by
	// orphaned_at, not by insertion or height.
	if err := s.MarkBlocksOrphaned(ctx, []string{"rb-2"}, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkBlocksOrphaned(ctx, []string{"rb-1"}, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	rows, err := s.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil {
		t.Fatalf("ListOrphanedBlocksToReconcile: %v", err)
	}
	if len(rows) != 2 || rows[0].BlockHash != "rb-2" || rows[1].BlockHash != "rb-1" {
		t.Fatalf("queue = %v, want [rb-2 rb-1] oldest-first", hashesOf(rows))
	}
	for _, r := range rows {
		if r.ReconciledAt != nil {
			t.Errorf("%s: ReconciledAt = %v, want nil while queued", r.BlockHash, r.ReconciledAt)
		}
	}

	// Reconcile rb-2 — only rb-1 remains queued.
	if err := s.MarkBlockReconciled(ctx, "rb-2", t0.Add(3*time.Minute)); err != nil {
		t.Fatalf("MarkBlockReconciled: %v", err)
	}
	rows, err = s.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].BlockHash != "rb-1" {
		t.Fatalf("queue after reconcile = %v, want [rb-1]", hashesOf(rows))
	}
	got, err := s.GetBlockProcessingStatus(ctx, "rb-2")
	if err != nil {
		t.Fatal(err)
	}
	if got.ReconciledAt == nil {
		t.Error("rb-2: ReconciledAt should be set after MarkBlockReconciled")
	}

	// Missing rows are silently skipped.
	if err := s.MarkBlockReconciled(ctx, "never-seen", t0); err != nil {
		t.Fatalf("MarkBlockReconciled on missing row: %v", err)
	}

	// Resurrection: the reconciled block re-joins the main chain with both
	// reorg markers cleared, so a future re-orphaning reconciles again.
	if err := s.UpsertBlockHeaderSeen(ctx, "rb-2", 601, t0.Add(time.Hour)); err != nil {
		t.Fatalf("resurrect: %v", err)
	}
	got, err = s.GetBlockProcessingStatus(ctx, "rb-2")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.BlockStatusActive {
		t.Errorf("resurrected status = %q, want active", got.Status)
	}
	if got.OrphanedAt != nil || got.ReconciledAt != nil {
		t.Errorf("resurrection must clear reorg markers: orphanedAt=%v reconciledAt=%v",
			got.OrphanedAt, got.ReconciledAt)
	}
}

// TestGetStatus_EnrichesOrphanedProofPaths builds a real compound BUMP for
// the orphaned block so GetStatus can re-derive the historical anchor's
// minimal path at read time; the extracted path must parse and compute the
// orphaned block's root. Once the BUMP is deleted the entry is served
// without a path (best-effort contract).
func TestGetStatus_EnrichesOrphanedProofPaths(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const (
		blockA = "000000000000000000000000000000000000000000000000000000000000aaaa"
		blockB = "000000000000000000000000000000000000000000000000000000000000bbbb"
	)
	blk, err := synthblock.Build(4, 900010)
	if err != nil {
		t.Fatalf("synthblock.Build: %v", err)
	}
	txid := blk.Txids[0]

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Mine into blockA (whose compound BUMP is retained), then re-anchor to
	// blockB so blockA lands in the orphaned-anchor history.
	if _, _, err := s.SetMinedByTxIDs(ctx, blockA, 900010, []string{txid}); err != nil {
		t.Fatalf("mine @A: %v", err)
	}
	if err := s.InsertBUMP(ctx, blockA, 900010, blk.BumpBytes); err != nil {
		t.Fatalf("InsertBUMP: %v", err)
	}
	if _, _, err := s.SetMinedByTxIDs(ctx, blockB, 900010, []string{txid}); err != nil {
		t.Fatalf("re-anchor @B: %v", err)
	}

	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.OrphanedProofs) != 1 || got.OrphanedProofs[0].BlockHash != blockA {
		t.Fatalf("OrphanedProofs = %+v, want one entry for blockA", got.OrphanedProofs)
	}
	path := got.OrphanedProofs[0].MerklePath
	if len(path) == 0 {
		t.Fatal("historical anchor's MerklePath not enriched from retained BUMP")
	}
	// The path must parse (NewMerklePathFromBinary) and compute the orphaned
	// block's merkle root for this txid.
	if err := synthblock.VerifyMerklePath(path, txid, blk.Root); err != nil {
		t.Fatalf("orphaned proof does not verify: %v", err)
	}

	// Best-effort: once blockA's BUMP is gone the entry is served without a
	// path rather than failing the read.
	if err := s.DeleteBUMPByBlockHash(ctx, blockA); err != nil {
		t.Fatalf("DeleteBUMPByBlockHash: %v", err)
	}
	got, err = s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.OrphanedProofs) != 1 {
		t.Fatalf("history lost after BUMP delete: %+v", got.OrphanedProofs)
	}
	if len(got.OrphanedProofs[0].MerklePath) != 0 {
		t.Errorf("entry must be served without a path once the BUMP is deleted, got %x",
			got.OrphanedProofs[0].MerklePath)
	}
}
