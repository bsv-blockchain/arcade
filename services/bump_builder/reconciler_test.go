package bump_builder

import (
	"context"
	"testing"
	"time"

	sdkchainhash "github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/pebble"
)

// Anchor-reconciler tests (issue #279) run against a REAL pebble store so
// the whole re-anchor/revert path — block-hash index maintenance, orphaned-
// anchor history, reconciled_at bookkeeping, crash-resume idempotency — is
// exercised end to end, not mocked.

const (
	recOrphan    = "0d6be57a5be1776686ee44a4c93623940463ec598104734a723d66c43efc5100"
	recCanonical = "7abcd229ab91a477e553deb8534d64a756410b6210af438b5b8c86eb4b5d6200"
	recNeighbor  = "13cd9751d86c0831b029719d681c17683960a5a54de4c931a943695e86f1e200"

	recShared1 = "624b7c58572e84d651f778a6142adb359e74786f12540c52cf39ff1ebc2dd700"
	recShared2 = "40626a72baa61c5e9cad3a3b15e9e32f29ebb8760001cfe7707afa798d9dc900"
	recAOnly   = "adba6f6c1840ccf5c17afdaec99c7467a525a951505bbcd34ad198a3fcfd8d00"
	recBOnly   = "65bbf60c689560769e4cbc7fcb65a2a2c2a67c789c6fee4502ce70ad11427400"
	recRebin   = "07722a1642bf1d059dfebb0b3a0200c4c600acdbc6c622b008e11a2f40fd7100"
)

func newPebbleForTest(t *testing.T) store.Store {
	t.Helper()
	st, err := pebble.New(config.Pebble{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// makeCompoundForTest builds a parseable BRC-74 compound with every txid as
// a flagged level-0 leaf. Root validity doesn't matter here — the
// reconciler only parses leaves and tests membership.
func makeCompoundForTest(t *testing.T, blockHeight uint32, txids ...string) []byte {
	t.Helper()
	leaves := make([]*transaction.PathElement, 0, len(txids))
	isTxid := true
	for i, id := range txids {
		h, err := sdkchainhash.NewHashFromHex(id)
		if err != nil {
			t.Fatalf("parse txid %s: %v", id, err)
		}
		leaves = append(leaves, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   h,
			Txid:   &isTxid,
		})
	}
	return transaction.NewMerklePath(blockHeight, [][]*transaction.PathElement{leaves}).Bytes()
}

// seedMined registers rows and anchors them to blockHash at height.
func seedMined(t *testing.T, st store.Store, blockHash string, height uint64, txids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range txids {
		if _, _, err := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: id, Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	if _, _, err := st.SetMinedByTxIDs(ctx, blockHash, height, txids); err != nil {
		t.Fatalf("seed mined: %v", err)
	}
}

func seedSeen(t *testing.T, st store.Store, txids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range txids {
		if _, _, err := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: id, Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

func newTestReconciler(st store.Store, pub *capturePublisher, stub *stubChaintracks, tweak func(*config.ReconcilerConfig)) *Reconciler {
	cfg := &config.Config{BumpBuilder: config.BumpBuilderConfig{
		AnchorGuardEnabled: true,
		Reconciler: config.ReconcilerConfig{
			Enabled:           true,
			IntervalMs:        50,
			BatchSize:         100,
			BlocksPerTick:     10,
			NeighborhoodDepth: 6,
			MaxDeferAttempts:  3,
		},
	}}
	if tweak != nil {
		tweak(&cfg.BumpBuilder.Reconciler)
	}
	return &Reconciler{
		cfg:         cfg,
		logger:      zap.NewNop(),
		store:       st,
		publisher:   pub,
		chainHeader: stub,
		defers:      make(map[string]int),
		now:         time.Now,
		done:        make(chan struct{}),
	}
}

func statusOf(t *testing.T, st store.Store, txid string) *models.TransactionStatus {
	t.Helper()
	got, err := st.GetStatus(context.Background(), txid)
	if err != nil || got == nil {
		t.Fatalf("GetStatus %s: %v (nil=%v)", txid, err, got == nil)
	}
	return got
}

// TestReconciler_MixedReanchorAndRevert is the incident in miniature: the
// orphan's shared txs re-anchor to the canonical block (whose stored BUMP
// is the fuel), the guard-denied canonical-only tx finally mines, and the
// orphan-only tx reverts to SEEN_ON_NETWORK — with corrected events,
// orphaned-anchor history, STUMP cleanup, and a reconciled_at stamp. A
// second pass proves idempotency (crash-resume contract).
func TestReconciler_MixedReanchorAndRevert(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	// State as the reorg left it: shared+bOnly MINED@orphan, aOnly SEEN
	// (the write guard refused it while the canonical block was an
	// alternate), canonical BUMP stored, orphan row queued.
	seedMined(t, st, recOrphan, 10, recShared1, recShared2, recBOnly)
	seedSeen(t, st, recAOnly)
	if err := st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1, recShared2, recAOnly)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	if err := st.InsertStump(ctx, &models.Stump{BlockHash: recOrphan, SubtreeIndex: 0, StumpData: []byte{0x01}}); err != nil {
		t.Fatalf("insert stump: %v", err)
	}
	if err := st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()); err != nil {
		t.Fatalf("upsert orphan row: %v", err)
	}
	if err := st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now()); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	// Shared txs re-anchored with the orphan preserved as history.
	for _, id := range []string{recShared1, recShared2} {
		got := statusOf(t, st, id)
		if got.Status != models.StatusMined || got.BlockHash != recCanonical {
			t.Fatalf("%s: want MINED@%s, got %s@%s", id, recCanonical, got.Status, got.BlockHash)
		}
		if len(got.OrphanedProofs) != 1 || got.OrphanedProofs[0].BlockHash != recOrphan {
			t.Fatalf("%s: want orphaned-anchor history [%s], got %+v", id, recOrphan, got.OrphanedProofs)
		}
	}
	// The guard-denied canonical-only tx finally mined.
	if got := statusOf(t, st, recAOnly); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("aOnly: want MINED@%s, got %s@%s", recCanonical, got.Status, got.BlockHash)
	}
	// The orphan-only tx reverted, history preserved, block fields cleared.
	if got := statusOf(t, st, recBOnly); got.Status != models.StatusSeenOnNetwork || got.BlockHash != "" ||
		len(got.OrphanedProofs) != 1 || got.OrphanedProofs[0].BlockHash != recOrphan {
		t.Fatalf("bOnly: want reverted SEEN with history, got %+v", got)
	}
	// STUMPs pruned; the orphan's BUMP retained (historical proofs).
	if stumps, _ := st.GetStumpsByBlockHash(ctx, recOrphan); len(stumps) != 0 {
		t.Fatalf("orphan STUMPs must be pruned, got %d", len(stumps))
	}
	// Row stamped reconciled and off the queue.
	if rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 0 {
		t.Fatalf("queue must be empty after reconcile: rows=%v err=%v", rows, err)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.ReconciledAt == nil {
		t.Fatalf("reconciled_at must be stamped, got %+v err=%v", bp, err)
	}

	// Corrected events: one MINED bulk (re-anchor marker, canonical block,
	// covering shared+aOnly) and one SEEN bulk (revert marker, bOnly).
	var minedEv, seenEv *models.TransactionStatus
	for _, ev := range pub.bulkEvents() {
		switch ev.ExtraInfo {
		case models.ExtraInfoReorgReanchor:
			minedEv = ev
		case models.ExtraInfoReorgUnmined:
			seenEv = ev
		}
	}
	if minedEv == nil || minedEv.BlockHash != recCanonical || len(minedEv.TxIDs) != 3 {
		t.Fatalf("re-anchor event wrong: %+v", minedEv)
	}
	if seenEv == nil || len(seenEv.TxIDs) != 1 || seenEv.TxIDs[0] != recBOnly {
		t.Fatalf("revert event wrong: %+v", seenEv)
	}

	// Idempotency: a second tick finds an empty queue and publishes nothing.
	before := len(pub.bulkEvents())
	r.tick(ctx)
	if after := len(pub.bulkEvents()); after != before {
		t.Fatalf("second tick must be a no-op, events %d → %d", before, after)
	}
}

// TestReconciler_ResurrectedBlockResetsRow: a stale orphan mark on a block
// that IS the active-chain block at its height resets the row to active
// without touching transactions.
func TestReconciler_ResurrectedBlockResetsRow(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10)) // the "orphan" is actually active

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("resurrected block's txs must be untouched, got %s@%s", got.Status, got.BlockHash)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must be reset to active with orphan/reconcile marks cleared, got %+v err=%v", bp, err)
	}
}

// TestReconciler_DefersUntilCanonicalBUMPArrives: no canonical BUMP yet →
// the orphan stays queued and its txs untouched; once the BUMP lands the
// next tick heals.
func TestReconciler_DefersUntilCanonicalBUMPArrives(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	if got := statusOf(t, st, recShared1); got.BlockHash != recOrphan {
		t.Fatalf("deferred orphan's txs must be untouched, got anchored to %s", got.BlockHash)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 1 {
		t.Fatalf("deferred orphan must stay queued, got %d rows", len(rows))
	}

	if err := st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	r.tick(ctx)
	if got := statusOf(t, st, recShared1); got.BlockHash != recCanonical {
		t.Fatalf("post-BUMP tick must re-anchor, got %s", got.BlockHash)
	}
}

// TestReconciler_DeferCapFallsBackToRevert: the OPT-IN legacy fallback
// (RevertWhenUnreconcilable) — when the canonical BUMP never arrives, the
// defer cap converts the orphan to revert-all. Default behavior is now to
// park instead (see TestReconciler_DeferCapParksByDefault, issue #282).
func TestReconciler_DeferCapFallsBackToRevert(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.MaxDeferAttempts = 1
		c.RevertWhenUnreconcilable = true // opt into the legacy revert-all fallback
	})
	r.tick(ctx) // defer 1/1
	r.tick(ctx) // cap reached → revert-all

	if got := statusOf(t, st, recShared1); got.Status != models.StatusSeenOnNetwork || got.BlockHash != "" {
		t.Fatalf("defer-cap fallback must revert, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("orphan must be reconciled after the fallback, got %d rows", len(rows))
	}
}

// TestReconciler_DeferCapParksByDefault: when the canonical BUMP never
// arrives, the DEFAULT fallback is to PARK the block (issue #282) — the txs
// stay MINED against the orphan (NOT reverted to SEEN_ON_NETWORK), no revert
// event is published, and the block leaves the queue (bounded, no infinite
// retry). Because the txs are left in place, a later stored/rebuilt canonical
// BUMP re-anchors them through the normal mine path.
func TestReconciler_DeferCapParksByDefault(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	// The canonical block at height 10 exists on-chain, but its BUMP is NOT
	// stored — the reconciler cannot prove where the txs belong.
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = 1 })
	r.tick(ctx) // defer 1/1
	r.tick(ctx) // cap reached → PARK (default)

	// The tx stays MINED against the orphan — the #282 invariant: no un-mine.
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("parked orphan's tx must stay MINED@orphan, got %s@%s", got.Status, got.BlockHash)
	}
	// No revert event published.
	for _, ev := range pub.bulkEvents() {
		if ev.ExtraInfo == models.ExtraInfoReorgUnmined {
			t.Fatalf("park must not publish a revert event, got %+v", ev)
		}
	}
	// Block off the queue (bounded — no infinite retry) and stamped.
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("parked block must leave the reconcile queue, got %d rows", len(rows))
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.ReconciledAt == nil {
		t.Fatalf("parked block must be stamped reconciled_at, got %+v err=%v", bp, err)
	}

	// The txs were left in place, so once the canonical block IS processed
	// (its BUMP stored → normal mine path), they re-anchor to canonical.
	if _, _, err := st.SetMinedByTxIDs(ctx, recCanonical, 10, []string{recShared1}); err != nil {
		t.Fatalf("SetMinedByTxIDs (build-path re-anchor): %v", err)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("a later canonical BUMP must re-anchor the parked tx, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_NeighborhoodReanchor: a deep-reorg tx re-binned into a
// LATER canonical block re-anchors there via the membership check.
func TestReconciler_NeighborhoodReanchor(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))

	seedMined(t, st, recOrphan, 10, recRebin)
	// Canonical block at 10 does NOT contain the tx; the block at 11 does.
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.InsertBUMP(ctx, recNeighbor, 11, makeCompoundForTest(t, 11, recRebin))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recNeighbor || got.BlockHeight != 11 {
		t.Fatalf("re-binned tx must anchor to the neighbor block, got %s@%s h%d", got.Status, got.BlockHash, got.BlockHeight)
	}
}

// TestReconciler_FullScanDetectsStaleAnchors: the startup sweep finds an
// 'active' row whose height is held by a different block — the recovery
// path for incidents that predate the detection edges (the height-764
// scale-cluster block).
func TestReconciler_FullScanDetectsStaleAnchors(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) // still 'active' — nothing ever detected it
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.fullScan(ctx)
	r.tick(ctx)

	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("full-scan must route the stale anchor into healing, got %s@%s", got.Status, got.BlockHash)
	}
	if len(pub.bulkEvents()) == 0 {
		t.Fatal("expected corrected events from the full-scan heal")
	}
}

// TestReconciler_FullScanRespectsHorizon: the startup sweep is bounded to
// within FullScanDepth of the active tip (issue #282) — a stale off-chain
// row far below the tip is NOT re-orphaned by the default depth, but an
// explicit target range picks it up. This is what stops the deep backstop
// from grinding a long chain's whole history oldest-first and starving the
// actual recent incident.
func TestReconciler_FullScanRespectsHorizon(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}

	// Active tip at height 200 (on-chain → never flagged; sets GetActiveTip).
	stub.setHeightHeader(200, headerWithHash(t, recCanonical, 200))
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 200, time.Now())
	// A stale off-chain 'active' row far below the tip: the active chain
	// holds a DIFFERENT block at height 10.
	stub.setHeightHeader(10, headerWithHash(t, recNeighbor, 10))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())

	// Default depth (144): height 10 < tip-144 (=56) ⇒ out of horizon, so it
	// must NOT be marked.
	r := newTestReconciler(st, pub, stub, nil)
	r.fullScan(ctx)
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("horizon must skip height 10 (tip 200, depth 144), got %d queued", len(rows))
	}
	if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("out-of-horizon row must stay active, got %+v err=%v", bp, err)
	}

	// An explicit target range covering height 10 overrides the depth horizon
	// ⇒ the stale anchor is now marked orphaned and queued.
	r2 := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.FullScanMinHeight = 1
		c.FullScanMaxHeight = 50
	})
	r2.fullScan(ctx)
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].BlockHash != recOrphan {
		t.Fatalf("targeted scan must mark height 10 orphaned, got %+v err=%v", rows, err)
	}
}
