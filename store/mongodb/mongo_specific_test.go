//go:build mongodb

package mongodb

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Behaviour that is specific to this backend or that no other backend's
// suite pins: the census aggregate, since-filtering, GridFS atomicity and
// size, the version-CAS fallback, the replay bound, and lease expiry.

func seedStatus(t *testing.T, s *Store, txid string, status models.Status, ts time.Time) {
	t.Helper()
	if _, _, err := s.GetOrInsertStatus(context.Background(), &models.TransactionStatus{TxID: txid, Status: status, Timestamp: ts}); err != nil {
		t.Fatalf("seed %s: %v", txid, err)
	}
}

func TestCensusStatusesSince_HalfOpenWindow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Now().Truncate(time.Millisecond).Add(-time.Hour)
	since, deadline := base.Add(10*time.Minute), base.Add(20*time.Minute)

	seedStatus(t, s, "before", models.StatusReceived, since.Add(-time.Millisecond)) // out: < since
	seedStatus(t, s, "at-since", models.StatusReceived, since)                      // in: >= since
	seedStatus(t, s, "mid", models.StatusReceived, since.Add(5*time.Minute))        // in
	seedStatus(t, s, "at-deadline", models.StatusReceived, deadline)                // out: not < deadline
	seedStatus(t, s, "sent", models.StatusSentToNetwork, since.Add(time.Minute))    // in, other status
	seedStatus(t, s, "mined", models.StatusMined, since.Add(time.Minute))           // in window but not requested

	got, err := s.CensusStatusesSince(ctx, since, deadline, []models.Status{models.StatusReceived, models.StatusSentToNetwork, models.StatusPendingRetry})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected exactly one entry per requested status, got %v", got)
	}
	if c := got[models.StatusReceived]; c.Count != 2 || !c.Oldest.Equal(since) {
		t.Errorf("RECEIVED census = %+v, want count 2 oldest %v", c, since)
	}
	if c := got[models.StatusSentToNetwork]; c.Count != 1 {
		t.Errorf("SENT census = %+v, want count 1", c)
	}
	if c := got[models.StatusPendingRetry]; c.Count != 0 || !c.Oldest.IsZero() {
		t.Errorf("PENDING_RETRY census must be zero-valued, got %+v", c)
	}
	if _, ok := got[models.StatusMined]; ok {
		t.Errorf("unrequested status must not appear")
	}

	// Zero since: only the deadline bounds the window.
	got, err = s.CensusStatusesSince(ctx, time.Time{}, deadline, []models.Status{models.StatusReceived})
	if err != nil {
		t.Fatal(err)
	}
	if c := got[models.StatusReceived]; c.Count != 3 {
		t.Errorf("zero-since census = %+v, want count 3", c)
	}
}

func TestIterateStatusesSince_HonorsSinceAndDesc(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	seedStatus(t, s, "old", models.StatusReceived, now.Add(-3*time.Hour))
	seedStatus(t, s, "mid", models.StatusReceived, now.Add(-2*time.Hour))
	seedStatus(t, s, "new", models.StatusReceived, now.Add(-time.Hour))

	var order []string
	if err := s.IterateStatusesSince(ctx, now.Add(-150*time.Minute), func(st *models.TransactionStatus) error {
		order = append(order, st.TxID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "new" || order[1] != "mid" {
		t.Fatalf("expected [new mid] (since honored, newest first), got %v", order)
	}
	all, err := s.GetStatusesSince(ctx, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].TxID != "new" || all[2].TxID != "old" {
		t.Fatalf("zero since must return every row newest first, got %d rows", len(all))
	}
	// fn error stops iteration and surfaces.
	sentinel := errors.New("stop")
	calls := 0
	err = s.IterateStatusesSince(ctx, time.Time{}, func(*models.TransactionStatus) error { calls++; return sentinel })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("fn error must stop and surface: err=%v calls=%d", err, calls)
	}
}

func TestGetStatus_MissingIsNilNil(t *testing.T) {
	s := newTestStore(t)
	got, err := s.GetStatus(context.Background(), "nope")
	if err != nil || got != nil {
		t.Fatalf("missing row must be (nil, nil), got (%v, %v)", got, err)
	}
}

func TestSetPendingRetryFields_UnknownTxIDIsNotFound(t *testing.T) {
	s := newTestStore(t)
	err := s.SetPendingRetryFields(context.Background(), "ghost", []byte{1}, time.Now())
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if got, _ := s.GetStatus(context.Background(), "ghost"); got != nil {
		t.Fatalf("must not create a phantom row: %+v", got)
	}
}

func TestLease_ExpiryAndRelease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if held, err := s.TryAcquireOrRenew(ctx, "l", "a", 200*time.Millisecond); err != nil || held.IsZero() {
		t.Fatalf("a acquire: %v %v", held, err)
	}
	if held, err := s.TryAcquireOrRenew(ctx, "l", "b", time.Second); err != nil || !held.IsZero() {
		t.Fatalf("b must be blocked while a holds: %v %v", held, err)
	}
	time.Sleep(250 * time.Millisecond)
	if held, err := s.TryAcquireOrRenew(ctx, "l", "b", time.Second); err != nil || held.IsZero() {
		t.Fatalf("b must acquire after a expired: %v %v", held, err)
	}
	if err := s.Release(ctx, "l", "a"); err != nil {
		t.Fatalf("release by non-holder must be a nil no-op: %v", err)
	}
	if held, err := s.TryAcquireOrRenew(ctx, "l", "a", time.Second); err != nil || !held.IsZero() {
		t.Fatalf("a's no-op release must not have freed b's lease: %v %v", held, err)
	}
	if err := s.Release(ctx, "l", "b"); err != nil {
		t.Fatal(err)
	}
	if held, err := s.TryAcquireOrRenew(ctx, "l", "a", time.Second); err != nil || held.IsZero() {
		t.Fatalf("a must acquire after b released: %v %v", held, err)
	}
}

// A rebuild must never leave a window where GetBUMP reports ErrNotFound:
// the new file lands before the old one is pruned, and exactly one copy
// remains afterwards.
func TestInsertBUMP_OverwriteNeverGaps(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const hash = "bump-overwrite"
	v1, v2 := bytes.Repeat([]byte{1}, 3000), bytes.Repeat([]byte{2}, 5000)
	if err := s.InsertBUMP(ctx, hash, 7, v1); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var readErr error
	var mu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			h, data, err := s.GetBUMP(ctx, hash)
			if err != nil || h != 7 || (!bytes.Equal(data, v1) && !bytes.Equal(data, v2)) {
				mu.Lock()
				readErr = fmt.Errorf("reader saw h=%d len=%d err=%v", h, len(data), err)
				mu.Unlock()
				return
			}
		}
	}()
	for i := 0; i < 5; i++ {
		if err := s.InsertBUMP(ctx, hash, 7, v2); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if readErr != nil {
		t.Fatal(readErr)
	}
	_, data, err := s.GetBUMP(ctx, hash)
	if err != nil || !bytes.Equal(data, v2) {
		t.Fatalf("final read: %v (len %d)", err, len(data))
	}
	n, err := s.bumps.bucket.GetFilesCollection().CountDocuments(ctx, doc(kv(fMetaBlockHash, hash)))
	if err != nil || n != 1 {
		t.Fatalf("expected exactly one surviving file, got %d (%v)", n, err)
	}
	if err := s.DeleteBUMPByBlockHash(ctx, hash); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetBUMP(ctx, hash); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("after delete expected ErrNotFound, got %v", err)
	}
	if err := s.DeleteBUMPByBlockHash(ctx, hash); err != nil {
		t.Fatalf("delete must be idempotent: %v", err)
	}
}

// GridFS is load-bearing: a STUMP larger than the 16 MB document cap must
// round-trip, and a re-insert for the same subtree supersedes the old copy.
func TestStump_LargePayloadAndSupersede(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const hash = "stump-big"
	big := make([]byte, 17<<20)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertStump(ctx, &models.Stump{BlockHash: hash, SubtreeIndex: 3, StumpData: big}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertStump(ctx, &models.Stump{BlockHash: hash, SubtreeIndex: 1, StumpData: []byte("small")}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertStump(ctx, &models.Stump{BlockHash: hash, SubtreeIndex: 1, StumpData: []byte("small-v2")}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetStumpsByBlockHash(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].SubtreeIndex != 1 || got[1].SubtreeIndex != 3 {
		t.Fatalf("expected subtrees [1 3], got %d stumps", len(got))
	}
	if string(got[0].StumpData) != "small-v2" {
		t.Fatalf("re-insert must supersede: got %q", got[0].StumpData)
	}
	if !bytes.Equal(got[1].StumpData, big) {
		t.Fatalf("17 MB stump did not round-trip (len %d)", len(got[1].StumpData))
	}
	n, err := s.stumps.bucket.GetFilesCollection().CountDocuments(ctx, doc(kv(fMetaBlockHash, hash)))
	if err != nil || n != 2 {
		t.Fatalf("expected 2 surviving files, got %d (%v)", n, err)
	}
	if err := s.DeleteStumpsByBlockHash(ctx, hash); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetStumpsByBlockHash(ctx, hash); err != nil || len(got) != 0 {
		t.Fatalf("after delete: %d stumps, %v", len(got), err)
	}
}

// Concurrent rebuilds of one block must converge on exactly one surviving
// file that the manifest references, never on none: each writer deletes only
// the file its own manifest swap replaced. Same for one STUMP subtree.
func TestInsertBlob_ConcurrentOverwritesKeepExactlyOne(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const hash, writers = "blob-race", 8
	payloads := make([][]byte, writers)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte(i + 1)}, 2048+i)
	}
	run := func(write func(int) error) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := write(i); err != nil {
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
	isPayload := func(data []byte) bool {
		for _, p := range payloads {
			if bytes.Equal(data, p) {
				return true
			}
		}
		return false
	}

	run(func(i int) error { return s.InsertBUMP(ctx, hash, 9, payloads[i]) })
	h, data, err := s.GetBUMP(ctx, hash)
	if err != nil || h != 9 || !isPayload(data) {
		t.Fatalf("after concurrent inserts GetBUMP = (%d, %d bytes, %v)", h, len(data), err)
	}
	var m bumpManifest
	if err := s.bumps.manifests.FindOne(ctx, idFilter(hash)).Decode(&m); err != nil {
		t.Fatal(err)
	}
	n, err := s.bumps.bucket.GetFilesCollection().CountDocuments(ctx, doc(kv(fMetaBlockHash, hash)))
	if err != nil || n != 1 {
		t.Fatalf("expected exactly one surviving BUMP file, got %d (%v)", n, err)
	}
	if c, _ := s.bumps.bucket.GetFilesCollection().CountDocuments(ctx, idFilter(m.FileID.Hex())); c != 0 {
		t.Fatalf("manifest id lookup by hex must not match; sanity")
	}
	var surviving struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := s.bumps.bucket.GetFilesCollection().FindOne(ctx, doc(kv(fMetaBlockHash, hash))).Decode(&surviving); err != nil {
		t.Fatal(err)
	}
	if surviving.ID != m.FileID {
		t.Fatalf("manifest references %s but the surviving file is %s", m.FileID.Hex(), surviving.ID.Hex())
	}

	run(func(i int) error {
		return s.InsertStump(ctx, &models.Stump{BlockHash: hash, SubtreeIndex: 4, StumpData: payloads[i]})
	})
	stumps, err := s.GetStumpsByBlockHash(ctx, hash)
	if err != nil || len(stumps) != 1 || stumps[0].SubtreeIndex != 4 || !isPayload(stumps[0].StumpData) {
		t.Fatalf("after concurrent stump inserts: %d stumps, %v", len(stumps), err)
	}
	n, err = s.stumps.bucket.GetFilesCollection().CountDocuments(ctx, doc(kv(fMetaBlockHash, hash)))
	if err != nil || n != 1 {
		t.Fatalf("expected exactly one surviving STUMP file, got %d (%v)", n, err)
	}
}

// The interface requires both anchor fields on every MINED row; a zero height
// is persisted as a literal 0 like the other backends, not dropped.
func TestSetMinedByTxIDs_ZeroHeightPersistsNumeric(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedStatus(t, s, "zero", models.StatusSeenOnNetwork, time.Now())
	if _, mined, err := s.SetMinedByTxIDs(ctx, "blk", 0, []string{"zero"}); err != nil || len(mined) != 1 {
		t.Fatalf("SetMinedByTxIDs: %v (%d mined)", err, len(mined))
	}
	var raw bson.M
	if err := s.tx.FindOne(ctx, idFilter("zero")).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if v, ok := raw[fBlockHeight]; !ok || v != int64(0) {
		t.Fatalf("block_height must be stored as a literal 0, got %v (present=%v)", v, ok)
	}
	if n := countTracker(t, s); n != 1 {
		t.Fatalf("MINED row with height 0 must still be tracked, got %d rows", n)
	}
}

func TestGetStumpsByBlockHash_CancelledContext(t *testing.T) {
	s := newTestStore(t)
	if err := s.InsertStump(context.Background(), &models.Stump{BlockHash: "h", SubtreeIndex: 0, StumpData: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.GetStumpsByBlockHash(ctx, "h"); err == nil {
		t.Fatal("expected an error from a cancelled context")
	}
}

// A row already MINED on the same block by another call (a replayed
// BLOCK_PROCESSED, or a replica) is rewritten with this call's timestamp and
// reported with that MINED row as its pre-image, so persisted row and
// returned snapshot agree; IMMUTABLE rows are untouched and unreported.
func TestSetMinedByTxIDs_SameBlockReplayAndImmutable(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := msNow()
	earlier := now.Add(-time.Minute)
	seedStatus(t, s, "fresh", models.StatusSeenOnNetwork, earlier)
	seedStatus(t, s, "twice", models.StatusMined, earlier)
	seedStatus(t, s, "frozen", models.StatusImmutable, earlier)
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{TxID: "twice", Status: models.StatusMined, BlockHash: "blk", BlockHeight: 42, Timestamp: earlier}); err != nil {
		t.Fatal(err)
	}

	prevs, mined, err := s.SetMinedByTxIDs(ctx, "blk", 42, []string{"fresh", "twice", "frozen", "ghost", "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	if len(prevs) != 2 || len(mined) != 2 {
		t.Fatalf("expected fresh+twice only, got prevs=%d mined=%d", len(prevs), len(mined))
	}
	byTx := map[string]*models.TransactionStatus{}
	for _, p := range prevs {
		byTx[p.TxID] = p
	}
	if p := byTx["fresh"]; p == nil || p.Status != models.StatusSeenOnNetwork || !p.Timestamp.Equal(earlier) {
		t.Fatalf("fresh prev = %+v", p)
	}
	if p := byTx["twice"]; p == nil || p.Status != models.StatusMined || p.BlockHash != "blk" || !p.Timestamp.Equal(earlier) {
		t.Fatalf("twice prev must be the earlier MINED row, got %+v", p)
	}
	for _, m := range mined {
		got, _ := s.GetStatus(ctx, m.TxID)
		if got.Status != models.StatusMined || got.BlockHash != "blk" || got.BlockHeight != 42 || !got.Timestamp.Equal(m.Timestamp) || !got.Timestamp.After(earlier) {
			t.Fatalf("%s: persisted %+v disagrees with returned %+v", m.TxID, got, m)
		}
		if len(got.OrphanedProofs) != 0 {
			t.Fatalf("%s: same-block re-mine must not invent history: %+v", m.TxID, got.OrphanedProofs)
		}
	}
	got, _ := s.GetStatus(ctx, "frozen")
	if got.Status != models.StatusImmutable || !got.Timestamp.Equal(earlier) {
		t.Fatalf("IMMUTABLE row must be untouched: %+v", got)
	}
}

// A block larger than one page is rewritten completely through keyset
// pages, every row is reported exactly once, and the guard re-checked at
// write time skips a row that was re-anchored elsewhere in the meantime.
func TestSetStatusByBlockHash_PagesAndReChecksAnchor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := s.batchSize*2 + 7
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		txid := fmt.Sprintf("blk-tx-%05d", i)
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusMined, BlockHash: "blk", BlockHeight: 42, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
		want[txid] = true
	}
	// One row moved to another block and one was promoted before the revert.
	if _, _, err := s.SetMinedByTxIDs(ctx, "other", 42, []string{"blk-tx-00003"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{TxID: "blk-tx-00004", Status: models.StatusImmutable}); err != nil {
		t.Fatal(err)
	}
	delete(want, "blk-tx-00003")
	delete(want, "blk-tx-00004")

	got, err := s.SetStatusByBlockHash(ctx, "blk", models.StatusSeenOnNetwork)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, txid := range got {
		seen[txid]++
	}
	if len(got) != len(want) || len(seen) != len(want) {
		t.Fatalf("expected %d distinct txids, got %d (%d distinct)", len(want), len(got), len(seen))
	}
	for txid := range want {
		if seen[txid] != 1 {
			t.Fatalf("%s reported %d times", txid, seen[txid])
		}
	}
	if left, _ := s.GetTxIDsByBlockHash(ctx, "blk"); len(left) != 1 || left[0] != "blk-tx-00004" {
		t.Fatalf("only the IMMUTABLE row may stay anchored, got %v", left)
	}
	moved, _ := s.GetStatus(ctx, "blk-tx-00003")
	if moved.Status != models.StatusMined || moved.BlockHash != "other" {
		t.Fatalf("re-anchored row must be untouched: %+v", moved)
	}
	reverted, _ := s.GetStatus(ctx, "blk-tx-00000")
	if reverted.Status != models.StatusSeenOnNetwork || reverted.BlockHash != "" || reverted.BlockHeight != 0 ||
		len(reverted.OrphanedProofs) != 1 || reverted.OrphanedProofs[0].BlockHash != "blk" || reverted.OrphanedProofs[0].BlockHeight != 42 {
		t.Fatalf("reverted row wrong: %+v", reverted)
	}
}

// Concurrent first upserts for one key must all succeed and converge on one
// document: the loser of the insert race retries against the winner's row.
func TestUpserts_ConcurrentFirstInsert(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const writers = 8
	run := func(name string, write func(int) error) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := write(i); err != nil {
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("%s: %v", name, err)
		}
	}
	now := msNow()
	run("UpsertBlockHeaderSeen", func(int) error { return s.UpsertBlockHeaderSeen(ctx, "race-blk", 7, now) })
	run("MarkBlockProcessed", func(int) error { return s.MarkBlockProcessed(ctx, "race-blk-2", 8, now) })
	run("UpsertDatahubEndpoint", func(i int) error {
		return s.UpsertDatahubEndpoint(ctx, store.DatahubEndpoint{URL: "http://race", Network: "main", Source: "discovered", LastSeen: now})
	})
	run("UpsertPeerPolicy", func(i int) error {
		return s.UpsertPeerPolicy(ctx, store.PeerPolicy{PeerID: "race-peer", Network: "main", MiningFeeSatoshis: uint64(i + 1), MiningFeeBytes: 1000, LastSeen: now})
	})
	for _, hash := range []string{"race-blk", "race-blk-2"} {
		if _, err := s.GetBlockProcessingStatus(ctx, hash); err != nil {
			t.Fatalf("%s: %v", hash, err)
		}
	}
	if eps, _ := s.ListDatahubEndpoints(ctx, "main"); len(eps) != 1 {
		t.Fatalf("expected one endpoint, got %d", len(eps))
	}
	if pps, _ := s.ListPeerPolicies(ctx, "main"); len(pps) != 1 {
		t.Fatalf("expected one peer policy, got %d", len(pps))
	}
}

// Past the submission bound the replay is refused before anything is
// materialized; within it, it is served.
func TestIterateStatusesByToken_RefusesPastBound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const token, total = "tok-huge", 50
	for i := 0; i < total; i++ {
		txid := fmt.Sprintf("tx-%03d", i)
		seedStatus(t, s, txid, models.StatusSeenOnNetwork, time.Now())
		if err := s.InsertSubmission(ctx, &models.Submission{SubmissionID: fmt.Sprintf("sub-%03d", i), TxID: txid, CallbackToken: token}); err != nil {
			t.Fatal(err)
		}
	}
	s.tokenReplayLimit = 10
	err := s.IterateStatusesByToken(ctx, token, time.Time{}, nil, func(*models.TransactionStatus) error {
		t.Fatal("fn must not be called when refusing")
		return nil
	})
	if !errors.Is(err, store.ErrReplayUnavailable) {
		t.Fatalf("expected ErrReplayUnavailable, got %v", err)
	}
	s.tokenReplayLimit = total
	n := 0
	if err := s.IterateStatusesByToken(ctx, token, time.Time{}, nil, func(*models.TransactionStatus) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != total {
		t.Fatalf("within budget expected %d rows, got %d", total, n)
	}
}

// UpsertPeerPolicy is a full overwrite and ListPeerPolicies scopes by network.
func TestMarkBlockMilestones_SynthesiseHeaderSeen(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	at := msNow()
	if err := s.MarkBlockProcessed(ctx, "blk", 5, at); err != nil {
		t.Fatal(err)
	}
	bp, err := s.GetBlockProcessingStatus(ctx, "blk")
	if err != nil {
		t.Fatal(err)
	}
	if !bp.HeaderSeenAt.Equal(at) || bp.ProcessedAt == nil || bp.Status != models.BlockStatusActive || bp.BlockHeight != 5 {
		t.Fatalf("callback-before-header row wrong: %+v", bp)
	}
	seen := at.Add(-time.Minute)
	if err := s.UpsertBlockHeaderSeen(ctx, "blk", 6, seen); err != nil {
		t.Fatal(err)
	}
	bp, _ = s.GetBlockProcessingStatus(ctx, "blk")
	if !bp.HeaderSeenAt.Equal(at) || bp.BlockHeight != 6 || bp.ProcessedAt == nil {
		t.Fatalf("header re-arrival must keep header_seen_at/processed_at and overwrite height: %+v", bp)
	}
}
