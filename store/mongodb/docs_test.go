package mongodb

import (
	"slices"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// These tests need no server: they pin the BSON encoding contract every
// query in the package relies on (absent-when-zero, ms precision, binary
// bytes) and prove the two pushed-down predicates agree with the Go
// functions they stand in for.

func TestTxDoc_RoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 123_456_789, time.UTC)
	st := &models.TransactionStatus{
		TxID:               "ab",
		Status:             models.StatusMined,
		StatusCode:         7,
		Timestamp:          now,
		BlockHash:          "bh",
		BlockHeight:        900_123,
		MerklePath:         models.HexBytes{1, 2, 3},
		ExtraInfo:          "x",
		CompetingTxs:       []string{"c1", "c2"},
		RawTx:              models.HexBytes{9, 9},
		RetryCount:         2,
		NextRetryAt:        now.Add(time.Minute),
		MerkleRegisteredAt: now.Add(-time.Minute),
		OrphanedProofs: []models.OrphanedAnchor{{
			BlockHash: "old", BlockHeight: 900_122, OrphanedAt: now,
			MerklePath: models.HexBytes{5}, // never persisted
		}},
	}
	doc := txDocFromStatus(st)
	doc.Version = 1
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var back txDoc
	if err := bson.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	got := back.toStatus()

	// Conversion truncated the caller's struct to ms so it equals the read.
	if !st.Timestamp.Equal(now.Truncate(time.Millisecond)) {
		t.Fatalf("caller timestamp not truncated to ms: %v", st.Timestamp)
	}
	if !got.Timestamp.Equal(st.Timestamp) || !got.NextRetryAt.Equal(st.NextRetryAt) ||
		!got.MerkleRegisteredAt.Equal(st.MerkleRegisteredAt) || !got.CreatedAt.Equal(st.CreatedAt) {
		t.Fatalf("time fields drifted: got %+v want %+v", got, st)
	}
	if got.TxID != st.TxID || got.Status != st.Status || got.StatusCode != st.StatusCode ||
		got.BlockHash != st.BlockHash || got.BlockHeight != st.BlockHeight ||
		got.ExtraInfo != st.ExtraInfo || got.RetryCount != st.RetryCount ||
		!slices.Equal(got.CompetingTxs, st.CompetingTxs) ||
		!slices.Equal(got.MerklePath, st.MerklePath) || !slices.Equal(got.RawTx, st.RawTx) {
		t.Fatalf("scalar fields drifted:\n got %+v\nwant %+v", got, st)
	}
	if len(got.OrphanedProofs) != 1 || got.OrphanedProofs[0].BlockHash != "old" ||
		got.OrphanedProofs[0].BlockHeight != 900_122 || len(got.OrphanedProofs[0].MerklePath) != 0 {
		t.Fatalf("orphaned anchors drifted: %+v", got.OrphanedProofs)
	}
}

// Zero values must not be written: absence is the only encoding of "none".
func TestTxDoc_ZeroFieldsAbsent(t *testing.T) {
	st := &models.TransactionStatus{TxID: "ab", Status: models.StatusReceived, Timestamp: time.Now()}
	raw, err := bson.Marshal(txDocFromStatus(st))
	if err != nil {
		t.Fatal(err)
	}
	var m bson.M
	if err := bson.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{
		fStatusCode, fBlockHash, fBlockHeight, fMerklePath, fExtraInfo, fCompetingTxs,
		fRawTx, fRetryCount, fNextRetryAt, fMerkleRegisteredAt, fOrphanedAnchors,
	} {
		if _, ok := m[absent]; ok {
			t.Errorf("zero-valued %s was written as %v; must be absent", absent, m[absent])
		}
	}
	for _, present := range []string{fID, fStatus, fTimestamp, fCreatedAt} {
		if _, ok := m[present]; !ok {
			t.Errorf("%s must always be present", present)
		}
	}
	if _, isBinary := m[fRawTx].(bson.Binary); isBinary {
		t.Errorf("raw_tx should be absent, not empty binary")
	}
}

func TestTxDoc_BytesAreBinary(t *testing.T) {
	st := &models.TransactionStatus{TxID: "ab", Status: models.StatusPendingRetry, RawTx: models.HexBytes{1, 2}, Timestamp: time.Now()}
	raw, err := bson.Marshal(txDocFromStatus(st))
	if err != nil {
		t.Fatal(err)
	}
	var m bson.M
	if err := bson.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m[fRawTx].(bson.Binary); !ok {
		t.Fatalf("raw_tx encoded as %T, want bson.Binary", m[fRawTx])
	}
}

func TestSubmissionDoc_RoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	next := now.Add(time.Minute)
	sub := &models.Submission{
		SubmissionID: "s1", TxID: "t1", CallbackURL: "http://cb", CallbackToken: "tok",
		FullStatusUpdates: true, LastDeliveredStatus: models.StatusSeenOnNetwork,
		RetryCount: 3, NextRetryAt: &next, Attempts: 4, LastAttemptAt: &now,
		LastResult: "status 403", CreatedAt: now,
	}
	raw, err := bson.Marshal(submissionDocFromModel(sub))
	if err != nil {
		t.Fatal(err)
	}
	var back submissionDoc
	if err := bson.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	got := back.toModel()
	if got.SubmissionID != sub.SubmissionID || got.TxID != sub.TxID || got.CallbackURL != sub.CallbackURL ||
		got.CallbackToken != sub.CallbackToken || got.FullStatusUpdates != sub.FullStatusUpdates ||
		got.LastDeliveredStatus != sub.LastDeliveredStatus || got.RetryCount != sub.RetryCount ||
		got.Attempts != sub.Attempts || got.LastResult != sub.LastResult || !got.CreatedAt.Equal(sub.CreatedAt) ||
		got.NextRetryAt == nil || !got.NextRetryAt.Equal(next) || got.LastAttemptAt == nil || !got.LastAttemptAt.Equal(now) {
		t.Fatalf("submission drifted:\n got %+v\nwant %+v", got, sub)
	}

	// nil pointer times stay nil, not zero-time.
	sub.NextRetryAt, sub.LastAttemptAt = nil, nil
	raw, _ = bson.Marshal(submissionDocFromModel(sub))
	var m bson.M
	_ = bson.Unmarshal(raw, &m)
	if _, ok := m[fNextRetryAt]; ok {
		t.Errorf("nil next_retry_at must be absent")
	}
	if _, ok := m[fLastAttemptAt]; ok {
		t.Errorf("nil last_attempt_at must be absent")
	}
}

// The datahub nil-policy rule: nil → key absent; zero policy → present with
// explicit zeros. Both must survive a round trip distinctly.
func TestDatahubEndpointDoc_PolicyAbsentVsZero(t *testing.T) {
	withNil := datahubEndpointDoc{URL: "u", Network: "main", Source: "configured", LastSeen: time.Now(), Policy: nil}
	raw, _ := bson.Marshal(withNil)
	var m bson.M
	_ = bson.Unmarshal(raw, &m)
	if _, ok := m[fPolicy]; ok {
		t.Fatalf("nil policy must be absent, got %v", m[fPolicy])
	}
	var back datahubEndpointDoc
	_ = bson.Unmarshal(raw, &back)
	if back.toModel().Policy != nil {
		t.Fatalf("absent policy must read back nil")
	}

	zero := &store.EndpointPolicy{}
	withZero := withNil
	withZero.Policy = endpointPolicyToDoc(zero)
	raw, _ = bson.Marshal(withZero)
	m = bson.M{}
	_ = bson.Unmarshal(raw, &m)
	sub, ok := m[fPolicy].(bson.D)
	if !ok || len(sub) != 5 {
		t.Fatalf("zero policy must be a 5-field subdocument, got %T %v", m[fPolicy], m[fPolicy])
	}
	back = datahubEndpointDoc{}
	_ = bson.Unmarshal(raw, &back)
	if p := back.toModel().Policy; p == nil || *p != *zero {
		t.Fatalf("zero policy must read back as a non-nil zero policy, got %+v", p)
	}

	full := &store.EndpointPolicy{MiningFeeSatoshis: 1, MiningFeeBytes: 1000, MaxTxSizePolicy: 2, MaxScriptSizePolicy: 3, MaxTxSigopsCountsPolicy: 4}
	withZero.Policy = endpointPolicyToDoc(full)
	raw, _ = bson.Marshal(withZero)
	back = datahubEndpointDoc{}
	_ = bson.Unmarshal(raw, &back)
	if p := back.toModel().Policy; p == nil || *p != *full {
		t.Fatalf("policy drifted: %+v", p)
	}
}

func TestPeerPolicyDoc_RoundTrip(t *testing.T) {
	pp := store.PeerPolicy{PeerID: "p", Network: "main", MiningFeeSatoshis: 100, MiningFeeBytes: 1000, MaxTxSizePolicy: 5, MaxScriptSizePolicy: 6, LastSeen: time.Now().Truncate(time.Millisecond)}
	raw, _ := bson.Marshal(peerPolicyToDoc(pp))
	var back peerPolicyDoc
	_ = bson.Unmarshal(raw, &back)
	got := back.toModel()
	if got.PeerID != pp.PeerID || got.Network != pp.Network || got.MiningFeeSatoshis != pp.MiningFeeSatoshis ||
		got.MiningFeeBytes != pp.MiningFeeBytes || got.MaxTxSizePolicy != pp.MaxTxSizePolicy ||
		got.MaxScriptSizePolicy != pp.MaxScriptSizePolicy || !got.LastSeen.Equal(pp.LastSeen) {
		t.Fatalf("peer policy drifted:\n got %+v\nwant %+v", got, pp)
	}
}

// latticeMatches evaluates latticeFilter the way the server would against a
// document whose status is prev ("" = field absent).
func latticeMatches(filter bson.D, prev models.Status) bool {
	if len(filter) == 0 {
		return true
	}
	nin := filter[0].Value.(bson.D)[0].Value.([]string)
	if prev == "" {
		return true // $nin matches a missing field
	}
	return !slices.Contains(nin, string(prev))
}

// The pushed-down lattice must agree with CanTransitionFrom on the full
// status × status grid, including the same-status re-assertion and the
// no-existing-row branches.
func TestLatticeFilter_MatchesCanTransitionFrom(t *testing.T) {
	prevs := append([]models.Status{""}, models.AllStatuses()...)
	for _, target := range models.AllStatuses() {
		filter := latticeFilter(target)
		for _, prev := range prevs {
			want := target.CanTransitionFrom(prev)
			if got := latticeMatches(filter, prev); got != want {
				t.Errorf("%s ← %q: filter says %v, CanTransitionFrom says %v", target, prev, got, want)
			}
		}
	}
	if len(latticeFilter(models.StatusImmutable)) != 0 {
		t.Errorf("IMMUTABLE is reachable from anything and must produce no guard")
	}
}

// trackerMatches evaluates trackerFilter's $or-of-conjunctions against a
// document with the given status and stored block_height (absent when 0,
// mirroring omitempty).
func trackerMatches(t *testing.T, filter bson.D, status models.Status, height uint64) bool {
	t.Helper()
	branches := filter[0].Value.(bson.A)
	for _, b := range branches {
		if conjunctionMatches(t, b.(bson.D), status, height) {
			return true
		}
	}
	return false
}

func conjunctionMatches(t *testing.T, branch bson.D, status models.Status, height uint64) bool {
	t.Helper()
	for _, e := range branch {
		switch e.Key {
		case fStatus:
			switch v := e.Value.(type) {
			case string:
				if string(status) != v {
					return false
				}
			case bson.D:
				if !slices.Contains(v[0].Value.([]string), string(status)) {
					return false
				}
			default:
				t.Fatalf("unexpected status clause %T", e.Value)
			}
		case fBlockHeight:
			cond := e.Value.(bson.D)[0]
			switch cond.Key {
			case opIn: // {$in: [null, 0]} — absent, null or zero
				if height != 0 {
					return false
				}
			case opGte:
				if height < uint64(cond.Value.(int64)) { //nolint:gosec // test values are small positives
					return false
				}
			default:
				t.Fatalf("unexpected height operator %s", cond.Key)
			}
		default:
			t.Fatalf("unexpected key %s", e.Key)
		}
	}
	return true
}

func TestTrackerFilter_MatchesKeep(t *testing.T) {
	heights := []uint64{0, 1, 899_900, 899_901, 900_000}
	for _, scan := range []store.TrackerScan{{}, {PruneMinedBelow: 899_901}} {
		filter := trackerFilter(scan)
		for _, status := range append(models.AllStatuses(), "BOGUS") {
			for _, h := range heights {
				want := scan.Keep(status, h)
				if got := trackerMatches(t, filter, status, h); got != want {
					t.Errorf("scan=%+v %s@%d: filter %v, Keep %v", scan, status, h, got, want)
				}
			}
		}
	}
}

func TestMsTrunc(t *testing.T) {
	if !msTrunc(time.Time{}).IsZero() {
		t.Fatal("zero must stay zero")
	}
	ts := time.Unix(1_700_000_000, 999_999_999)
	if got := msTrunc(ts); got.Nanosecond() != 999_000_000 {
		t.Fatalf("expected ms truncation, got %d ns", got.Nanosecond())
	}
}

func TestHeightNarrowing(t *testing.T) {
	if heightToInt64(0) != 0 || heightFromInt64(-5) != 0 || heightFromInt64(7) != 7 {
		t.Fatal("height narrowing/widening broken")
	}
	if heightToInt64(^uint64(0)) != 1<<63-1 {
		t.Fatal("out-of-range height must clamp, not wrap")
	}
}

// Query-side times must be truncated to the millisecond exactly like stored
// timestamps, so a sub-millisecond `since` compares against the boundary the
// writer persisted (BSON datetimes cannot represent anything finer).
func TestSinceFilter_TruncatesToMillisecond(t *testing.T) {
	since := time.Unix(1_700_000_000, 999_999_999)
	f := sinceFilter(since)
	got := f[0].Value.(bson.D)[0].Value.(time.Time)
	if !got.Equal(since.Truncate(time.Millisecond)) {
		t.Fatalf("since not truncated: got %v", got)
	}
	if len(sinceFilter(time.Time{})) != 0 {
		t.Fatal("zero since must produce no clause")
	}
}
