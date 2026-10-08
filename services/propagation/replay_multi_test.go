package propagation

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// A tx is registered once any endpoint accepts it: with one merkle-service
// unreachable the batch still lands as fully_ok and every tx is broadcast
// (F-024 is satisfied by the surviving endpoint).
func TestRegisterBatch_PoolWithOneDeadEndpoint_FullyOK(t *testing.T) {
	log := &eventLog{}
	live := newMerkleServer(log, http.StatusOK)
	defer live.Close()

	pool := merkleservice.NewPool([]string{live.URL, "http://127.0.0.1:1"}, "", 2*time.Second)
	cfg := &config.Config{CallbackURL: "http://arcade/cb", CallbackToken: "tok"}
	cfg.Propagation.MerkleConcurrency = 4
	ms := newMockStore()
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool)

	before := testutil.ToFloat64(metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok"))
	batch := []propagationMsg{{TXID: "tx-a"}, {TXID: "tx-b"}}
	registered, failed := p.registerBatch(context.Background(), batch)
	if len(registered) != 2 || len(failed) != 0 {
		t.Fatalf("registered=%d failed=%d want 2/0", len(registered), len(failed))
	}
	if got := testutil.ToFloat64(metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok")) - before; got != 1 {
		t.Fatalf("fully_ok delta=%v want 1", got)
	}
	if log.count("register:") != 2 {
		t.Fatalf("live endpoint registrations=%d want 2", log.count("register:"))
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.merkleMarks) != 1 || len(ms.merkleMarks[0]) != 2 {
		t.Fatalf("merkle_registered_at must be stamped for both txs, marks=%v", ms.merkleMarks)
	}
}

// When an endpoint's breaker closes, the recovery replay re-registers the
// window with THAT endpoint only, ignoring merkle_registered_at (the stamp is
// pool-wide and cannot say which endpoint missed the row).
func TestRunRecoveryReplay_TargetsOnlyRecoveredEndpoint(t *testing.T) {
	logA, logB := &eventLog{}, &eventLog{}
	srvA := newMerkleServer(logA, http.StatusOK)
	defer srvA.Close()
	srvB := newMerkleServer(logB, http.StatusOK)
	defer srvB.Close()

	pool := merkleservice.NewPool([]string{srvA.URL, srvB.URL}, "", 2*time.Second)
	cfg := &config.Config{CallbackURL: "http://arcade/cb", CallbackToken: "tok"}
	cfg.Propagation.MerkleConcurrency = 4
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-recent", Status: models.StatusSeenOnNetwork, MerkleRegisteredAt: time.Now()},
		{TxID: "tx-older", Status: models.StatusAcceptedByNetwork},
		{TxID: "tx-mined", Status: models.StatusMined}, // terminal: skipped
	}
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool)

	p.runRecoveryReplay(context.Background(), pool, srvB.URL, time.Now())

	if n := logB.count("register:"); n != 2 {
		t.Fatalf("recovered endpoint registrations=%d want 2 (recent stamp must NOT skip)", n)
	}
	if n := logA.count("register:"); n != 0 {
		t.Fatalf("healthy endpoint must not be re-registered, got %d", n)
	}
}

// scheduleRecoveryReplay coalesces: a second breaker-closed event for the
// same endpoint while a pass is running triggers exactly one more pass, and
// nothing runs after Stop flagged the propagator.
func TestScheduleRecoveryReplay_CoalescesAndHonorsStop(t *testing.T) {
	logB := &eventLog{}
	srvB := newMerkleServer(logB, http.StatusOK)
	defer srvB.Close()
	logA := &eventLog{}
	srvA := newMerkleServer(logA, http.StatusOK)
	defer srvA.Close()

	pool := merkleservice.NewPool([]string{srvA.URL, srvB.URL}, "", 2*time.Second)
	cfg := &config.Config{CallbackURL: "http://arcade/cb", CallbackToken: "tok"}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{{TxID: "tx-1", Status: models.StatusSeenOnNetwork}}
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool)

	ctx := context.Background()
	opened := time.Now().Add(-time.Minute)
	p.scheduleRecoveryReplay(ctx, pool, srvB.URL, opened)
	p.scheduleRecoveryReplay(ctx, pool, srvB.URL, opened)
	p.scheduleRecoveryReplay(ctx, pool, srvB.URL, opened)
	p.backgroundWG.Wait()

	// At most two passes (the running one, plus one coalesced rerun), never
	// three; at least one.
	if n := logB.count("register:"); n < 1 || n > 2 {
		t.Fatalf("recovery passes registered tx-1 %d times, want 1..2", n)
	}

	p.recoveryMu.Lock()
	p.recoveryStopped = true
	p.recoveryMu.Unlock()
	before := logB.count("register:")
	p.scheduleRecoveryReplay(ctx, pool, srvB.URL, opened)
	p.backgroundWG.Wait()
	if logB.count("register:") != before {
		t.Fatal("no recovery pass may start after Stop")
	}
}
