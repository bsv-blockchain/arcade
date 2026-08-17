//go:build postgres

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/store"
)

const ppNetMainnet = "mainnet"

func TestPeerPolicies_UpsertAndList(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	in := []store.PeerPolicy{
		{PeerID: "peer-a", Network: ppNetMainnet, MiningFeeSatoshis: 100, MiningFeeBytes: 1000,
			MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000, LastSeen: now},
		// peer-b advertises no size limits, as a teranode predating fee_policy
		// does — and as any row written before the columns existed reads back,
		// since they default to 0. The zeros must survive the round trip: they
		// are the "did not advertise" sentinel readers skip.
		{PeerID: "peer-b", Network: ppNetMainnet, MiningFeeSatoshis: 50, MiningFeeBytes: 1000, LastSeen: now.Add(time.Minute)},
	}
	for _, pp := range in {
		if err := s.UpsertPeerPolicy(ctx, pp); err != nil {
			t.Fatalf("upsert %s: %v", pp.PeerID, err)
		}
	}

	out, err := s.ListPeerPolicies(ctx, ppNetMainnet)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 policies, got %d: %+v", len(out), out)
	}
	got := map[string]store.PeerPolicy{}
	for _, pp := range out {
		got[pp.PeerID] = pp
	}
	for _, want := range in {
		g, ok := got[want.PeerID]
		if !ok {
			t.Fatalf("missing peer %s", want.PeerID)
		}
		if g.MiningFeeSatoshis != want.MiningFeeSatoshis || g.MiningFeeBytes != want.MiningFeeBytes {
			t.Errorf("%s fee: got {%d %d} want {%d %d}", want.PeerID,
				g.MiningFeeSatoshis, g.MiningFeeBytes, want.MiningFeeSatoshis, want.MiningFeeBytes)
		}
		if g.MaxTxSizePolicy != want.MaxTxSizePolicy || g.MaxScriptSizePolicy != want.MaxScriptSizePolicy {
			t.Errorf("%s sizes: got {%d %d} want {%d %d}", want.PeerID,
				g.MaxTxSizePolicy, g.MaxScriptSizePolicy, want.MaxTxSizePolicy, want.MaxScriptSizePolicy)
		}
		if !g.LastSeen.Equal(want.LastSeen) {
			t.Errorf("%s last_seen: got %v want %v", want.PeerID, g.LastSeen, want.LastSeen)
		}
	}
}

func TestPeerPolicies_NetworkScopedAndOverwrite(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	for _, pp := range []store.PeerPolicy{
		{PeerID: "m1", Network: ppNetMainnet, MiningFeeSatoshis: 100, MiningFeeBytes: 1000, LastSeen: now},
		{PeerID: "r1", Network: "regtest", MiningFeeSatoshis: 1, MiningFeeBytes: 1000, LastSeen: now},
	} {
		if err := s.UpsertPeerPolicy(ctx, pp); err != nil {
			t.Fatalf("upsert %s: %v", pp.PeerID, err)
		}
	}

	regtest, err := s.ListPeerPolicies(ctx, "regtest")
	if err != nil {
		t.Fatalf("list regtest: %v", err)
	}
	if len(regtest) != 1 || regtest[0].PeerID != "r1" {
		t.Fatalf("regtest list: got %+v", regtest)
	}

	if upErr := s.UpsertPeerPolicy(ctx, store.PeerPolicy{
		PeerID: "m1", Network: ppNetMainnet, MiningFeeSatoshis: 42, MiningFeeBytes: 1000, LastSeen: now.Add(time.Hour),
	}); upErr != nil {
		t.Fatalf("overwrite upsert: %v", upErr)
	}
	mainnet, err := s.ListPeerPolicies(ctx, ppNetMainnet)
	if err != nil {
		t.Fatalf("list mainnet: %v", err)
	}
	if len(mainnet) != 1 || mainnet[0].MiningFeeSatoshis != 42 {
		t.Fatalf("mainnet list after overwrite: got %+v", mainnet)
	}
}

func TestPeerPolicies_RejectsEmptyPeerID(t *testing.T) {
	s := newTestStore(t)
	if err := s.UpsertPeerPolicy(context.Background(), store.PeerPolicy{Network: ppNetMainnet}); err == nil {
		t.Fatal("expected error upserting peer policy with empty peer id")
	}
}
