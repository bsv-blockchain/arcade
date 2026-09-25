package app

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
)

func seenAt(ep store.DatahubEndpoint, lastSeen time.Time) store.DatahubEndpoint {
	ep.LastSeen = lastSeen
	return ep
}

func publicLookup(_ context.Context, _ string) ([]net.IP, error) {
	return []net.IP{net.ParseIP("93.184.216.34")}, nil
}

// TestEndpointSource_StaleDiscoveredRowsAgedOut: a discovered URL that no peer
// has announced within the TTL is no longer handed to the teranode client
// (issue #320). Configured rows carry no heartbeat and are never aged out;
// rows with no LastSeen (written before it was tracked) are kept.
func TestEndpointSource_StaleDiscoveredRowsAgedOut(t *testing.T) {
	now := time.Date(2026, 8, 18, 1, 13, 0, 0, time.UTC)

	src := newEndpointSource(&fakeDatahubLister{eps: []store.DatahubEndpoint{
		seenAt(discovered("https://fresh.example/api/v1"), now.Add(-10*time.Second)),
		seenAt(discovered("https://bsva-ovh-teranode-eu-3.example/api/v1"), now.Add(-84*24*time.Hour)),
		seenAt(discovered("https://just-expired.example/api/v1"), now.Add(-time.Hour-time.Second)),
		seenAt(discovered("https://just-inside.example/api/v1"), now.Add(-time.Hour)),
		discovered("https://no-last-seen.example/api/v1"),
		seenAt(configured("https://seed.example/api/v1"), now.Add(-90*24*time.Hour)),
	}}, "mainnet", true, false, time.Hour, zaptest.NewLogger(t))
	src.lookupIP = publicLookup
	src.now = func() time.Time { return now }

	urls, err := src.ListEndpointURLs(context.Background())
	if err != nil {
		t.Fatalf("ListEndpointURLs: %v", err)
	}

	want := []string{
		"https://fresh.example/api/v1",
		"https://just-inside.example/api/v1",
		"https://no-last-seen.example/api/v1",
		"https://seed.example/api/v1",
	}
	if !reflect.DeepEqual(urls, want) {
		t.Fatalf("got %v, want %v", urls, want)
	}
}

// TestEndpointSource_ReannouncedRowReturns: filtering on read is self-healing —
// once a peer announces the URL again, it is listed again.
func TestEndpointSource_ReannouncedRowReturns(t *testing.T) {
	now := time.Date(2026, 8, 18, 1, 13, 0, 0, time.UTC)
	lister := &fakeDatahubLister{eps: []store.DatahubEndpoint{
		seenAt(discovered("https://peer.example/api/v1"), now.Add(-3*time.Hour)),
	}}

	src := newEndpointSource(lister, "mainnet", true, false, time.Hour, zaptest.NewLogger(t))
	src.lookupIP = publicLookup
	src.now = func() time.Time { return now }

	urls, err := src.ListEndpointURLs(context.Background())
	if err != nil {
		t.Fatalf("ListEndpointURLs: %v", err)
	}
	if len(urls) != 0 {
		t.Fatalf("stale row listed: %v", urls)
	}

	lister.eps[0].LastSeen = now.Add(-5 * time.Second)

	urls, err = src.ListEndpointURLs(context.Background())
	if err != nil {
		t.Fatalf("ListEndpointURLs: %v", err)
	}
	if len(urls) != 1 || urls[0] != "https://peer.example/api/v1" {
		t.Fatalf("re-announced row not listed: %v", urls)
	}
}

// TestEndpointSource_DiscoveredTTLDefault: an unset (zero or negative) TTL
// falls back to the documented default rather than aging out every row.
func TestEndpointSource_DiscoveredTTLDefault(t *testing.T) {
	want := time.Duration(config.DefaultEndpointHealthDiscoveredTTLMs) * time.Millisecond

	for _, ttl := range []time.Duration{0, -time.Second} {
		src := newEndpointSource(&fakeDatahubLister{}, "mainnet", true, false, ttl, zaptest.NewLogger(t))
		if src.discoveredTTL != want {
			t.Fatalf("ttl %v: discoveredTTL = %v, want %v", ttl, src.discoveredTTL, want)
		}
	}
}

// TestEndpointSource_AgedOutRowWarnsAgainWhenReannounced: an aged-out row is
// not counted as current, so its warn-dampening entry is pruned. If the peer
// announces the same bad URL again, it is logged at WARN once more rather
// than staying at DEBUG from the earlier episode.
func TestEndpointSource_AgedOutRowWarnsAgainWhenReannounced(t *testing.T) {
	now := time.Date(2026, 8, 18, 1, 13, 0, 0, time.UTC)
	const bad = "http://asset:8090/api/v1"
	lister := &fakeDatahubLister{eps: []store.DatahubEndpoint{
		seenAt(discovered(bad), now.Add(-5*time.Second)),
	}}

	core, logs := observer.New(zapcore.DebugLevel)
	src := newEndpointSource(lister, "mainnet", true, false, time.Hour, zap.New(core))
	src.now = func() time.Time { return now }
	src.lookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		return nil, fmt.Errorf("lookup %s: no such host", host)
	}

	levels := func() []zapcore.Level {
		var out []zapcore.Level
		for _, e := range logs.TakeAll() {
			out = append(out, e.Level)
		}
		return out
	}
	list := func() {
		t.Helper()
		if _, err := src.ListEndpointURLs(context.Background()); err != nil {
			t.Fatalf("ListEndpointURLs: %v", err)
		}
	}

	list()
	list()
	if got := levels(); !reflect.DeepEqual(got, []zapcore.Level{zapcore.WarnLevel, zapcore.DebugLevel}) {
		t.Fatalf("first episode: got %v, want WARN then DEBUG", got)
	}

	lister.eps[0].LastSeen = now.Add(-2 * time.Hour) // peer went quiet: aged out
	list()
	if got := levels(); len(got) != 0 {
		t.Fatalf("aged-out row was still checked: %v", got)
	}

	lister.eps[0].LastSeen = now.Add(-5 * time.Second) // announced again, still bad
	list()
	if got := levels(); !reflect.DeepEqual(got, []zapcore.Level{zapcore.WarnLevel}) {
		t.Fatalf("re-announced bad row: got %v, want a fresh WARN", got)
	}
}
