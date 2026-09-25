package app

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

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
