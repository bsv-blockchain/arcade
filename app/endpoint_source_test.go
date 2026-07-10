package app

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/bsv-blockchain/arcade/store"
)

// fakeDatahubLister returns a fixed endpoint set, mimicking the registry.
type fakeDatahubLister struct {
	eps []store.DatahubEndpoint
}

func (f *fakeDatahubLister) ListDatahubEndpoints(_ context.Context, _ string) ([]store.DatahubEndpoint, error) {
	return f.eps, nil
}

func configured(url string) store.DatahubEndpoint {
	return store.DatahubEndpoint{URL: url, Network: "mainnet", Source: store.DatahubEndpointSourceConfigured}
}

func discovered(url string) store.DatahubEndpoint {
	return store.DatahubEndpoint{URL: url, Network: "mainnet", Source: store.DatahubEndpointSourceDiscovered}
}

// TestEndpointSource_ConfiguredRowsBypassValidation: operator-configured
// URLs may legitimately point at cluster-internal names; they must pass
// through without any DNS traffic.
func TestEndpointSource_ConfiguredRowsBypassValidation(t *testing.T) {
	src := newEndpointSource(&fakeDatahubLister{eps: []store.DatahubEndpoint{
		configured("http://teranode.internal.svc:8090/api/v1"),
	}}, "mainnet", true, false, zaptest.NewLogger(t))
	src.lookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		t.Fatalf("configured row triggered DNS lookup for %q", host)
		return nil, nil
	}

	urls, err := src.ListEndpointURLs(context.Background())
	if err != nil {
		t.Fatalf("ListEndpointURLs: %v", err)
	}
	if len(urls) != 1 || urls[0] != "http://teranode.internal.svc:8090/api/v1" {
		t.Fatalf("configured row filtered: %v", urls)
	}
}

// TestEndpointSource_DiscoveredUnresolvableFiltered: the poisoned-registry
// case — a discovered row whose hostname doesn't resolve (asset:8090) must
// be dropped at refresh time in every pod.
func TestEndpointSource_DiscoveredUnresolvableFiltered(t *testing.T) {
	src := newEndpointSource(&fakeDatahubLister{eps: []store.DatahubEndpoint{
		discovered("http://asset:8090/api/v1"),
		discovered("https://peer.example/api/v1"),
	}}, "mainnet", true, false, zaptest.NewLogger(t))
	src.lookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		if host == "asset" {
			return nil, fmt.Errorf("lookup asset: no such host")
		}
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}

	urls, err := src.ListEndpointURLs(context.Background())
	if err != nil {
		t.Fatalf("ListEndpointURLs: %v", err)
	}
	if len(urls) != 1 || urls[0] != "https://peer.example/api/v1" {
		t.Fatalf("expected only the resolvable discovered row, got %v", urls)
	}
}

// TestEndpointSource_ValidationCached: a discovered row that validates is
// not re-resolved on the next listing (success-only TTL cache).
func TestEndpointSource_ValidationCached(t *testing.T) {
	src := newEndpointSource(&fakeDatahubLister{eps: []store.DatahubEndpoint{
		discovered("https://peer.example/api/v1"),
	}}, "mainnet", true, false, zaptest.NewLogger(t))
	var lookups atomic.Int64
	src.lookupIP = func(_ context.Context, _ string) ([]net.IP, error) {
		lookups.Add(1)
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}

	for i := 0; i < 3; i++ {
		if _, err := src.ListEndpointURLs(context.Background()); err != nil {
			t.Fatalf("ListEndpointURLs: %v", err)
		}
	}
	if got := lookups.Load(); got != 1 {
		t.Fatalf("expected 1 lookup across 3 listings, got %d", got)
	}
}

// TestEndpointSource_RejectionNotCached: a row rejected while unresolvable
// is admitted as soon as its DNS recovers — rejections are never cached.
func TestEndpointSource_RejectionNotCached(t *testing.T) {
	src := newEndpointSource(&fakeDatahubLister{eps: []store.DatahubEndpoint{
		discovered("https://flaky.example/api/v1"),
	}}, "mainnet", true, false, zaptest.NewLogger(t))
	var resolvable atomic.Bool
	src.lookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		if !resolvable.Load() {
			return nil, fmt.Errorf("lookup %s: no such host", host)
		}
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}

	urls, _ := src.ListEndpointURLs(context.Background())
	if len(urls) != 0 {
		t.Fatalf("unresolvable row admitted: %v", urls)
	}
	resolvable.Store(true)
	urls, _ = src.ListEndpointURLs(context.Background())
	if len(urls) != 1 {
		t.Fatalf("recovered row still filtered: %v", urls)
	}
}

// TestEndpointSource_AllowPrivateAdmitsPrivateResolution: with
// p2p.allow_private_urls=true (local/standalone deployments), a discovered
// row resolving to RFC1918 is admitted — but an unresolvable one still isn't.
func TestEndpointSource_AllowPrivateAdmitsPrivateResolution(t *testing.T) {
	src := newEndpointSource(&fakeDatahubLister{eps: []store.DatahubEndpoint{
		discovered("http://teranode.local:8090/api/v1"),
		discovered("http://asset:8090/api/v1"),
	}}, "mainnet", true, true, zaptest.NewLogger(t))
	src.lookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		if host == "asset" {
			return nil, fmt.Errorf("lookup asset: no such host")
		}
		return []net.IP{net.ParseIP("10.0.0.5")}, nil
	}

	urls, err := src.ListEndpointURLs(context.Background())
	if err != nil {
		t.Fatalf("ListEndpointURLs: %v", err)
	}
	if len(urls) != 1 || urls[0] != "http://teranode.local:8090/api/v1" {
		t.Fatalf("expected privately-resolving row only, got %v", urls)
	}
}

// TestEndpointSource_DiscoveredExcludedWhenDiscoveryOff pins the existing
// includeDiscovered toggle alongside the new validation path.
func TestEndpointSource_DiscoveredExcludedWhenDiscoveryOff(t *testing.T) {
	src := newEndpointSource(&fakeDatahubLister{eps: []store.DatahubEndpoint{
		configured("https://seed.example/api/v1"),
		discovered("https://peer.example/api/v1"),
	}}, "mainnet", false, false, zaptest.NewLogger(t))
	src.lookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		t.Fatalf("lookup should not run when discovery is off, got %q", host)
		return nil, nil
	}

	urls, err := src.ListEndpointURLs(context.Background())
	if err != nil {
		t.Fatalf("ListEndpointURLs: %v", err)
	}
	if len(urls) != 1 || urls[0] != "https://seed.example/api/v1" {
		t.Fatalf("expected configured row only, got %v", urls)
	}
}
