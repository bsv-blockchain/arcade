//go:build e2e

package harness

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestArcadeRuntime_BootsAndServesHealth is the integration smoke test
// for the full harness stack: containers (Postgres + Redpanda +
// merkle-service) + in-process libp2p host + in-process datahub +
// in-process arcade.
//
// Asserts arcade boots without error and answers /health on its
// host-bound port. Doesn't drive any tx flow yet — that's smoke_test.go.
func TestArcadeRuntime_BootsAndServesHealth(t *testing.T) {
	skipIfNoDocker(t)

	libp2pHost, err := NewLibP2PHost(t, "regtest", 0)
	if err != nil {
		t.Fatalf("libp2p host: %v", err)
	}
	t.Cleanup(func() { _ = libp2pHost.Close() })

	datahub, err := NewDatahub(t)
	if err != nil {
		t.Fatalf("datahub: %v", err)
	}

	containers := New(t, WithBootstrapPeers(libp2pHost.BootstrapMultiaddr()))

	rt := StartArcade(t, ArcadeOptions{
		MerkleServiceURL: containers.Containers.MerkleHostURL,
		DatahubURL:       datahub.HostURL(),
		LibP2PBootstrap:  libp2pHost.BootstrapMultiaddr(),
		MerkleAuthToken:  "e2e-watch-token",
		CallbackToken:    "e2e-callback-token",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, rt.BaseURL+"/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health status=%d body=%s", resp.StatusCode, body)
	}
	t.Logf("arcade /health: %s", body)
}
