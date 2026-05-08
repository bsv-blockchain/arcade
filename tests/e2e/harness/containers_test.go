//go:build e2e

package harness

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// TestContainers_BootAndTearDown is the smallest possible end-to-end
// proof that the container layer wires up correctly. It boots Postgres,
// Redpanda, and merkle-service on a shared network, hits merkle-service's
// /health endpoint via the host-mapped port, and lets t.Cleanup tear
// everything down. Skipped when no container runtime is reachable.
func TestContainers_BootAndTearDown(t *testing.T) {
	skipIfNoDocker(t)

	h := New(t)

	resp, err := httpGet(h.Containers.MerkleHostURL + "/health")
	if err != nil {
		t.Fatalf("merkle /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 200 healthy or 503 degraded both prove the HTTP server is up. We
	// don't assert peer count here — that's exercised by the libp2p
	// fixture in a later step.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("unexpected /health status %d", resp.StatusCode)
	}
}

func httpGet(url string) (*http.Response, error) {
	c := &http.Client{Timeout: 5 * time.Second}
	return c.Get(url)
}

// skipIfNoDocker probes the configured docker socket and skips the test
// if no runtime is reachable. Lets the e2e suite land in a repo whose
// devs don't all have docker/podman wired up; CI runners always do.
func skipIfNoDocker(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("no container runtime reachable (NewDockerProvider): %v", err)
	}
	defer func() { _ = provider.Close() }()
	if err := provider.Health(ctx); err != nil {
		t.Skipf("container runtime not healthy: %v", err)
	}
}
