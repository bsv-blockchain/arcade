package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestObserveStatusClass — the helper is used everywhere HTTP latency lands
// in a histogram. Its bucket assignments are load-bearing for dashboards, so
// regressions here change the meaning of every existing alert.
func TestObserveStatusClass(t *testing.T) {
	cases := []struct {
		code int
		want string
	}{
		{0, "transport_error"},
		{200, "2xx"},
		{204, "2xx"},
		{299, "2xx"},
		{301, "3xx"},
		{400, "4xx"},
		{404, "4xx"},
		{500, "5xx"},
		{503, "5xx"},
		{599, "5xx"},
		{100, "other"},
		{600, "other"},
	}
	for _, tc := range cases {
		if got := ObserveStatusClass(tc.code); got != tc.want {
			t.Errorf("ObserveStatusClass(%d) = %q, want %q", tc.code, got, tc.want)
		}
	}
}

// TestMetricsHandlerScrapes — wire up the prometheus default-registry HTTP
// handler the way the health server does, hit it, and confirm we get back a
// payload that looks like a Prometheus exposition. This is the contract a
// scrape config will rely on.
func TestMetricsHandlerScrapes(t *testing.T) {
	// Touch a couple of metrics so they actually exist on the registry.
	TxValidatorPendingDepth.Set(7)
	TxValidatorOutcomeTotal.WithLabelValues("accepted").Inc()
	BumpBuilderBlocksProcessedTotal.Inc()
	PropagationOutcomeTotal.WithLabelValues("accepted").Add(3)

	srv := httptest.NewServer(promhttp.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape returned %d", resp.StatusCode)
	}

	body := readAll(t, resp)
	for _, expected := range []string{
		"arcade_tx_validator_pending_depth 7",
		"arcade_tx_validator_outcome_total{outcome=\"accepted\"}",
		"arcade_bump_builder_blocks_processed_total",
		"arcade_propagation_outcome_total{outcome=\"accepted\"}",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("scrape output missing %q\n--- body ---\n%s", expected, body)
		}
	}
}

// TestCounterIncrementsAreObservable — touching a counter must show up via
// testutil's CollectAndCount/ToFloat64 helpers, which is how alert/dashboard
// tests would observe it.
func TestCounterIncrementsAreObservable(t *testing.T) {
	before := testutil.ToFloat64(P2PNodeStatusMessagesTotal)
	P2PNodeStatusMessagesTotal.Inc()
	P2PNodeStatusMessagesTotal.Inc()
	after := testutil.ToFloat64(P2PNodeStatusMessagesTotal)
	if after-before != 2 {
		t.Errorf("counter delta = %v, want 2", after-before)
	}
}

// TestTeranodeHealthGaugeRoundTrips — gauges must read back the value they
// were set to, including across the per-endpoint label combinatorics that the
// dashboards iterate.
func TestTeranodeHealthGaugeRoundTrips(t *testing.T) {
	TeranodeEndpointHealth.WithLabelValues("https://datahub-1.example", "configured").Set(1)
	TeranodeEndpointHealth.WithLabelValues("https://datahub-2.example", "discovered").Set(0)

	if v := testutil.ToFloat64(TeranodeEndpointHealth.WithLabelValues("https://datahub-1.example", "configured")); v != 1 {
		t.Errorf("expected 1 for healthy datahub, got %v", v)
	}
	if v := testutil.ToFloat64(TeranodeEndpointHealth.WithLabelValues("https://datahub-2.example", "discovered")); v != 0 {
		t.Errorf("expected 0 for unhealthy datahub, got %v", v)
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 1024)
	for {
		n, err := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}
	return string(buf)
}
