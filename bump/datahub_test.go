package bump

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// failingDatahub returns 500 on every request so FetchBlockDataForBUMP runs
// the per-URL log path before aggregating into the final error.
func failingDatahub(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchBlockDataForBUMP_PerAttemptDebugLog(t *testing.T) {
	a := failingDatahub(t, http.StatusInternalServerError)
	b := failingDatahub(t, http.StatusServiceUnavailable)

	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	_, _, _, err := FetchBlockDataForBUMP(
		context.Background(),
		[]string{a.URL, b.URL},
		"deadbeef",
		logger,
	)
	if err == nil {
		t.Fatal("expected error when all URLs fail")
	}

	entries := recorded.FilterMessage("datahub fetch attempt").All()
	if len(entries) != 2 {
		t.Fatalf("expected 2 per-attempt log entries, got %d", len(entries))
	}

	// Each entry should carry idx, url, status, error.
	statuses := []int{http.StatusInternalServerError, http.StatusServiceUnavailable}
	for i, e := range entries {
		fields := e.ContextMap()
		if got := fields["idx"]; got != int64(i) {
			t.Errorf("entry[%d] idx: got %v want %d", i, got, i)
		}
		if got := fields["status"]; got != int64(statuses[i]) {
			t.Errorf("entry[%d] status: got %v want %d", i, got, statuses[i])
		}
		urlStr, _ := fields["url"].(string)
		if urlStr == "" {
			t.Errorf("entry[%d] url missing", i)
		}
		errStr, _ := fields["error"].(string)
		if !strings.Contains(errStr, "status") {
			t.Errorf("entry[%d] error should include status, got %q", i, errStr)
		}
	}
}

func TestFetchBlockDataForBUMP_EmptySliceFailsClearly(t *testing.T) {
	_, _, _, err := FetchBlockDataForBUMP(context.Background(), nil, "deadbeef", zap.NewNop())
	if err == nil {
		t.Fatal("expected error when no URLs are configured")
	}
	if !strings.Contains(err.Error(), "no DataHub URLs") {
		t.Errorf("expected explicit empty-list error, got %q", err.Error())
	}
}
