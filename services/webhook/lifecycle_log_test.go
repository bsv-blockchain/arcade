package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

// TestDeliver_LogsCallbackDeliveredAtInfo closes the webhook-delivery log
// gap: a successful delivery must be visible at Info (not Debug) so a
// txid/callback_url search surfaces the terminal "delivered" event without
// needing debug logging enabled. Volume stays bounded because only
// subscribed txs reach deliver at all.
func TestDeliver_LogsCallbackDeliveredAtInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID:  "sub-info-1",
				TxID:          txA,
				CallbackURL:   srv.URL,
				CallbackToken: "tok-A",
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1},
		config.CallbackConfig{AllowPrivateIPs: true},
		logger, recordingPub{}, st, nil,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      txA,
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	entries := recorded.FilterMessage("callback delivered").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'callback delivered' log line, got %d", len(entries))
	}
	if entries[0].Level != zapcore.InfoLevel {
		t.Errorf("expected Info level, got %v", entries[0].Level)
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["txid"].(string); got != txA {
		t.Errorf("txid = %q, want %q", got, txA)
	}
	if got, _ := fields["callback_url"].(string); got != srv.URL {
		t.Errorf("callback_url = %q, want %q", got, srv.URL)
	}
}
