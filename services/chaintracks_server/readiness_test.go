package chaintracks_server

import (
	"fmt"
	"testing"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/services/servicetest"
)

func newReadinessService(t *testing.T, port int) *Service {
	t.Helper()
	cfg := &config.Config{}
	cfg.ChaintracksServer.Enabled = true
	cfg.ChaintracksServer.Host = "127.0.0.1"
	cfg.ChaintracksServer.Port = port
	svc := New(cfg, zap.NewNop(), nil, newFakeChaintracks())
	if svc == nil {
		t.Fatal("New returned nil")
	}
	return svc
}

func TestService_StartSignalsReadyAfterListen(t *testing.T) {
	port := servicetest.FreePort(t)
	svc := newReadinessService(t, port)
	servicetest.AssertSignalsAfterBind(t, fmt.Sprintf("127.0.0.1:%d", port), svc.Start, svc.NotifyReady)
}

func TestService_StartListenFailureDoesNotSignal(t *testing.T) {
	svc := newReadinessService(t, -1)
	servicetest.AssertStartFailureSilent(t, svc.Start, svc.NotifyReady)
}
