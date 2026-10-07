package api_server

import (
	"fmt"
	"testing"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/services/servicetest"
)

func newReadinessServer(port int) *Server {
	cfg := &config.Config{}
	cfg.APIServer.Host = "127.0.0.1"
	cfg.APIServer.Port = port
	cfg.Telemetry.ServiceName = "arcade-test"
	return New(cfg, zap.NewNop(), nil, nil, nil, nil, nil, nil, nil, nil)
}

func TestServer_StartSignalsReadyAfterListen(t *testing.T) {
	port := servicetest.FreePort(t)
	srv := newReadinessServer(port)
	servicetest.AssertSignalsAfterBind(t, fmt.Sprintf("127.0.0.1:%d", port), srv.Start, srv.NotifyReady)
}

func TestServer_StartListenFailureDoesNotSignal(t *testing.T) {
	srv := newReadinessServer(-1)
	servicetest.AssertStartFailureSilent(t, srv.Start, srv.NotifyReady)
}
