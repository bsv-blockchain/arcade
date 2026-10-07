package sse

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/services/servicetest"
)

func newReadinessService(t *testing.T, port int, publisher events.Publisher) *Service {
	t.Helper()
	cfg := &config.Config{}
	cfg.SSE.Enabled = true
	cfg.SSE.Host = "127.0.0.1"
	cfg.SSE.Port = port
	cfg.SSE.DrainQuiesceMS = 1
	cfg.SSE.ShutdownTimeoutMS = 500
	svc := New(cfg, zap.NewNop(), publisher, &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	})
	if svc == nil {
		t.Fatal("New returned nil")
	}
	return svc
}

func newMemoryPublisher(t *testing.T) events.Publisher {
	t.Helper()
	broker := kafka.NewMemoryBroker(8)
	t.Cleanup(func() { _ = broker.Close() })
	return events.NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop(), 0)
}

func TestService_StartSignalsReadyAfterListen(t *testing.T) {
	port := servicetest.FreePort(t)
	svc := newReadinessService(t, port, newMemoryPublisher(t))
	servicetest.AssertSignalsAfterBind(t, fmt.Sprintf("127.0.0.1:%d", port), svc.Start, svc.NotifyReady)
}

func TestService_StartListenFailureDoesNotSignal(t *testing.T) {
	svc := newReadinessService(t, -1, newMemoryPublisher(t))
	servicetest.AssertStartFailureSilent(t, svc.Start, svc.NotifyReady)
}

// TestService_SubscribeErrorDoesNotSignal covers the fallible step before
// the bind: newManager subscribes to the events publisher, and a failure
// there must leave readiness unsignaled.
func TestService_SubscribeErrorDoesNotSignal(t *testing.T) {
	svc := newReadinessService(t, servicetest.FreePort(t), subscribeErrPublisher{})
	servicetest.AssertStartFailureSilent(t, svc.Start, svc.NotifyReady)
}

type subscribeErrPublisher struct{}

func (subscribeErrPublisher) Publish(context.Context, *models.TransactionStatus) error {
	return nil
}

func (subscribeErrPublisher) PublishBulk(context.Context, *models.TransactionStatus) error {
	return nil
}

func (subscribeErrPublisher) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return nil, errors.New("broker down")
}

func (subscribeErrPublisher) Close() error { return nil }
