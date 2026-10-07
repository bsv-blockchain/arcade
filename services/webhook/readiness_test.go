package webhook

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

func TestService_StartSignalsReadyAfterSubscribe(t *testing.T) {
	pub := &scriptedPub{ch: make(chan *models.TransactionStatus)}
	svc := New(config.WebhookConfig{MaxConcurrentDeliveries: 1}, config.CallbackConfig{}, zap.NewNop(), pub, nil, nil)

	ready := make(chan struct{})
	svc.NotifyReady(func() { close(ready) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- svc.Start(ctx) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for startup readiness")
	}
	select {
	case err := <-errCh:
		t.Fatalf("Start returned before cancel: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestService_SubscribeErrorDoesNotSignal(t *testing.T) {
	svc := New(config.WebhookConfig{}, config.CallbackConfig{}, zap.NewNop(), errPublisher{}, nil, nil)
	signaled := false
	svc.NotifyReady(func() { signaled = true })
	err := svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected subscribe error")
	}
	if signaled {
		t.Fatal("readiness signaled after subscribe failed")
	}
}

type errPublisher struct{}

func (errPublisher) Publish(context.Context, *models.TransactionStatus) error { return nil }
func (errPublisher) PublishBulk(context.Context, *models.TransactionStatus) error {
	return nil
}

func (errPublisher) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return nil, errors.New("broker down")
}
func (errPublisher) Close() error { return nil }
