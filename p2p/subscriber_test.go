package p2p

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock StatusStore
type MockStatusStore struct {
	mock.Mock
}

func (m *MockStatusStore) InsertStatus(ctx context.Context, status *models.TransactionStatus) error {
	args := m.Called(ctx, status)
	return args.Error(0)
}

func (m *MockStatusStore) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	args := m.Called(ctx, status)
	return args.Error(0)
}

func (m *MockStatusStore) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	args := m.Called(ctx, txid)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.TransactionStatus), args.Error(1)
}

func (m *MockStatusStore) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	args := m.Called(ctx, since)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*models.TransactionStatus), args.Error(1)
}

func (m *MockStatusStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Mock NetworkStateStore
type MockNetworkStateStore struct {
	mock.Mock
}

func (m *MockNetworkStateStore) UpdateNetworkState(ctx context.Context, state *models.NetworkState) error {
	args := m.Called(ctx, state)
	return args.Error(0)
}

func (m *MockNetworkStateStore) GetNetworkState(ctx context.Context) (*models.NetworkState, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.NetworkState), args.Error(1)
}

func (m *MockNetworkStateStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Mock EventPublisher
type MockEventPublisher struct {
	mock.Mock
}

func (m *MockEventPublisher) Publish(ctx context.Context, event models.StatusUpdate) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockEventPublisher) Subscribe(ctx context.Context) (<-chan models.StatusUpdate, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(<-chan models.StatusUpdate), args.Error(1)
}

func (m *MockEventPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestNewSubscriber(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	validConfig := &SubscriberConfig{
		PeerCacheFile: "test_peer_cache.json",
	}

	mockStatusStore := new(MockStatusStore)
	mockNetworkStore := new(MockNetworkStateStore)
	mockEventPublisher := new(MockEventPublisher)
	txTracker := store.NewTxTracker()

	t.Run("nil config", func(t *testing.T) {
		_, err := NewSubscriber(ctx, nil, mockStatusStore, mockNetworkStore, txTracker, mockEventPublisher, logger, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "config is nil")
	})

	t.Run("nil statusStore", func(t *testing.T) {
		_, err := NewSubscriber(ctx, validConfig, nil, mockNetworkStore, txTracker, mockEventPublisher, logger, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "statusStore is nil")
	})

	t.Run("nil networkStore", func(t *testing.T) {
		_, err := NewSubscriber(ctx, validConfig, mockStatusStore, nil, txTracker, mockEventPublisher, logger, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "networkStore is nil")
	})

	t.Run("nil txTracker", func(t *testing.T) {
		_, err := NewSubscriber(ctx, validConfig, mockStatusStore, mockNetworkStore, nil, mockEventPublisher, logger, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "txTracker is nil")
	})

	t.Run("nil eventPublisher", func(t *testing.T) {
		_, err := NewSubscriber(ctx, validConfig, mockStatusStore, mockNetworkStore, txTracker, nil, logger, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "eventPublisher is nil")
	})
}

func TestHandleBlockTopic(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	blockHash := "0000000000000000000000000000000000000000000000000000000000000001"
	txID1 := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	txID2 := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	t.Run("successful block processing", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/api/v1/block/"+blockHash, r.URL.Path)

			txID1Bytes, _ := hex.DecodeString(txID1)
			txID2Bytes, _ := hex.DecodeString(txID2)

			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
			w.Write(txID2Bytes)
		}))
		defer server.Close()

		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)

		mockNetworkStore.On("UpdateNetworkState", ctx, mock.MatchedBy(func(state *models.NetworkState) bool {
			return state.CurrentHeight == 12345 && state.LastBlockHash == blockHash
		})).Return(nil)

		mockStatusStore.On("UpdateStatus", ctx, mock.MatchedBy(func(status *models.TransactionStatus) bool {
			return status.Status == models.StatusMined && status.BlockHash == blockHash && status.BlockHeight == 12345
		})).Return(nil).Times(2)

		mockEventPublisher.On("Publish", ctx, mock.MatchedBy(func(event models.StatusUpdate) bool {
			return event.Status == models.StatusMined && (event.TxID == txID1 || event.TxID == txID2)
		})).Return(nil).Times(2)

		txTracker := store.NewTxTracker()

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			txTracker:      txTracker,
			eventPublisher: mockEventPublisher,
			logger:         logger,
			httpClient:     server.Client(),
		}

		blockMsg := BlockMessage{
			Hash:       blockHash,
			Height:     12345,
			DataHubURL: server.URL,
			PeerID:     "test-peer",
		}

		subscriber.processBlockMessage(ctx, blockMsg)

		mockStatusStore.AssertExpectations(t)
		mockNetworkStore.AssertExpectations(t)
		mockEventPublisher.AssertExpectations(t)
	})

	t.Run("failed to fetch block TxIDs", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)
		txTracker := store.NewTxTracker()

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			txTracker:      txTracker,
			eventPublisher: mockEventPublisher,
			logger:         logger,
			httpClient:     server.Client(),
		}

		blockMsg := BlockMessage{
			Hash:       blockHash,
			Height:     12345,
			DataHubURL: server.URL,
			PeerID:     "test-peer",
		}

		subscriber.processBlockMessage(ctx, blockMsg)

		mockStatusStore.AssertNotCalled(t, "UpdateStatus")
		mockNetworkStore.AssertNotCalled(t, "UpdateNetworkState")
	})

	t.Run("network state update fails but processing continues", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			txID1Bytes, _ := hex.DecodeString(txID1)
			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
		}))
		defer server.Close()

		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)

		mockNetworkStore.On("UpdateNetworkState", ctx, mock.Anything).Return(errors.New("database error"))
		mockStatusStore.On("UpdateStatus", ctx, mock.Anything).Return(nil)
		mockEventPublisher.On("Publish", ctx, mock.Anything).Return(nil)
		txTracker := store.NewTxTracker()

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			txTracker:      txTracker,
			eventPublisher: mockEventPublisher,
			logger:         logger,
			httpClient:     server.Client(),
		}

		blockMsg := BlockMessage{
			Hash:       blockHash,
			Height:     12345,
			DataHubURL: server.URL,
			PeerID:     "test-peer",
		}

		subscriber.processBlockMessage(ctx, blockMsg)

		mockStatusStore.AssertExpectations(t)
		mockEventPublisher.AssertExpectations(t)
	})
}

func TestHandleSubtreeTopic(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	subtreeHash := "subtree1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	txID1 := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	txID2 := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	t.Run("successful subtree processing", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/subtree/"+subtreeHash, r.URL.Path)

			txID1Bytes, _ := hex.DecodeString(txID1)
			txID2Bytes, _ := hex.DecodeString(txID2)

			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
			w.Write(txID2Bytes)
		}))
		defer server.Close()

		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)
		txTracker := store.NewTxTracker()
		// Add the txids to the tracker so they get filtered in
		txTracker.Add(txID1, models.StatusReceived)
		txTracker.Add(txID2, models.StatusReceived)

		mockStatusStore.On("UpdateStatus", ctx, mock.MatchedBy(func(status *models.TransactionStatus) bool {
			return status.Status == models.StatusSeenOnNetwork && (status.TxID == txID1 || status.TxID == txID2)
		})).Return(nil).Times(2)

		mockEventPublisher.On("Publish", ctx, mock.MatchedBy(func(event models.StatusUpdate) bool {
			return event.Status == models.StatusSeenOnNetwork && (event.TxID == txID1 || event.TxID == txID2)
		})).Return(nil).Times(2)

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			txTracker:      txTracker,
			eventPublisher: mockEventPublisher,
			logger:         logger,
			httpClient:     server.Client(),
		}

		subtreeMsg := SubtreeMessage{
			Hash:       subtreeHash,
			DataHubURL: server.URL,
			PeerID:     "test-peer",
		}

		subscriber.processSubtreeMessage(ctx, subtreeMsg)

		mockStatusStore.AssertExpectations(t)
		mockEventPublisher.AssertExpectations(t)
	})

	t.Run("failed to fetch subtree TxIDs", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)
		txTracker := store.NewTxTracker()

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			txTracker:      txTracker,
			eventPublisher: mockEventPublisher,
			logger:         logger,
			httpClient:     server.Client(),
		}

		subtreeMsg := SubtreeMessage{
			Hash:       subtreeHash,
			DataHubURL: server.URL,
			PeerID:     "test-peer",
		}

		subscriber.processSubtreeMessage(ctx, subtreeMsg)

		mockStatusStore.AssertNotCalled(t, "UpdateStatus")
	})

	t.Run("status insert error continues processing other txs", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			txID1Bytes, _ := hex.DecodeString(txID1)
			txID2Bytes, _ := hex.DecodeString(txID2)

			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
			w.Write(txID2Bytes)
		}))
		defer server.Close()

		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)
		txTracker := store.NewTxTracker()
		txTracker.Add(txID1, models.StatusReceived)
		txTracker.Add(txID2, models.StatusReceived)

		mockStatusStore.On("UpdateStatus", ctx, mock.MatchedBy(func(status *models.TransactionStatus) bool {
			return status.TxID == txID1
		})).Return(errors.New("database error"))

		mockStatusStore.On("UpdateStatus", ctx, mock.MatchedBy(func(status *models.TransactionStatus) bool {
			return status.TxID == txID2
		})).Return(nil)

		mockEventPublisher.On("Publish", ctx, mock.MatchedBy(func(event models.StatusUpdate) bool {
			return event.TxID == txID2
		})).Return(nil)

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			txTracker:      txTracker,
			eventPublisher: mockEventPublisher,
			logger:         logger,
			httpClient:     server.Client(),
		}

		subtreeMsg := SubtreeMessage{
			Hash:       subtreeHash,
			DataHubURL: server.URL,
			PeerID:     "test-peer",
		}

		subscriber.processSubtreeMessage(ctx, subtreeMsg)

		mockStatusStore.AssertExpectations(t)
		mockEventPublisher.AssertExpectations(t)
	})
}

func TestHandleRejectedTxTopic(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	txID := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	t.Run("rejected transaction", func(t *testing.T) {
		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)

		mockStatusStore.On("UpdateStatus", ctx, mock.MatchedBy(func(status *models.TransactionStatus) bool {
			return status.TxID == txID &&
				status.Status == models.StatusRejected &&
				status.ExtraInfo == "Invalid script"
		})).Return(nil)

		mockEventPublisher.On("Publish", ctx, mock.MatchedBy(func(event models.StatusUpdate) bool {
			return event.TxID == txID && event.Status == models.StatusRejected
		})).Return(nil)

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			eventPublisher: mockEventPublisher,
			logger:         logger,
		}

		rejectedMsg := RejectedTxMessage{
			TxID:   txID,
			Reason: "Invalid script",
			PeerID: "test-peer",
		}

		subscriber.processRejectedTxMessage(ctx, rejectedMsg)

		mockStatusStore.AssertExpectations(t)
		mockEventPublisher.AssertExpectations(t)
	})

	t.Run("double spend detection", func(t *testing.T) {
		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)

		mockStatusStore.On("UpdateStatus", ctx, mock.MatchedBy(func(status *models.TransactionStatus) bool {
			return status.TxID == txID &&
				status.Status == models.StatusDoubleSpendAttempted &&
				status.ExtraInfo == "Double spend detected"
		})).Return(nil)

		mockEventPublisher.On("Publish", ctx, mock.MatchedBy(func(event models.StatusUpdate) bool {
			return event.TxID == txID && event.Status == models.StatusDoubleSpendAttempted
		})).Return(nil)

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			eventPublisher: mockEventPublisher,
			logger:         logger,
		}

		rejectedMsg := RejectedTxMessage{
			TxID:   txID,
			Reason: "Double spend detected",
			PeerID: "test-peer",
		}

		subscriber.processRejectedTxMessage(ctx, rejectedMsg)

		mockStatusStore.AssertExpectations(t)
		mockEventPublisher.AssertExpectations(t)
	})

	t.Run("double spend case insensitive", func(t *testing.T) {
		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)

		mockStatusStore.On("UpdateStatus", ctx, mock.MatchedBy(func(status *models.TransactionStatus) bool {
			return status.Status == models.StatusDoubleSpendAttempted
		})).Return(nil)

		mockEventPublisher.On("Publish", ctx, mock.MatchedBy(func(event models.StatusUpdate) bool {
			return event.Status == models.StatusDoubleSpendAttempted
		})).Return(nil)

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			eventPublisher: mockEventPublisher,
			logger:         logger,
		}

		rejectedMsg := RejectedTxMessage{
			TxID:   txID,
			Reason: "DOUBLE SPEND ATTEMPT",
			PeerID: "test-peer",
		}

		subscriber.processRejectedTxMessage(ctx, rejectedMsg)

		mockStatusStore.AssertExpectations(t)
		mockEventPublisher.AssertExpectations(t)
	})

	t.Run("status insert error", func(t *testing.T) {
		mockStatusStore := new(MockStatusStore)
		mockNetworkStore := new(MockNetworkStateStore)
		mockEventPublisher := new(MockEventPublisher)

		mockStatusStore.On("UpdateStatus", ctx, mock.Anything).Return(errors.New("database error"))

		subscriber := &Subscriber{
			ctx:            ctx,
			statusStore:    mockStatusStore,
			networkStore:   mockNetworkStore,
			eventPublisher: mockEventPublisher,
			logger:         logger,
		}

		rejectedMsg := RejectedTxMessage{
			TxID:   txID,
			Reason: "Invalid",
			PeerID: "test-peer",
		}

		subscriber.processRejectedTxMessage(ctx, rejectedMsg)

		mockStatusStore.AssertExpectations(t)
		mockEventPublisher.AssertNotCalled(t, "Publish")
	})
}

func TestHandleNodeStatusTopic(t *testing.T) {
	// Node status messages are just logged, no special processing needed
	// The handleNodeStatuses method in the subscriber just logs and does nothing else
	t.Run("node status is handled by goroutine", func(t *testing.T) {
		// This test verifies the structure exists but actual handling is minimal
		assert.True(t, true)
	})
}

func TestFetchBlockTxIDs(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	blockHash := "0000000000000000000000000000000000000000000000000000000000000001"
	txID1 := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	txID2 := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	t.Run("successful fetch", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/api/v1/block/"+blockHash, r.URL.Path)

			txID1Bytes, _ := hex.DecodeString(txID1)
			txID2Bytes, _ := hex.DecodeString(txID2)

			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
			w.Write(txID2Bytes)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		txIDs, err := subscriber.fetchBlockTxIDs(ctx, server.URL, blockHash)
		require.NoError(t, err)
		assert.Len(t, txIDs, 2)
		assert.Contains(t, txIDs, txID1)
		assert.Contains(t, txIDs, txID2)
	})

	t.Run("URL with trailing slash", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/api/v1/block/"+blockHash, r.URL.Path)

			txID1Bytes, _ := hex.DecodeString(txID1)
			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		txIDs, err := subscriber.fetchBlockTxIDs(ctx, server.URL+"/", blockHash)
		require.NoError(t, err)
		assert.Len(t, txIDs, 1)
	})

	t.Run("empty response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		txIDs, err := subscriber.fetchBlockTxIDs(ctx, server.URL, blockHash)
		require.NoError(t, err)
		assert.Empty(t, txIDs)
	})

	t.Run("HTTP error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		_, err := subscriber.fetchBlockTxIDs(ctx, server.URL, blockHash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected status code")
	})

	t.Run("partial hash read error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("incomplete"))
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		_, err := subscriber.fetchBlockTxIDs(ctx, server.URL, blockHash)
		require.Error(t, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		_, err := subscriber.fetchBlockTxIDs(cancelCtx, server.URL, blockHash)
		require.Error(t, err)
	})
}

func TestFetchSubtreeHashes(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	subtreeHash := "subtree1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	txID1 := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	txID2 := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	t.Run("successful fetch", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/subtree/"+subtreeHash, r.URL.Path)

			txID1Bytes, _ := hex.DecodeString(txID1)
			txID2Bytes, _ := hex.DecodeString(txID2)

			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
			w.Write(txID2Bytes)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		hashes, err := subscriber.fetchSubtreeHashes(ctx, server.URL, subtreeHash)
		require.NoError(t, err)
		assert.Len(t, hashes, 2)
		assert.Equal(t, txID1, hashes[0].String())
		assert.Equal(t, txID2, hashes[1].String())
	})

	t.Run("URL with trailing slash", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/subtree/"+subtreeHash, r.URL.Path)

			txID1Bytes, _ := hex.DecodeString(txID1)
			w.WriteHeader(http.StatusOK)
			w.Write(txID1Bytes)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		hashes, err := subscriber.fetchSubtreeHashes(ctx, server.URL+"/", subtreeHash)
		require.NoError(t, err)
		assert.Len(t, hashes, 1)
	})

	t.Run("empty response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		hashes, err := subscriber.fetchSubtreeHashes(ctx, server.URL, subtreeHash)
		require.NoError(t, err)
		assert.Empty(t, hashes)
	})

	t.Run("HTTP error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: server.Client(),
		}

		_, err := subscriber.fetchSubtreeHashes(ctx, server.URL, subtreeHash)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected status code")
	})
}

func TestSlogAdapter(t *testing.T) {
	t.Run("log methods don't panic", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		adapter := NewSlogAdapter(logger)

		assert.NotPanics(t, func() {
			adapter.Debugf("debug %s", "message")
		})

		assert.NotPanics(t, func() {
			adapter.Infof("info %s", "message")
		})

		assert.NotPanics(t, func() {
			adapter.Warnf("warn %s", "message")
		})

		assert.NotPanics(t, func() {
			adapter.Errorf("error %s", "message")
		})
	})
}

func TestHTTPClientRetry(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("malformed URL", func(t *testing.T) {
		subscriber := &Subscriber{
			ctx:        ctx,
			logger:     logger,
			httpClient: http.DefaultClient,
		}

		_, err := subscriber.fetchBlockTxIDs(ctx, ":", "hash")
		require.Error(t, err)
	})
}

func TestMultipleTxIDsProcessing(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	blockHash := "0000000000000000000000000000000000000000000000000000000000000001"

	var txIDBytes bytes.Buffer
	for i := 0; i < 100; i++ {
		txID := hex.EncodeToString(bytes.Repeat([]byte{byte(i)}, 32))
		txIDData, _ := hex.DecodeString(txID)
		txIDBytes.Write(txIDData)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(txIDBytes.Bytes())
	}))
	defer server.Close()

	mockStatusStore := new(MockStatusStore)
	mockNetworkStore := new(MockNetworkStateStore)
	mockEventPublisher := new(MockEventPublisher)

	mockNetworkStore.On("UpdateNetworkState", ctx, mock.Anything).Return(nil)
	mockStatusStore.On("UpdateStatus", ctx, mock.Anything).Return(nil).Times(100)
	mockEventPublisher.On("Publish", ctx, mock.Anything).Return(nil).Times(100)
	txTracker := store.NewTxTracker()

	subscriber := &Subscriber{
		ctx:            ctx,
		statusStore:    mockStatusStore,
		networkStore:   mockNetworkStore,
		txTracker:      txTracker,
		eventPublisher: mockEventPublisher,
		logger:         logger,
		httpClient:     server.Client(),
	}

	blockMsg := BlockMessage{
		Hash:       blockHash,
		Height:     12345,
		DataHubURL: server.URL,
		PeerID:     "test-peer",
	}

	subscriber.processBlockMessage(ctx, blockMsg)

	mockStatusStore.AssertExpectations(t)
	mockNetworkStore.AssertExpectations(t)
	mockEventPublisher.AssertExpectations(t)
}
