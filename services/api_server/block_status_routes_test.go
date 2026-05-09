package api_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// blockProcStore is a tiny in-memory mock for the block-processing methods.
// Embedding mockStore picks up no-op implementations of the rest of the
// store.Store surface so we don't have to spell out every method.
type blockProcStore struct {
	mockStore

	mu               sync.Mutex
	rows             map[string]*models.BlockProcessingStatus
	processedCalls   []string
	bumpBuiltCalls   []string
	headerCalls      []string
	orphanedCalls    [][]string
	listErr          error
	getErr           error
	markProcessedErr error
}

func newBlockProcStore() *blockProcStore {
	return &blockProcStore{rows: make(map[string]*models.BlockProcessingStatus)}
}

func (s *blockProcStore) UpsertBlockHeaderSeen(_ context.Context, hash string, height uint64, seen time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headerCalls = append(s.headerCalls, hash)
	row, ok := s.rows[hash]
	if !ok {
		s.rows[hash] = &models.BlockProcessingStatus{
			BlockHash:    hash,
			BlockHeight:  height,
			HeaderSeenAt: seen,
			Status:       models.BlockStatusActive,
		}
		return nil
	}
	row.BlockHeight = height
	row.Status = models.BlockStatusActive
	row.OrphanedAt = nil
	return nil
}

func (s *blockProcStore) MarkBlockProcessed(_ context.Context, hash string, height uint64, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markProcessedErr != nil {
		return s.markProcessedErr
	}
	s.processedCalls = append(s.processedCalls, hash)
	row, ok := s.rows[hash]
	if !ok {
		t := at
		s.rows[hash] = &models.BlockProcessingStatus{
			BlockHash:    hash,
			BlockHeight:  height,
			HeaderSeenAt: at,
			ProcessedAt:  &t,
			Status:       models.BlockStatusActive,
		}
		return nil
	}
	t := at
	row.ProcessedAt = &t
	return nil
}

func (s *blockProcStore) MarkBlockBUMPBuilt(_ context.Context, hash string, height uint64, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bumpBuiltCalls = append(s.bumpBuiltCalls, hash)
	t := at
	row, ok := s.rows[hash]
	if !ok {
		s.rows[hash] = &models.BlockProcessingStatus{
			BlockHash:    hash,
			BlockHeight:  height,
			HeaderSeenAt: at,
			BUMPBuiltAt:  &t,
			Status:       models.BlockStatusActive,
		}
		return nil
	}
	row.BUMPBuiltAt = &t
	return nil
}

func (s *blockProcStore) MarkBlocksOrphaned(_ context.Context, hashes []string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]string(nil), hashes...)
	s.orphanedCalls = append(s.orphanedCalls, cp)
	for _, h := range hashes {
		if row, ok := s.rows[h]; ok {
			row.Status = models.BlockStatusOrphaned
			t := at
			row.OrphanedAt = &t
		}
	}
	return nil
}

func (s *blockProcStore) GetBlockProcessingStatus(_ context.Context, hash string) (*models.BlockProcessingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return nil, s.getErr
	}
	row, ok := s.rows[hash]
	if !ok {
		return nil, store.ErrNotFound
	}
	return row, nil
}

func (s *blockProcStore) ListBlockProcessingStatus(_ context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	rows := make([]*models.BlockProcessingStatus, 0, len(s.rows))
	for _, r := range s.rows {
		if beforeHeight > 0 && r.BlockHeight >= beforeHeight {
			continue
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].BlockHeight > rows[j].BlockHeight })
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// --- handler tests ---

func TestHandleListBlockProcessingStatus_Pagination(t *testing.T) {
	bs := newBlockProcStore()
	for i := uint64(1); i <= 75; i++ {
		_ = bs.UpsertBlockHeaderSeen(context.Background(), fmt.Sprintf("h%04d", i), i, time.Now())
	}
	_, router := setupServerWithStore(&kafka.RecordingBroker{}, &bs.mockStore)
	// setupServerWithStore wires the embedded mockStore; replace the store
	// reference to point at our real blockProcStore.
	srv, _ := setupServerWithStoreAndBlockProc(t, bs)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/blocks/processing-status?limit=20", nil)
	srv.routerForTest().ServeHTTP(rec, req)
	_ = router // silence unused
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var page1 listBlockProcessingStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &page1); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(page1.Blocks) != 20 {
		t.Fatalf("page1 len=%d want 20", len(page1.Blocks))
	}
	if page1.Blocks[0].BlockHeight != 75 {
		t.Errorf("first block height=%d want 75", page1.Blocks[0].BlockHeight)
	}
	if page1.NextCursor == nil || *page1.NextCursor != page1.Blocks[19].BlockHeight {
		t.Errorf("nextCursor=%v want %d", page1.NextCursor, page1.Blocks[19].BlockHeight)
	}

	// Walk via cursor.
	seen := 20
	cursor := *page1.NextCursor
	for {
		rec := httptest.NewRecorder()
		url := fmt.Sprintf("/api/v1/blocks/processing-status?limit=20&before-height=%d", cursor)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		srv.routerForTest().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("page status=%d body=%s", rec.Code, rec.Body.String())
		}
		var page listBlockProcessingStatusResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &page)
		seen += len(page.Blocks)
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if seen != 75 {
		t.Errorf("walked %d rows, want 75", seen)
	}
}

func TestHandleListBlockProcessingStatus_LimitClamp(t *testing.T) {
	bs := newBlockProcStore()
	for i := uint64(1); i <= 250; i++ {
		_ = bs.UpsertBlockHeaderSeen(context.Background(), fmt.Sprintf("h%04d", i), i, time.Now())
	}
	srv, _ := setupServerWithStoreAndBlockProc(t, bs)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/blocks/processing-status?limit=9999", nil)
	srv.routerForTest().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var page listBlockProcessingStatusResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if len(page.Blocks) != blockStatusListMaxLimit {
		t.Errorf("len=%d want %d (clamped)", len(page.Blocks), blockStatusListMaxLimit)
	}
}

func TestHandleListBlockProcessingStatus_BadLimit(t *testing.T) {
	bs := newBlockProcStore()
	srv, _ := setupServerWithStoreAndBlockProc(t, bs)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/blocks/processing-status?limit=-1", nil)
	srv.routerForTest().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d want 400", rec.Code)
	}
}

func TestHandleGetBlockProcessingStatus_Found(t *testing.T) {
	bs := newBlockProcStore()
	now := time.Now().UTC().Truncate(time.Millisecond)
	_ = bs.UpsertBlockHeaderSeen(context.Background(), "abc", 5, now)
	_ = bs.MarkBlockProcessed(context.Background(), "abc", 5, now.Add(time.Second))
	srv, _ := setupServerWithStoreAndBlockProc(t, bs)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/blocks/processing-status/abc", nil)
	srv.routerForTest().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp blockProcessingStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.BlockHash != "abc" || resp.BlockHeight != 5 {
		t.Errorf("got %+v", resp)
	}
	if !resp.HasBlockProcessed {
		t.Error("HasBlockProcessed should be true")
	}
	if resp.HasCompoundBUMP {
		t.Error("HasCompoundBUMP should be false")
	}
}

func TestHandleGetBlockProcessingStatus_NotFound(t *testing.T) {
	bs := newBlockProcStore()
	srv, _ := setupServerWithStoreAndBlockProc(t, bs)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/blocks/processing-status/missing", nil)
	srv.routerForTest().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status=%d want 404", rec.Code)
	}
}

func TestHandleBlockProcessed_RecordsStatusBeforeKafka(t *testing.T) {
	bs := newBlockProcStore()
	srv, router := setupServerWithStoreAndBlockProc(t, bs)
	_ = srv

	body := mustMarshalJSON(t, models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: "blk-1",
	})
	rec := httptest.NewRecorder()
	req := authedCallbackRequest(t, body)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(bs.processedCalls) != 1 || bs.processedCalls[0] != "blk-1" {
		t.Errorf("processedCalls=%v want [blk-1]", bs.processedCalls)
	}
}

func TestHandleBlockProcessed_StoreErrorDoesNotFailRequest(t *testing.T) {
	bs := newBlockProcStore()
	bs.markProcessedErr = errors.New("store down")
	srv, router := setupServerWithStoreAndBlockProc(t, bs)
	_ = srv

	body := mustMarshalJSON(t, models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: "blk-2",
	})
	rec := httptest.NewRecorder()
	req := authedCallbackRequest(t, body)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status=%d want 200 (store error must not fail request)", rec.Code)
	}
}

// setupServerWithStoreAndBlockProc wires up the handler stack with a
// blockProcStore as the backing store. The embedded mockStore takes the
// rest of the interface — the only methods that route through our fake are
// the block-processing ones.
type serverHandle struct {
	srv    *Server
	router http.Handler
}

func (h *serverHandle) routerForTest() http.Handler { return h.router }

func setupServerWithStoreAndBlockProc(t *testing.T, bs *blockProcStore) (*serverHandle, http.Handler) {
	t.Helper()
	srv, router := setupServerWithStore(&kafka.RecordingBroker{}, &bs.mockStore)
	srv.store = bs
	// The router was already constructed pointing at &bs.mockStore (via
	// setupServerWithStore) — but registerRoutes wires routes through
	// closures that look up s.store on every request, so swapping the field
	// is enough.
	_ = router
	return &serverHandle{srv: srv, router: router}, router
}
