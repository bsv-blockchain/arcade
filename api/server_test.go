package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/arcade/models"
	"github.com/bitcoin-sv/arcade/teranode"
	"github.com/bitcoin-sv/arcade/validator"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Valid P2PKH transaction
const validTxHex = "0100000001484d40d45b9ea0d652fca8258ab7caa42541eb52975857f96fb50cd732c8b481000000006a47304402203e6b6e3f0b5c2f64d80f3e9e0e3f0e3f0e3f0e3f0e3f0e3f0e3f0e3f0e3f0e3f02203e6b6e3f0b5c2f64d80f3e9e0e3f0e3f0e3f0e3f0e3f0e3f0e3f0e3f0e3f0e3f012103a524f43d6166ad3567f18b0c5f1d3dcf5a7b7c8e5d6e7f8a9b0c1d2e3f4a5b6cffffffff0140420f00000000001976a914c3f8e5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b288ac00000000"

// Mock implementations

// mockStatusStore implements store.StatusStore
type mockStatusStore struct {
	statuses map[string]*models.TransactionStatus
	err      error
}

func newMockStatusStore() *mockStatusStore {
	return &mockStatusStore{
		statuses: make(map[string]*models.TransactionStatus),
	}
}

func (m *mockStatusStore) InsertStatus(ctx context.Context, status *models.TransactionStatus) error {
	if m.err != nil {
		return m.err
	}
	status.Status = models.StatusReceived
	m.statuses[status.TxID] = status
	return nil
}

func (m *mockStatusStore) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	if m.err != nil {
		return m.err
	}
	m.statuses[status.TxID] = status
	return nil
}

func (m *mockStatusStore) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	if m.err != nil {
		return nil, m.err
	}
	status, ok := m.statuses[txid]
	if !ok {
		return nil, nil
	}
	return status, nil
}

func (m *mockStatusStore) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*models.TransactionStatus
	for _, status := range m.statuses {
		if status.Timestamp.After(since) || status.Timestamp.Equal(since) {
			result = append(result, status)
		}
	}
	return result, nil
}

func (m *mockStatusStore) Close() error {
	return nil
}

// mockSubmissionStore implements store.SubmissionStore
type mockSubmissionStore struct {
	submissions map[string][]*models.Submission
	err         error
}

func newMockSubmissionStore() *mockSubmissionStore {
	return &mockSubmissionStore{
		submissions: make(map[string][]*models.Submission),
	}
}

func (m *mockSubmissionStore) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	if m.err != nil {
		return m.err
	}
	m.submissions[sub.TxID] = append(m.submissions[sub.TxID], sub)
	return nil
}

func (m *mockSubmissionStore) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.submissions[txid], nil
}

func (m *mockSubmissionStore) GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*models.Submission
	for _, subs := range m.submissions {
		for _, sub := range subs {
			if sub.CallbackToken == callbackToken {
				result = append(result, sub)
			}
		}
	}
	return result, nil
}

func (m *mockSubmissionStore) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	if m.err != nil {
		return m.err
	}
	for _, subs := range m.submissions {
		for _, sub := range subs {
			if sub.SubmissionID == submissionID {
				sub.LastDeliveredStatus = lastStatus
				sub.RetryCount = retryCount
				sub.NextRetryAt = nextRetry
				return nil
			}
		}
	}
	return nil
}

func (m *mockSubmissionStore) Close() error {
	return nil
}

// mockNetworkStateStore implements store.NetworkStateStore
type mockNetworkStateStore struct {
	state *models.NetworkState
	err   error
}

func newMockNetworkStateStore() *mockNetworkStateStore {
	return &mockNetworkStateStore{
		state: &models.NetworkState{
			CurrentHeight: 800000,
			LastBlockHash: "00000000000000000000000000000000000000000000000000000000deadbeef",
			LastBlockTime: time.Now(),
		},
	}
}

func (m *mockNetworkStateStore) UpdateNetworkState(ctx context.Context, state *models.NetworkState) error {
	if m.err != nil {
		return m.err
	}
	m.state = state
	return nil
}

func (m *mockNetworkStateStore) GetNetworkState(ctx context.Context) (*models.NetworkState, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.state, nil
}

func (m *mockNetworkStateStore) Close() error {
	return nil
}

// mockPublisher implements events.Publisher
type mockPublisher struct {
	published []models.StatusUpdate
	err       error
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{
		published: make([]models.StatusUpdate, 0),
	}
}

func (m *mockPublisher) Publish(ctx context.Context, event models.StatusUpdate) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, event)
	return nil
}

func (m *mockPublisher) Subscribe(ctx context.Context) (<-chan models.StatusUpdate, error) {
	if m.err != nil {
		return nil, m.err
	}
	ch := make(chan models.StatusUpdate, 10)
	return ch, nil
}

func (m *mockPublisher) Close() error {
	return nil
}

// Helper functions

func setupMockTeranodeServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/tx", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/txs", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return httptest.NewServer(mux)
}

func setupTestServer(t *testing.T) (*Server, *mockStatusStore, *mockSubmissionStore, *mockNetworkStateStore, *mockPublisher, *teranode.Client, *httptest.Server) {
	statusStore := newMockStatusStore()
	submissionStore := newMockSubmissionStore()
	networkStore := newMockNetworkStateStore()
	publisher := newMockPublisher()

	mockTeranode := setupMockTeranodeServer()
	teranodeClient := teranode.NewClient([]string{mockTeranode.URL})

	v := validator.NewValidator(&validator.Policy{
		MaxTxSizePolicy:         10000,
		MaxTxSigopsCountsPolicy: 4294967296,
		MaxScriptSizePolicy:     100000000,
		MinFeePerKB:             1,
		EnableFeeCheck:          false,
		EnableScriptExecution:   false,
	})

	policy := &PolicyResponse{
		MaxScriptSizePolicy:     100000000,
		MaxTxSigOpsCountsPolicy: 4294967296,
		MaxTxSizePolicy:         10000,
		MiningFee: FeeAmount{
			Bytes:    1,
			Satoshis: 1,
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	server := NewServer(
		statusStore,
		submissionStore,
		networkStore,
		publisher,
		teranodeClient,
		v,
		policy,
		"",
		logger,
	)

	return server, statusStore, submissionStore, networkStore, publisher, teranodeClient, mockTeranode
}

func newTestRequest(method, target string, body string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return req
}

// Tests

func TestHandlePostTransaction_Success(t *testing.T) {
	server, statusStore, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	reqBody := `{"rawTx":"` + validTxHex + `"}`
	req := newTestRequest(http.MethodPost, "/v1/tx", reqBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handlePostTransaction(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp TransactionResponse
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.NotEmpty(t, resp.TxID)
	assert.Equal(t, string(models.StatusReceived), resp.TxStatus)

	status := statusStore.statuses[resp.TxID]
	assert.NotNil(t, status)
	assert.Equal(t, models.StatusReceived, status.Status)

	time.Sleep(100 * time.Millisecond)

	updatedStatus := statusStore.statuses[resp.TxID]
	assert.Contains(t, []models.Status{models.StatusSentToNetwork, models.StatusAcceptedByNetwork}, updatedStatus.Status)
}

func TestHandlePostTransaction_InvalidHex(t *testing.T) {
	server, _, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	reqBody := `{"rawTx":"INVALID_HEX"}`
	req := newTestRequest(http.MethodPost, "/v1/tx", reqBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handlePostTransaction(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Contains(t, resp["error"], "Invalid transaction hex")
}

func TestHandlePostTransaction_WithCallback(t *testing.T) {
	server, _, submissionStore, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	reqBody := `{"rawTx":"` + validTxHex + `"}`
	req := newTestRequest(http.MethodPost, "/v1/tx", reqBody)
	req.Header.Set("X-CallbackUrl", "https://example.com/webhook")
	req.Header.Set("X-CallbackToken", "secret-token")
	req.Header.Set("X-CallbackBatch", "true")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handlePostTransaction(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp TransactionResponse
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	subs, err := submissionStore.GetSubmissionsByTxID(context.Background(), resp.TxID)
	require.NoError(t, err)
	require.Len(t, subs, 1)

	assert.Equal(t, "https://example.com/webhook", subs[0].CallbackURL)
	assert.Equal(t, "secret-token", subs[0].CallbackToken)
	assert.Equal(t, false, subs[0].FullStatusUpdates)
}

func TestHandlePostTransaction_ValidationError(t *testing.T) {
	server, _, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	server.validator = validator.NewValidator(&validator.Policy{
		MaxTxSizePolicy:         10,
		MaxTxSigopsCountsPolicy: 10,
		MaxScriptSizePolicy:     10,
		MinFeePerKB:             1000000,
		EnableFeeCheck:          true,
		EnableScriptExecution:   false,
	})

	e := echo.New()
	reqBody := `{"rawTx":"` + validTxHex + `"}`
	req := newTestRequest(http.MethodPost, "/v1/tx", reqBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handlePostTransaction(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Contains(t, resp["error"], "Validation failed")
}

func TestHandlePostTransactions_Success(t *testing.T) {
	server, statusStore, _, _, publisher, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	reqBody := `[{"rawTx":"` + validTxHex + `"},{"rawTx":"` + validTxHex + `"}]`
	req := newTestRequest(http.MethodPost, "/v1/txs", reqBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handlePostTransactions(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp []TransactionResponse
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Len(t, resp, 2)

	txid := resp[0].TxID
	for _, r := range resp {
		assert.NotEmpty(t, r.TxID)
		assert.Equal(t, txid, r.TxID)
		assert.Equal(t, string(models.StatusSentToNetwork), r.TxStatus)
	}

	assert.NotNil(t, statusStore.statuses[txid])

	assert.Len(t, publisher.published, 2)
}

func TestHandlePostTransactions_InvalidHex(t *testing.T) {
	server, _, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	reqBody := `[{"rawTx":"INVALID_HEX"}]`
	req := newTestRequest(http.MethodPost, "/v1/txs", reqBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handlePostTransactions(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Contains(t, resp["error"], "Invalid transaction hex")
}

func TestHandleGetTransaction_Found(t *testing.T) {
	server, statusStore, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	txid := "deadbeef"
	statusStore.statuses[txid] = &models.TransactionStatus{
		TxID:        txid,
		Status:      models.StatusMined,
		Timestamp:   time.Now(),
		BlockHash:   "blockhash",
		BlockHeight: 800000,
	}

	e := echo.New()
	req := newTestRequest(http.MethodGet, "/v1/tx/"+txid, "")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("txid")
	c.SetParamValues(txid)

	err := server.handleGetTransaction(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp TransactionResponse
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, txid, resp.TxID)
	assert.Equal(t, string(models.StatusMined), resp.TxStatus)
	assert.Equal(t, "blockhash", resp.BlockHash)
	assert.Equal(t, uint64(800000), resp.BlockHeight)
}

func TestHandleGetTransaction_NotFound(t *testing.T) {
	server, _, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	req := newTestRequest(http.MethodGet, "/v1/tx/nonexistent", "")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("txid")
	c.SetParamValues("nonexistent")

	err := server.handleGetTransaction(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Contains(t, resp["error"], "Transaction not found")
}

func TestHandleGetPolicy(t *testing.T) {
	server, _, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	req := newTestRequest(http.MethodGet, "/v1/policy", "")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handleGetPolicy(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp PolicyResponse
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, server.policy.MaxScriptSizePolicy, resp.MaxScriptSizePolicy)
	assert.Equal(t, server.policy.MaxTxSigOpsCountsPolicy, resp.MaxTxSigOpsCountsPolicy)
	assert.Equal(t, server.policy.MaxTxSizePolicy, resp.MaxTxSizePolicy)
	assert.Equal(t, server.policy.MiningFee.Bytes, resp.MiningFee.Bytes)
	assert.Equal(t, server.policy.MiningFee.Satoshis, resp.MiningFee.Satoshis)
}

func TestHandleGetHealth_Healthy(t *testing.T) {
	server, _, _, _, _, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()
	req := newTestRequest(http.MethodGet, "/v1/health", "")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handleGetHealth(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp HealthResponse
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.True(t, resp.Healthy)
	assert.Empty(t, resp.Reason)
	assert.Equal(t, "0.1.0", resp.Version)
}

func TestParseHeaders_AllHeaders(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/tx", nil)
	req.Header.Set("X-CallbackUrl", "https://example.com/webhook")
	req.Header.Set("X-CallbackToken", "my-token")
	req.Header.Set("X-CallbackBatch", "true")
	req.Header.Set("X-FullStatusUpdates", "true")
	req.Header.Set("X-WaitFor", "MINED")
	req.Header.Set("X-MaxTimeout", "10")
	req.Header.Set("X-SkipFeeValidation", "true")
	req.Header.Set("X-SkipScriptValidation", "true")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	headers := parseHeaders(c)

	assert.Equal(t, "https://example.com/webhook", headers.CallbackURL)
	assert.Equal(t, "my-token", headers.CallbackToken)
	assert.True(t, headers.CallbackBatch)
	assert.True(t, headers.FullStatusUpdates)
	assert.Equal(t, "MINED", headers.WaitFor)
	assert.Equal(t, 10, headers.MaxTimeout)
	assert.True(t, headers.SkipFeeValidation)
	assert.True(t, headers.SkipScriptValidation)
}

func TestParseHeaders_Defaults(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/tx", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	headers := parseHeaders(c)

	assert.Empty(t, headers.CallbackURL)
	assert.Empty(t, headers.CallbackToken)
	assert.False(t, headers.CallbackBatch)
	assert.False(t, headers.FullStatusUpdates)
	assert.Equal(t, defaultWaitFor, headers.WaitFor)
	assert.Equal(t, defaultMaxTimeout, headers.MaxTimeout)
	assert.False(t, headers.SkipFeeValidation)
	assert.False(t, headers.SkipScriptValidation)
}

func TestParseHeaders_MaxTimeoutClamp(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/tx", nil)
	req.Header.Set("X-MaxTimeout", "100")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	headers := parseHeaders(c)

	assert.Equal(t, maxMaxTimeout, headers.MaxTimeout)
}

func TestParseHeaders_InvalidMaxTimeout(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/tx", nil)
	req.Header.Set("X-MaxTimeout", "invalid")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	headers := parseHeaders(c)

	assert.Equal(t, defaultMaxTimeout, headers.MaxTimeout)
}

func TestExtractTxID_Valid(t *testing.T) {
	rawTx, err := hex.DecodeString(validTxHex)
	require.NoError(t, err)

	txid, err := extractTxID(rawTx)
	require.NoError(t, err)
	assert.NotEmpty(t, txid)
}

func TestExtractTxID_Invalid(t *testing.T) {
	rawTx := []byte{0x00, 0x01, 0x02}

	_, err := extractTxID(rawTx)
	assert.Error(t, err)
}

func TestToTransactionResponse_Nil(t *testing.T) {
	resp := toTransactionResponse(nil)
	assert.Empty(t, resp.TxID)
	assert.Empty(t, resp.TxStatus)
}

func TestToTransactionResponse_Complete(t *testing.T) {
	status := &models.TransactionStatus{
		TxID:         "deadbeef",
		Status:       models.StatusMined,
		Timestamp:    time.Now(),
		BlockHash:    "blockhash",
		BlockHeight:  800000,
		MerklePath:   "merkle",
		ExtraInfo:    "info",
		CompetingTxs: []string{"tx1", "tx2"},
	}

	resp := toTransactionResponse(status)

	assert.Equal(t, status.TxID, resp.TxID)
	assert.Equal(t, string(status.Status), resp.TxStatus)
	assert.Equal(t, status.BlockHash, resp.BlockHash)
	assert.Equal(t, status.BlockHeight, resp.BlockHeight)
	assert.Equal(t, status.MerklePath, resp.MerklePath)
	assert.Equal(t, status.ExtraInfo, resp.ExtraInfo)
	assert.Equal(t, status.CompetingTxs, resp.CompetingTxs)
}

func TestServer_IntegrationFlow(t *testing.T) {
	server, statusStore, _, _, publisher, _, mockTeranode := setupTestServer(t)
	defer mockTeranode.Close()

	e := echo.New()

	reqBody := `{"rawTx":"` + validTxHex + `"}`
	req := newTestRequest(http.MethodPost, "/v1/tx", reqBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := server.handlePostTransaction(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var postResp TransactionResponse
	err = json.Unmarshal(rec.Body.Bytes(), &postResp)
	require.NoError(t, err)
	txid := postResp.TxID

	req2 := newTestRequest(http.MethodGet, "/v1/tx/"+txid, "")
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	c2.SetParamNames("txid")
	c2.SetParamValues(txid)

	err = server.handleGetTransaction(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var getResp TransactionResponse
	err = json.Unmarshal(rec2.Body.Bytes(), &getResp)
	require.NoError(t, err)

	assert.Equal(t, txid, getResp.TxID)
	assert.Equal(t, string(models.StatusSentToNetwork), getResp.TxStatus)

	assert.NotNil(t, statusStore.statuses[txid])
	assert.Equal(t, models.StatusSentToNetwork, statusStore.statuses[txid].Status)
	assert.Len(t, publisher.published, 1)
}
