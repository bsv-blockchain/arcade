package api_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestWithCORS_PreflightShortCircuits(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/tx", func(c *gin.Context) { c.Status(http.StatusAccepted) })

	srv := httptest.NewServer(withCORS(router))
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodOptions, srv.URL+"/tx", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got == "" {
		t.Error("Access-Control-Allow-Methods missing")
	}
	if got := resp.Header.Get("Access-Control-Allow-Headers"); got == "" {
		t.Error("Access-Control-Allow-Headers missing")
	}
}

func TestWithCORS_HeadersOnActualRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	srv := httptest.NewServer(withCORS(router))
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/health", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
	}
}
