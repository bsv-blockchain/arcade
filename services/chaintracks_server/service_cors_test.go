package chaintracks_server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestChaintracksHTTPHandlerAllowsBrowserClients(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/chaintracks/v2/height", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"height": 1})
	})
	handler := chaintracksHTTPHandler(router)

	t.Run("preflight", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/chaintracks/v2/height", nil)
		req.Header.Set("Origin", "https://wallet.example")
		req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		res := httptest.NewRecorder()

		handler.ServeHTTP(res, req)

		require.Equal(t, http.StatusNoContent, res.Code)
		require.Equal(t, "*", res.Header().Get("Access-Control-Allow-Origin"))
		require.True(t, strings.Contains(res.Header().Get("Access-Control-Allow-Methods"), http.MethodGet))
	})

	t.Run("actual request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/chaintracks/v2/height", nil)
		req.Header.Set("Origin", "https://wallet.example")
		res := httptest.NewRecorder()

		handler.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		require.Equal(t, "*", res.Header().Get("Access-Control-Allow-Origin"))
	})
}
