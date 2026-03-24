package client

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParseErrorResponse(t *testing.T) {
	c := &Client{}

	tests := []struct {
		name           string
		statusCode     int
		body           string
		wantContains   string
		wantSentinel   error
	}{
		{
			name:         "JSON error response",
			statusCode:   400,
			body:         `{"error":"bad transaction format"}`,
			wantContains: "HTTP 400: bad transaction format",
			wantSentinel: ErrUnexpectedHTTPStatus,
		},
		{
			name:         "non-JSON response",
			statusCode:   502,
			body:         "Bad Gateway",
			wantContains: "HTTP 502: Bad Gateway",
			wantSentinel: ErrUnexpectedHTTPStatus,
		},
		{
			name:         "empty body",
			statusCode:   500,
			body:         "",
			wantContains: "HTTP 500",
			wantSentinel: ErrUnexpectedHTTPStatus,
		},
		{
			name:         "JSON without error field",
			statusCode:   503,
			body:         `{"message":"service unavailable"}`,
			wantContains: "HTTP 503",
			wantSentinel: ErrUnexpectedHTTPStatus,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tt.statusCode,
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}
			err := c.parseErrorResponse(resp)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tt.wantSentinel) {
				t.Errorf("expected error to wrap %v, got: %v", tt.wantSentinel, err)
			}
			if !strings.Contains(err.Error(), tt.wantContains) {
				t.Errorf("expected error to contain %q, got: %q", tt.wantContains, err.Error())
			}
		})
	}
}
