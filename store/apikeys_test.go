package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAPIKey_Usable(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)

	tests := []struct {
		name string
		key  APIKey
		want bool
	}{
		{"active no-expiry", APIKey{Status: APIKeyActive, ExpiresAt: nil}, true},
		{"active with future expiry", APIKey{Status: APIKeyActive, ExpiresAt: &future}, true},
		{"expiry equals now", APIKey{Status: APIKeyActive, ExpiresAt: &now}, false},
		{"expired", APIKey{Status: APIKeyActive, ExpiresAt: &past}, false},
		{"revoked", APIKey{Status: APIKeyRevoked, ExpiresAt: nil}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.key.Usable(now))
		})
	}
}

func TestNextQuotaUsed(t *testing.T) {
	tests := []struct {
		name              string
		used, want, limit int64
		result            int64
	}{
		{"below limit", 0, 30, 100, 30},
		{"partial grant clamps to limit", 30, 80, 100, 100},
		{"already at limit", 100, 1, 100, 100},
		{"limit lowered below used keeps used", 110, 5, 50, 110},
		{"first reservation clamps to limit", 0, 150, 100, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.result, NextQuotaUsed(tt.used, tt.want, tt.limit))
		})
	}
}
