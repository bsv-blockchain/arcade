package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIKeyCLI(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotAuth = r.Method, r.URL.Path, r.Header.Get("Authorization")
		gotBody = nil
		if b, _ := io.ReadAll(r.Body); len(b) > 0 {
			_ = json.Unmarshal(b, &gotBody)
		}
		if r.URL.Path == "/admin/v1/keys/key_missing" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"api key not found"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"key_1"}`))
	}))
	defer srv.Close()

	run := func(args ...string) (string, error) {
		cmd := newAPIKeyCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs(append([]string{"--admin-url", srv.URL, "--admin-token", "tok"}, args...))
		err := cmd.Execute()
		return out.String(), err
	}

	out, err := run("create", "--name", "acme", "--tier", "pro")
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/admin/v1/keys", gotPath)
	require.Equal(t, "Bearer tok", gotAuth)
	require.Equal(t, "pro", gotBody["tier"])
	require.Contains(t, out, `"id": "key_1"`)

	_, err = run("list")
	require.NoError(t, err)
	require.Equal(t, http.MethodGet, gotMethod)

	_, err = run("set-tier", "key_1", "free")
	require.NoError(t, err)
	require.Equal(t, http.MethodPatch, gotMethod)
	require.Equal(t, "/admin/v1/keys/key_1", gotPath)
	require.Equal(t, "free", gotBody["tier"])

	_, err = run("revoke", "key_1")
	require.NoError(t, err)
	require.Equal(t, http.MethodDelete, gotMethod)

	_, err = run("get", "key_missing")
	require.ErrorContains(t, err, "404")
}

func TestAPIKeyCLI_MissingTokenFailsLocally(t *testing.T) {
	var requested bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requested = true
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer srv.Close()

	t.Setenv("ARCADE_ADMIN_TOKEN", "")
	t.Setenv("BSV_BROADCASTER_ADMIN_TOKEN", "")

	cmd := newAPIKeyCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--admin-url", srv.URL, "list"})
	err := cmd.Execute()
	require.ErrorContains(t, err, "admin token required (--admin-token, BSV_BROADCASTER_ADMIN_TOKEN or ARCADE_ADMIN_TOKEN)")
	require.False(t, requested, "no request should have been sent without a token")
}

func TestAPIKeyCLI_TokenEnvPrecedence(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer srv.Close()

	run := func(flagToken string) (string, error) {
		cmd := newAPIKeyCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		args := []string{"--admin-url", srv.URL}
		if flagToken != "" {
			args = append(args, "--admin-token", flagToken)
		}
		cmd.SetArgs(append(args, "list"))
		err := cmd.Execute()
		return out.String(), err
	}

	t.Run("ARCADE_ADMIN_TOKEN used when nothing else is set", func(t *testing.T) {
		t.Setenv("BSV_BROADCASTER_ADMIN_TOKEN", "")
		t.Setenv("ARCADE_ADMIN_TOKEN", "legacy-tok")
		_, err := run("")
		require.NoError(t, err)
		require.Equal(t, "Bearer legacy-tok", gotAuth)
	})

	t.Run("BSV_BROADCASTER_ADMIN_TOKEN takes priority over ARCADE_ADMIN_TOKEN", func(t *testing.T) {
		t.Setenv("ARCADE_ADMIN_TOKEN", "legacy-tok")
		t.Setenv("BSV_BROADCASTER_ADMIN_TOKEN", "new-tok")
		_, err := run("")
		require.NoError(t, err)
		require.Equal(t, "Bearer new-tok", gotAuth)
	})

	t.Run("--admin-token flag takes priority over both env vars", func(t *testing.T) {
		t.Setenv("ARCADE_ADMIN_TOKEN", "legacy-tok")
		t.Setenv("BSV_BROADCASTER_ADMIN_TOKEN", "new-tok")
		_, err := run("flag-tok")
		require.NoError(t, err)
		require.Equal(t, "Bearer flag-tok", gotAuth)
	})
}

func TestAPIKeyCLI_NegativeExpiresInRejected(t *testing.T) {
	cmd := newAPIKeyCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--admin-token", "tok", "create", "--expires-in", "-1h"})
	err := cmd.Execute()
	require.ErrorContains(t, err, "must not be negative")
}

func TestAPIKeyCLI_IDArgsArePathEscaped(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// r.URL.Path is net/http's already-unescaped form; EscapedPath() is
		// what actually went out on the wire, which is what we're checking.
		gotPath = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	cmd := newAPIKeyCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--admin-url", srv.URL, "--admin-token", "tok", "get", "key/with space"})
	require.NoError(t, cmd.Execute())
	require.Equal(t, "/admin/v1/keys/key%2Fwith%20space", gotPath)
}

func TestAPIKeyCLI_OversizeResponseErrorsInsteadOfTruncating(t *testing.T) {
	big := bytes.Repeat([]byte("a"), maxAdminResponseBytes+10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(big)
	}))
	defer srv.Close()

	cmd := newAPIKeyCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--admin-url", srv.URL, "--admin-token", "tok", "list"})
	err := cmd.Execute()
	require.ErrorContains(t, err, "exceeds")
	// cobra prints its own usage text to out on an error return; that's
	// unrelated to this check. What matters is that none of the oversize
	// body itself (truncated or otherwise) ever reached the output.
	require.NotContains(t, out.String(), strings.Repeat("a", 64), "an oversize body must not be printed, truncated or otherwise")
}
