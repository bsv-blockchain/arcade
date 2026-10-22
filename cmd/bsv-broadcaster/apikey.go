package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// maxAdminResponseBytes bounds how much of an admin response body this CLI
// will buffer. Read as maxAdminResponseBytes+1 so a body exactly at the cap
// is accepted but anything larger is detected and reported as an error,
// rather than silently truncated and printed as if it were the whole
// response.
const maxAdminResponseBytes = 1 << 20

// newAPIKeyCmd is `arcade apikey …`: a thin client for the admin API
// (admin.* listener). Reach the listener with kubectl port-forward or from
// inside the cluster; it is never exposed publicly.
func newAPIKeyCmd() *cobra.Command {
	var adminURL, adminToken string
	cmd := &cobra.Command{
		Use:   "apikey",
		Short: "Manage API keys through the admin API",
	}
	cmd.PersistentFlags().StringVar(&adminURL, "admin-url", "http://127.0.0.1:8090", "admin API base URL")
	cmd.PersistentFlags().StringVar(&adminToken, "admin-token", "",
		"admin bearer token (default $BSV_BROADCASTER_ADMIN_TOKEN or $ARCADE_ADMIN_TOKEN)")

	client := func(c *cobra.Command) (*adminClient, error) {
		tok := adminToken
		if tok == "" {
			tok = os.Getenv("BSV_BROADCASTER_ADMIN_TOKEN")
		}
		if tok == "" {
			tok = os.Getenv("ARCADE_ADMIN_TOKEN")
		}
		if tok == "" {
			return nil, fmt.Errorf("admin token required (--admin-token, BSV_BROADCASTER_ADMIN_TOKEN or ARCADE_ADMIN_TOKEN)")
		}
		return &adminClient{
			base: strings.TrimRight(adminURL, "/"), token: tok,
			http: &http.Client{Timeout: 30 * time.Second}, out: c.OutOrStdout(),
		}, nil
	}

	var name, tier string
	var expiresIn time.Duration
	create := &cobra.Command{
		Use:   "create",
		Short: "Create a key; the plaintext key is printed once and never again",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			if expiresIn < 0 {
				return fmt.Errorf("--expires-in must not be negative")
			}
			ac, err := client(c)
			if err != nil {
				return err
			}
			body := map[string]any{"name": name, "tier": tier}
			if expiresIn > 0 {
				body["expiresAt"] = time.Now().Add(expiresIn).UTC()
			}
			return ac.do(c.Context(), http.MethodPost, "/admin/v1/keys", body)
		},
	}
	create.Flags().StringVar(&name, "name", "", "owner or label")
	create.Flags().StringVar(&tier, "tier", "free", "tier name")
	create.Flags().DurationVar(&expiresIn, "expires-in", 0, "expire the key after this long (0 = never)")

	cmd.AddCommand(
		create,
		&cobra.Command{
			Use: "list", Short: "List keys", Args: cobra.NoArgs,
			RunE: func(c *cobra.Command, _ []string) error {
				ac, err := client(c)
				if err != nil {
					return err
				}
				return ac.do(c.Context(), http.MethodGet, "/admin/v1/keys", nil)
			},
		},
		&cobra.Command{
			Use: "get <id>", Short: "Show one key", Args: cobra.ExactArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				ac, err := client(c)
				if err != nil {
					return err
				}
				return ac.do(c.Context(), http.MethodGet, "/admin/v1/keys/"+url.PathEscape(args[0]), nil)
			},
		},
		&cobra.Command{
			Use: "set-tier <id> <tier>", Short: "Change a key's tier", Args: cobra.ExactArgs(2),
			RunE: func(c *cobra.Command, args []string) error {
				ac, err := client(c)
				if err != nil {
					return err
				}
				return ac.do(c.Context(), http.MethodPatch, "/admin/v1/keys/"+url.PathEscape(args[0]), map[string]any{"tier": args[1]})
			},
		},
		&cobra.Command{
			Use: "revoke <id>", Short: "Revoke a key", Args: cobra.ExactArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				ac, err := client(c)
				if err != nil {
					return err
				}
				return ac.do(c.Context(), http.MethodDelete, "/admin/v1/keys/"+url.PathEscape(args[0]), nil)
			},
		},
	)
	return cmd
}

type adminClient struct {
	base  string
	token string
	http  *http.Client
	out   io.Writer
}

// do sends one admin request and prints the indented JSON response; a
// non-2xx status is returned as an error carrying the response body.
func (a *adminClient) do(ctx context.Context, method, path string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAdminResponseBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxAdminResponseBytes {
		return fmt.Errorf("%s %s: response body exceeds %d bytes", method, path, maxAdminResponseBytes)
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, raw, "", "  ") == nil {
		raw = pretty.Bytes()
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(raw))
	}
	_, err = fmt.Fprintln(a.out, string(raw))
	return err
}
