package client

import (
	"net/http"
	"time"
)

// Option is a functional option for configuring the Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client to use for requests.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithTimeout sets the timeout for HTTP requests.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}
