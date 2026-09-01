package api_server

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderDocsIncludesChaintracksReference(t *testing.T) {
	var body bytes.Buffer

	require.NoError(t, RenderDocs(&body))

	page := body.String()
	for _, expected := range []string{
		"Chaintracks is a separate Arcade listener",
		"http://localhost:8083/health",
		"/chaintracks/v2/network",
		"/chaintracks/v2/height",
		"/chaintracks/v2/tip",
		"/chaintracks/v2/tip.bin",
		"/chaintracks/v2/tip/stream",
		"/chaintracks/v2/reorg/stream",
		"Legacy v1 compatibility endpoints",
		"/chaintracks/v1/getChain",
		"/chaintracks/v1/findHeaderHexForBlockHash?hash={hash}",
	} {
		assert.Contains(t, page, expected)
	}
}
