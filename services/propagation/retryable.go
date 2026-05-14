package propagation

import "strings"

// retryablePatterns contains error message substrings that indicate a transient
// broadcast failure eligible for retry (e.g., parent tx not yet visible to the
// network). These are deliberately narrow — Teranode wraps every per-tx error
// with "Failed to process transaction: ... failed to validate transaction",
// including genuinely invalid txs, so matching on "failed to validate" alone
// caused every bad tx to loop through PENDING_RETRY for minutes before being
// finally rejected.
var retryablePatterns = []string{
	"missing inputs",
	"txn-mempool-conflict",
}

// IsRetryableError returns true if the error message indicates a transient
// broadcast failure that may succeed on retry.
func IsRetryableError(errMsg string) bool {
	if errMsg == "" {
		return false
	}
	lower := strings.ToLower(errMsg)
	for _, pattern := range retryablePatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}
