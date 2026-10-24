package apikey

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNormalizeClientIPIPv6Slash64 checks the per-client-IP throttle's address
// normalization: genuine IPv6 addresses are bucketed by their /64 (so one
// client cycling through addresses in its own /64 shares a bucket), an
// IPv4-mapped IPv6 address unmaps to the same bucket as its plain IPv4
// form, and anything unparsable is keyed verbatim.
func TestNormalizeClientIPIPv6Slash64(t *testing.T) {
	require.Equal(t, NormalizeClientIP("2001:db8:1234:5678::1"), NormalizeClientIP("2001:db8:1234:5678::2"),
		"addresses in the same /64 share a bucket")
	require.NotEqual(t, NormalizeClientIP("2001:db8:1234:5678::1"), NormalizeClientIP("2001:db8:9999:5678::1"),
		"addresses in different /64s get different buckets")

	require.Equal(t, "203.0.113.5", NormalizeClientIP("203.0.113.5"), "IPv4 is keyed verbatim")
	require.Equal(t, NormalizeClientIP("203.0.113.5"), NormalizeClientIP("::ffff:203.0.113.5"),
		"an IPv4-mapped IPv6 address unmaps into the same bucket as its plain IPv4 form")
	require.Equal(t, "not-an-ip", NormalizeClientIP("not-an-ip"),
		"anything netip cannot parse falls back to the raw string")
}
