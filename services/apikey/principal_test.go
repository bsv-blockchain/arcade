package apikey

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/broadcaster/config"
)

// TestAnonymousSubjectNormalizesClientIP pins that the anonymous Subject --
// which feeds the anonymous quota bucket, the fail-open bucket and the
// in-flight concurrency map -- is keyed by NormalizeClientIP(ClientIP), not
// the raw address: an IPv6 client rotating addresses within its own /64
// must not get fresh quota on every rotation, and an IPv4-mapped IPv6
// address must collapse to the same subject as its plain IPv4 form.
func TestAnonymousSubjectNormalizesClientIP(t *testing.T) {
	subj := func(ip string) string {
		return Principal{Tier: config.TierAnonymous, ClientIP: ip}.Subject()
	}

	require.Equal(t, subj("2001:db8:1234:5678::1"), subj("2001:db8:1234:5678::2"),
		"two addresses in the same /64 must share a Subject")
	require.NotEqual(t, subj("2001:db8:1234:5678::1"), subj("2001:db8:9999:5678::1"),
		"addresses in different /64s must get different subjects")

	require.Equal(t, subj("203.0.113.5"), subj("::ffff:203.0.113.5"),
		"an IPv4 address and its IPv4-mapped IPv6 form must share a Subject")

	// ClientIP itself must stay raw -- only Subject() normalizes.
	p := Principal{Tier: config.TierAnonymous, ClientIP: "2001:db8:1234:5678::1"}
	require.Equal(t, "2001:db8:1234:5678::1", p.ClientIP, "ClientIP must not be mutated by Subject")
}

// TestKeyedSubjectUnaffectedByClientIP pins that a keyed principal's Subject
// depends only on KeyID, never on ClientIP -- NormalizeClientIP applies to
// the anonymous branch only.
func TestKeyedSubjectUnaffectedByClientIP(t *testing.T) {
	p := Principal{KeyID: "key_1", Tier: "pro", ClientIP: "2001:db8::1"}
	require.Equal(t, "key:key_1", p.Subject())
}
