package apikey

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateAndValidate(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		k, err := Generate()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(k, Prefix))
		require.Len(t, k, len(Prefix)+bodyLen+1+checksumLen)
		require.NoError(t, Validate(k))
		require.False(t, seen[k], "Generate must not repeat")
		seen[k] = true
	}
}

func TestValidateRejects(t *testing.T) {
	k, err := Generate()
	require.NoError(t, err)
	flip := func(s string, i int) string {
		b := []byte(s)
		if b[i] == 'a' {
			b[i] = 'b'
		} else {
			b[i] = 'a'
		}
		return string(b)
	}
	for name, bad := range map[string]string{
		"empty":            "",
		"wrong prefix":     "ghp_" + k[len(Prefix)-1:],
		"truncated":        k[:len(k)-1],
		"body tampered":    flip(k, len(Prefix)+3),
		"checksum altered": flip(k, len(k)-1),
		"bad charset":      k[:len(Prefix)] + "!" + k[len(Prefix)+1:],
		// Only the body/checksum separator is replaced here, leaving the
		// prefix's own trailing "_" untouched. strings.Replace(k, "_", "-", 2)
		// would also clobber that first underscore, so the key would already
		// be rejected by the prefix check before the separator check ever
		// ran.
		"missing separator": k[:len(Prefix)] + strings.Replace(k[len(Prefix):], "_", "-", 1),
	} {
		require.ErrorIs(t, Validate(bad), ErrMalformed, name)
	}
}

func TestHashAndHint(t *testing.T) {
	k, err := Generate()
	require.NoError(t, err)
	first, second := Hash(k), Hash(k)
	require.Equal(t, first, second, "Hash must be deterministic")
	require.Len(t, Hash(k), 64)
	require.NotEqual(t, Hash(k), Hash(k+"x"))
	require.Equal(t, k[:9]+"…"+k[len(k)-3:], Hint(k))
}

func TestNewID(t *testing.T) {
	a, err := NewID()
	require.NoError(t, err)
	b, err := NewID()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(a, "key_"))
	require.Len(t, a, len("key_")+24)
	require.NotEqual(t, a, b)
}
