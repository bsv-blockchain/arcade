package models

import "testing"

func FuzzHexBytesUnmarshalJSON(f *testing.F) {
	// Seed corpus
	f.Add([]byte(`null`))
	f.Add([]byte(`""`))
	f.Add([]byte(`"00"`))
	f.Add([]byte(`"deadbeef"`))
	f.Add([]byte(`"0123456789abcdef"`))
	f.Add([]byte(`invalid`))

	f.Fuzz(func(_ *testing.T, data []byte) {
		var h HexBytes
		_ = h.UnmarshalJSON(data) // Should not panic
	})
}
