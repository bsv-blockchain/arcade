package arcade

import (
	"bytes"
	"testing"
)

func FuzzReadVarInt(f *testing.F) {
	// Seed corpus with valid varints
	f.Add([]byte{0x00})                                                 // 0
	f.Add([]byte{0xfc})                                                 // 252 (max single byte)
	f.Add([]byte{0xfd, 0x01, 0x00})                                     // 1 (2-byte)
	f.Add([]byte{0xfe, 0x01, 0x00, 0x00, 0x00})                         // 1 (4-byte)
	f.Add([]byte{0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // 1 (8-byte)

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = readVarInt(r) // Should not panic
	})
}
