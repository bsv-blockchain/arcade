package chaintracks

import (
	"math/big"
)

// oneLsh256 is 1 shifted left 256 bits (used for chainwork calculation)
var oneLsh256 = new(big.Int).Lsh(big.NewInt(1), 256) //nolint:gochecknoglobals // Constant value for calculations

// CompactToBig converts a compact representation of a 256-bit number (as used in Bitcoin difficulty)
// to a big.Int. The compact format is a special floating point notation where:
// - The first byte is the exponent (number of bytes)
// - The remaining 3 bytes are the mantissa
// - The sign bit (0x00800000) indicates if the number is negative
func CompactToBig(compact uint32) *big.Int {
	// Extract the mantissa, sign bit, and exponent
	mantissa := compact & 0x007fffff
	isNegative := compact&0x00800000 != 0
	exponent := uint(compact >> 24)

	// Since the base for the exponent is 256, the exponent can be treated
	// as the number of bytes to represent the full 256-bit number.
	// This is equivalent to: N = mantissa * 256^(exponent-3)
	var bn *big.Int
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		bn = big.NewInt(int64(mantissa))
	} else {
		bn = big.NewInt(int64(mantissa))
		bn.Lsh(bn, 8*(exponent-3))
	}

	// Make it negative if the sign bit is set
	if isNegative {
		bn = bn.Neg(bn)
	}

	return bn
}

// CalculateWork calculates the work represented by a given difficulty target (bits).
// Work is calculated as: work = 2^256 / (target + 1)
// This gives higher work values for more difficult targets (smaller target numbers).
func CalculateWork(bits uint32) *big.Int {
	// Convert compact bits to target
	target := CompactToBig(bits)

	// Return zero work if target is negative or zero (invalid)
	if target.Sign() <= 0 {
		return big.NewInt(0)
	}

	// Calculate work: (2^256) / (target + 1)
	denominator := new(big.Int).Add(target, big.NewInt(1))
	work := new(big.Int).Div(oneLsh256, denominator)

	return work
}

// AddWork adds work to cumulative chainwork
func AddWork(cumulativeWork *big.Int, bits uint32) *big.Int {
	work := CalculateWork(bits)
	result := new(big.Int).Add(cumulativeWork, work)
	return result
}

// CompareChainWork compares two chainwork values
// Returns:
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
func CompareChainWork(a, b *big.Int) int {
	return a.Cmp(b)
}

// ChainWorkToHex converts chainwork to a 64-character hex string (padded)
func ChainWorkToHex(work *big.Int) string {
	// Format as 64-character hex string (32 bytes)
	hexStr := work.Text(16)
	// Pad with leading zeros to make it 64 characters
	for len(hexStr) < 64 {
		hexStr = "0" + hexStr
	}
	return hexStr
}

// ChainWorkFromHex parses a hex string to chainwork
func ChainWorkFromHex(hexStr string) (*big.Int, error) {
	work := new(big.Int)
	_, success := work.SetString(hexStr, 16)
	if !success {
		return nil, ErrInvalidHeader
	}
	return work, nil
}
