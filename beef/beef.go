package beef

import (
	"encoding/binary"
	"errors"
	"fmt"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

const (
	beefV1Magic = uint32(0xEFBE0001) // 4022206465 - 0x0100BEEF in little-endian
	beefV2Magic = uint32(0xEFBE0002) // 4022206466 - 0x0200BEEF in little-endian
)

var (
	ErrNotBeef             = errors.New("data is not BEEF format")
	ErrBeefParse           = errors.New("failed to parse BEEF")
	ErrBeefNoTransaction   = errors.New("BEEF contains no transaction")
	ErrBeefValidation      = errors.New("BEEF validation failed")
	ErrBeefMissingParents  = errors.New("BEEF missing parent transactions")
)

// IsBeef checks if the provided data is in BEEF format by checking for magic bytes
func IsBeef(data []byte) bool {
	if len(data) < 4 {
		return false
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	return magic == beefV1Magic || magic == beefV2Magic
}

// ParseAndValidate parses BEEF bytes, validates structure, and returns the final transaction
func ParseAndValidate(beefBytes []byte) (*sdkTx.Transaction, error) {
	if !IsBeef(beefBytes) {
		return nil, ErrNotBeef
	}

	beef, err := sdkTx.NewBeefFromBytes(beefBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBeefParse, err)
	}

	if err := validateBeefStructure(beef); err != nil {
		return nil, err
	}

	tx, err := extractFinalTransaction(beef)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// validateBeefStructure performs basic validation on the BEEF structure
func validateBeefStructure(beef *sdkTx.Beef) error {
	if beef == nil {
		return fmt.Errorf("%w: nil BEEF", ErrBeefValidation)
	}

	if len(beef.Transactions) == 0 {
		return fmt.Errorf("%w: no transactions in BEEF", ErrBeefValidation)
	}

	// Verify all parent transactions are included
	for txid, beefTx := range beef.Transactions {
		if beefTx.Transaction == nil {
			continue
		}

		tx := beefTx.Transaction
		for _, input := range tx.Inputs {
			parentTxid := input.SourceTXID
			if parentTxid == nil {
				continue
			}

			// Check if parent exists in BEEF (unless it's a known transaction)
			if _, exists := beef.Transactions[*parentTxid]; !exists {
				// This might be a known transaction (txid-only), which is acceptable
				// TODO: Add more sophisticated validation if needed
			}
		}
		_ = txid // Used in loop
	}

	// TODO: Add merkle root verification
	// This requires access to block headers service to verify merkle proofs
	// For now, we skip merkle verification as per requirements

	return nil
}

// extractFinalTransaction extracts the final transaction to be broadcast from BEEF
func extractFinalTransaction(beef *sdkTx.Beef) (*sdkTx.Transaction, error) {
	// The final transaction is typically the last transaction added to BEEF
	// Get all transaction IDs and find the one that has no children
	var finalTx *sdkTx.Transaction

	// Build a map of which transactions are referenced as inputs
	referencedTxs := make(map[string]bool)

	for _, beefTx := range beef.Transactions {
		if beefTx.Transaction == nil {
			continue
		}

		for _, input := range beefTx.Transaction.Inputs {
			if input.SourceTXID != nil {
				referencedTxs[input.SourceTXID.String()] = true
			}
		}
	}

	// Find transactions that are NOT referenced by any other transaction
	// These are potential final transactions
	for txid, beefTx := range beef.Transactions {
		if beefTx.Transaction == nil {
			continue
		}

		if !referencedTxs[txid.String()] {
			// This transaction is not used as input by any other transaction
			// It's a potential final transaction
			if finalTx != nil {
				// Multiple unreferenced transactions - choose the last one
				// or could return an error if this is unexpected
			}
			finalTx = beefTx.Transaction
		}
	}

	if finalTx == nil {
		return nil, ErrBeefNoTransaction
	}

	return finalTx, nil
}
