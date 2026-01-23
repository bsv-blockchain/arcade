// Package errors provides ARC-compatible error types and status codes.
package errors

import (
	"errors"
	"fmt"
)

// StatusCode represents ARC-compatible HTTP status codes for transaction errors.
type StatusCode int

const (
	// Standard HTTP codes
	StatusOK         StatusCode = 200
	StatusBadRequest StatusCode = 400
	StatusNotFound   StatusCode = 404

	// ARC-specific error codes (460-499 range)
	StatusTxFormat                StatusCode = 460 // Not extended format
	StatusUnlockingScripts        StatusCode = 461 // Malformed unlocking scripts
	StatusInputs                  StatusCode = 462 // Invalid inputs
	StatusMalformed               StatusCode = 463 // Malformed transaction
	StatusOutputs                 StatusCode = 464 // Invalid outputs
	StatusFees                    StatusCode = 465 // Fee too low
	StatusConflict                StatusCode = 466 // Conflicting transaction
	StatusGeneric                 StatusCode = 467 // Generic validation error
	StatusBeefInvalid             StatusCode = 468 // BEEF validation failed
	StatusMerkleRoots             StatusCode = 469 // Merkle roots validation failed
	StatusFrozenPolicy            StatusCode = 471 // Input frozen (policy)
	StatusFrozenConsensus         StatusCode = 472 // Input frozen (consensus)
	StatusCumulativeFees          StatusCode = 473 // Cumulative fee validation failed
	StatusTxSize                  StatusCode = 474 // Transaction size validation failed
	StatusMinedAncestorsNotInBUMP StatusCode = 475 // Mined ancestors not found in BUMPs
)

// arcDocURL is the base URL for ARC error documentation.
const arcDocURL = "https://bitcoin-sv.github.io/arc/#/errors?id=_"

// ErrorFields represents the structured error response matching ARC's format.
type ErrorFields struct {
	Type      string  `json:"type"`
	Title     string  `json:"title"`
	Status    int     `json:"status"`
	Detail    string  `json:"detail"`
	ExtraInfo *string `json:"extraInfo,omitempty"`
}

// ArcError wraps an error with an ARC-compatible status code.
type ArcError struct {
	Err        error
	StatusCode StatusCode
	ExtraInfo  string
}

// Error implements the error interface.
func (e *ArcError) Error() string {
	if e.ExtraInfo != "" {
		return fmt.Sprintf("%s: %s", e.Err.Error(), e.ExtraInfo)
	}
	return e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *ArcError) Unwrap() error {
	return e.Err
}

// NewArcError creates a new ArcError with the given error and status code.
func NewArcError(err error, code StatusCode) *ArcError {
	return &ArcError{
		Err:        err,
		StatusCode: code,
	}
}

// NewArcErrorWithInfo creates a new ArcError with extra info.
func NewArcErrorWithInfo(err error, code StatusCode, extraInfo string) *ArcError {
	return &ArcError{
		Err:        err,
		StatusCode: code,
		ExtraInfo:  extraInfo,
	}
}

// ToErrorFields converts an ArcError to ErrorFields for JSON response.
func (e *ArcError) ToErrorFields() *ErrorFields {
	fields := NewErrorFields(e.StatusCode, e.ExtraInfo)
	if e.ExtraInfo == "" {
		fields.ExtraInfo = nil
	}
	return fields
}

// GetArcError extracts an ArcError from an error chain, or returns nil.
func GetArcError(err error) *ArcError {
	var arcErr *ArcError
	if errors.As(err, &arcErr) {
		return arcErr
	}
	return nil
}

// NewErrorFields creates ErrorFields for the given status code.
func NewErrorFields(status StatusCode, extraInfo string) *ErrorFields {
	fields := &ErrorFields{
		Status: int(status),
		Type:   fmt.Sprintf("%s%d", arcDocURL, status),
	}

	if extraInfo != "" {
		fields.ExtraInfo = &extraInfo
	}

	switch status {
	case StatusBadRequest:
		fields.Title = "Bad request"
		fields.Detail = "The request seems to be malformed and cannot be processed"
	case StatusNotFound:
		fields.Title = "Not found"
		fields.Detail = "The requested resource could not be found"
	case StatusTxFormat:
		fields.Title = "Not extended format"
		fields.Detail = "Missing input scripts: Transaction could not be transformed to extended format"
	case StatusUnlockingScripts:
		fields.Title = "Malformed transaction"
		fields.Detail = "Transaction unlocking scripts are invalid"
	case StatusInputs:
		fields.Title = "Invalid inputs"
		fields.Detail = "Transaction is invalid because the inputs are non-existent or spent"
	case StatusMalformed:
		fields.Title = "Malformed transaction"
		fields.Detail = "Transaction is malformed and cannot be processed"
	case StatusOutputs:
		fields.Title = "Invalid outputs"
		fields.Detail = "Transaction is invalid because the outputs are non-existent or invalid"
	case StatusFees:
		fields.Title = "Fee too low"
		fields.Detail = "Transaction fee is too low"
	case StatusConflict:
		fields.Title = "Conflicting tx found"
		fields.Detail = "Transaction is valid, but there is a conflicting tx in the block template"
	case StatusGeneric:
		fields.Title = "Generic error"
		fields.Detail = "Transaction could not be processed"
	case StatusBeefInvalid:
		fields.Title = "Invalid BEEF"
		fields.Detail = "BEEF validation failed: BEEF invalid"
	case StatusMerkleRoots:
		fields.Title = "Merkle Roots validation failed"
		fields.Detail = "BEEF validation failed: couldn't verify Merkle Roots"
	case StatusFrozenPolicy:
		fields.Title = "Input Frozen"
		fields.Detail = "Input Frozen (blacklist manager policy blacklisted)"
	case StatusFrozenConsensus:
		fields.Title = "Input Frozen"
		fields.Detail = "Input Frozen (blacklist manager consensus blacklisted)"
	case StatusCumulativeFees:
		fields.Title = "Cumulative fee validation failed"
		fields.Detail = "Cumulative fee validation failed"
	case StatusTxSize:
		fields.Title = "Transaction size validation failed"
		fields.Detail = "Transaction size validation failed"
	case StatusMinedAncestorsNotInBUMP:
		fields.Title = "Mined ancestors not found in BUMPs"
		fields.Detail = "BEEF validation failed: couldn't find mined ancestor of the transaction in provided BUMPs"
	default:
		fields.Status = int(StatusGeneric)
		fields.Type = fmt.Sprintf("%s%d", arcDocURL, StatusGeneric)
		fields.Title = "Generic error"
		fields.Detail = "Transaction could not be processed"
	}

	return fields
}
