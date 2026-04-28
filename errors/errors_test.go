package arcerrors

import (
	"errors"
	"fmt"
	"testing"
)

func TestNewArcError(t *testing.T) {
	err := NewArcError(errors.New("test error"), StatusFees)
	if err.StatusCode != StatusFees {
		t.Errorf("expected StatusFees (465), got %d", err.StatusCode)
	}
	if err.Error() != "test error" {
		t.Errorf("expected 'test error', got %q", err.Error())
	}
}

func TestNewArcErrorWithInfo(t *testing.T) {
	err := NewArcErrorWithInfo(errors.New("fee low"), StatusFees, "need 100 sat/kb")
	if err.ExtraInfo != "need 100 sat/kb" {
		t.Errorf("expected extra info, got %q", err.ExtraInfo)
	}
	if err.Error() != "fee low: need 100 sat/kb" {
		t.Errorf("expected combined message, got %q", err.Error())
	}
}

func TestGetArcError(t *testing.T) {
	arcErr := NewArcError(errors.New("inner"), StatusMalformed)
	wrapped := fmt.Errorf("outer: %w", arcErr)

	got := GetArcError(wrapped)
	if got == nil {
		t.Fatal("expected to extract ArcError from chain")
	}
	if got.StatusCode != StatusMalformed {
		t.Errorf("expected StatusMalformed (463), got %d", got.StatusCode)
	}
}

func TestGetArcError_NotPresent(t *testing.T) {
	got := GetArcError(errors.New("plain error"))
	if got != nil {
		t.Error("expected nil for non-ArcError")
	}
}

func TestToErrorFields(t *testing.T) {
	err := NewArcError(errors.New("test"), StatusFees)
	fields := err.ToErrorFields()

	if fields.Status != 465 {
		t.Errorf("expected status 465, got %d", fields.Status)
	}
	if fields.Title != "Fee too low" {
		t.Errorf("expected 'Fee too low', got %q", fields.Title)
	}
}

func TestNewErrorFields_AllCodes(t *testing.T) {
	codes := []StatusCode{
		StatusOK, StatusBadRequest, StatusNotFound,
		StatusTxFormat, StatusUnlockingScripts, StatusInputs,
		StatusMalformed, StatusOutputs, StatusFees,
		StatusConflict, StatusGeneric, StatusBeefInvalid,
		StatusMerkleRoots, StatusFrozenPolicy, StatusFrozenConsensus,
		StatusCumulativeFees, StatusTxSize, StatusMinedAncestorsNotInBUMP,
	}

	for _, code := range codes {
		fields := NewErrorFields(code, "")
		if fields.Status != int(code) {
			t.Errorf("code %d: expected status %d, got %d", code, code, fields.Status)
		}
		if fields.Title == "" {
			t.Errorf("code %d: expected non-empty title", code)
		}
		if fields.Detail == "" {
			t.Errorf("code %d: expected non-empty detail", code)
		}
	}
}

func TestUnwrap(t *testing.T) {
	inner := errors.New("inner error")
	arcErr := NewArcError(inner, StatusGeneric)
	if !errors.Is(arcErr, inner) {
		t.Error("expected Unwrap to expose inner error")
	}
}
