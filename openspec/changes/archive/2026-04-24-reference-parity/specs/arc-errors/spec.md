## ADDED Requirements

### Requirement: ARC-compatible error codes
The system SHALL define error status codes in the 460-475 range matching the ARC specification: StatusTxFormat (460), StatusUnlockingScripts (461), StatusInputs (462), StatusMalformed (463), StatusOutputs (464), StatusFees (465), StatusConflict (466), StatusGeneric (467), StatusBeefInvalid (468), StatusMerkleRoots (469), StatusFrozenPolicy (471), StatusFrozenConsensus (472), StatusCumulativeFees (473), StatusTxSize (474), StatusMinedAncestorsNotInBUMP (475).

#### Scenario: Validation error returns ARC error
- **WHEN** a transaction fails fee validation
- **THEN** the system SHALL return an `ArcError` with `StatusCode` 465 (StatusFees) wrapping the underlying error

### Requirement: Structured error response format
The system SHALL provide an `ErrorFields` struct with `type` (documentation URL), `title`, `status` (numeric code), `detail`, and optional `extraInfo` fields, matching the ARC error response format.

#### Scenario: Convert ArcError to JSON response
- **WHEN** an `ArcError` with StatusCode 463 is converted to `ErrorFields`
- **THEN** the result SHALL have `type` containing the ARC doc URL with code 463, `title` "Malformed transaction", `status` 463, and appropriate `detail`

### Requirement: Error chain extraction
The system SHALL provide `GetArcError(err)` to extract an `ArcError` from an error chain using `errors.As`.

#### Scenario: Extract ArcError from wrapped error
- **WHEN** `GetArcError` is called on `fmt.Errorf("wrapped: %w", arcErr)` where arcErr is an ArcError
- **THEN** it SHALL return the original ArcError
