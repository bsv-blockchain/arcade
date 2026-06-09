//go:build !cgo

// This file is the non-cgo counterpart to script_verifier_gobdk.go. arcade's
// transaction validation requires go-bdk, which is cgo-only, so a binary built
// without cgo cannot validate. newVerifier therefore fails fast at validator
// construction (process startup) rather than silently degrading or skipping
// validation.
package validator

import "errors"

// errCGORequired is returned by newVerifier when the binary was built without
// cgo (CGO_ENABLED=0), so go-bdk is unavailable.
var errCGORequired = errors.New("validator: go-bdk requires a cgo-enabled build (CGO_ENABLED=1); this binary was built without cgo")

func newVerifier(_ *Policy) (bdkValidator, error) {
	return nil, errCGORequired
}
