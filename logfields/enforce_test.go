package logfields

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// bannedKeys are field-name string literals that must never appear as the
// first argument to a zap.String/Strings/Int/Uint64 call outside this
// package. The live canonical keys catch a regression back to an ad-hoc
// zap.X("txid", ...) call instead of the logfields constructor; the
// retired keys catch a regression back to a name Task 4 explicitly
// renamed away from (txids_sample, txid_total, and the blockHash
// camelCase outlier).
var bannedKeys = map[string]struct{}{
	// live canonical keys
	"txid":         {},
	"txids":        {},
	"txid_count":   {},
	"block_hash":   {},
	"block_height": {},
	"subtree_hash": {},
	"callback_url": {},
	"stage":        {},
	// retired ad-hoc variants
	"txids_sample": {},
	"txid_total":   {},
	"blockHash":    {},
}

// zapFieldFuncs are the zap constructor names this guard inspects. Each
// takes the field name as its first argument.
var zapFieldFuncs = map[string]struct{}{
	"String":  {},
	"Strings": {},
	"Int":     {},
	"Uint64":  {},
}

// scanForViolations walks every non-test .go file under root (skipping
// .git/vendor/node_modules and the logfields package itself) and returns
// one human-readable violation string per zap.String/Strings/Int/Uint64
// call whose first argument is a banned field-name literal.
func scanForViolations(root string) ([]string, error) {
	var violations []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		// This package owns the canonical literals — skip it.
		if filepath.Dir(rel) == "logfields" {
			return nil
		}

		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parsing %s: %w", path, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkgIdent, ok := sel.X.(*ast.Ident)
			if !ok || pkgIdent.Name != "zap" {
				return true
			}
			if _, isFieldFunc := zapFieldFuncs[sel.Sel.Name]; !isFieldFunc {
				return true
			}
			if len(call.Args) == 0 {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, uerr := strconv.Unquote(lit.Value)
			if uerr != nil {
				return true
			}
			if _, banned := bannedKeys[value]; banned {
				pos := fset.Position(call.Pos())
				violations = append(violations, fmt.Sprintf(
					"%s: zap.%s(%q, ...) — use the logfields constructor instead",
					pos, sel.Sel.Name, value,
				))
			}
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return violations, nil
}

// repoRoot returns the arcade module root, derived from this test file's
// own path (logfields/enforce_test.go -> parent directory) so the test
// works regardless of the working directory `go test` invokes it from.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine caller file for repoRoot")
	}
	return filepath.Dir(filepath.Dir(file))
}

// TestNoCanonicalKeyLiteralsOutsidePackage is the enforcement guard: it
// fails if any non-test .go file outside package logfields constructs one
// of the canonical (or retired) log-field keys via a raw zap.String/
// Strings/Int/Uint64 call instead of going through this package's
// constructors. This is what prevents the field-name migration from
// regressing once merged.
func TestNoCanonicalKeyLiteralsOutsidePackage(t *testing.T) {
	violations, err := scanForViolations(repoRoot(t))
	if err != nil {
		t.Fatalf("scanning repo for canonical log-field literals: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf(
			"found %d canonical log-field literal(s) outside package logfields; use the logfields constructor instead:\n%s",
			len(violations), strings.Join(violations, "\n"),
		)
	}
}

// TestScanDetectsViolation proves the AST scan actually catches a
// regression rather than trivially passing: it writes a synthetic package
// containing a banned zap.String("txid", ...) call to a temp directory and
// asserts scanForViolations flags it, then asserts a clean sibling file in
// the same temp directory produces no violation.
func TestScanDetectsViolation(t *testing.T) {
	dir := t.TempDir()

	bad := `package scratch

import "go.uber.org/zap"

func bad() zap.Field {
	return zap.String("txid", "deadbeef")
}
`
	if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte(bad), 0o600); err != nil {
		t.Fatalf("writing synthetic bad file: %v", err)
	}

	violations, err := scanForViolations(dir)
	if err != nil {
		t.Fatalf("scanForViolations: %v", err)
	}
	if len(violations) != 1 {
		t.Fatalf("scanForViolations found %d violations in synthetic bad file, want 1: %v", len(violations), violations)
	}
	if !strings.Contains(violations[0], `zap.String("txid"`) {
		t.Errorf("violation message = %q, want it to mention zap.String(\"txid\"", violations[0])
	}

	// Remove the bad file and confirm a clean file in the same directory
	// produces zero violations — proves the scanner isn't just always
	// failing.
	if removeErr := os.Remove(filepath.Join(dir, "bad.go")); removeErr != nil {
		t.Fatalf("removing synthetic bad file: %v", removeErr)
	}
	good := `package scratch

import "go.uber.org/zap"

func good() zap.Field {
	return zap.String("not_a_canonical_key", "deadbeef")
}
`
	if writeErr := os.WriteFile(filepath.Join(dir, "good.go"), []byte(good), 0o600); writeErr != nil {
		t.Fatalf("writing synthetic good file: %v", writeErr)
	}
	violations, err = scanForViolations(dir)
	if err != nil {
		t.Fatalf("scanForViolations: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("scanForViolations found %d violations in clean file, want 0: %v", len(violations), violations)
	}
}
