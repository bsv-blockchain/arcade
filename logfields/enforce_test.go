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
// first argument to one of the zapFieldFuncs constructors outside this
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
	"status":       {},
	"stage":        {},
	// retired ad-hoc variants
	"txids_sample": {},
	"txid_total":   {},
	"blockHash":    {},
}

// zapFieldFuncs are the zap constructor names this guard inspects, each of
// which takes the field name as its first string argument. Coverage is
// deliberately broad: the strongly-typed constructors actually used for
// these keys (String/Strings/Int/Uint64), PLUS the escape-hatch
// constructors (Any/Reflect) that could otherwise smuggle a canonical key
// past the guard with an untyped value. Adding another zap.<Fn>("txid",…)
// shape to the codebase means adding its name here too.
var zapFieldFuncs = map[string]struct{}{
	"String":  {},
	"Strings": {},
	"Int":     {},
	"Uint64":  {},
	"Any":     {},
	"Reflect": {},
}

// scanForViolations walks every non-test .go file under root (skipping
// .git/vendor/node_modules and the logfields package itself) and returns
// one human-readable violation string per zapFieldFuncs call (see that
// var for the exact constructor coverage) whose first argument is a banned
// field-name literal.
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
// of the canonical (or retired) log-field keys via a raw zap constructor
// (see zapFieldFuncs for the exact coverage) instead of going through this
// package's constructors. This is what prevents the field-name migration
// from regressing once merged.
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
	// Each probe is one synthetic bad call the guard MUST flag. Coverage
	// spans the string constructor (txid), the canonical "status" key (the
	// gap this review closed), and the Any/Reflect escape hatches that could
	// otherwise smuggle a canonical key past a String/Strings/Int/Uint64-only
	// scan.
	probes := []struct {
		name string
		call string // the zap.<Fn>(...) expression under test
		want string // substring the violation message must contain
	}{
		{"string_txid", `zap.String("txid", "deadbeef")`, `zap.String("txid"`},
		{"string_status", `zap.String("status", "MINED")`, `zap.String("status"`},
		{"any_txid", `zap.Any("txid", 42)`, `zap.Any("txid"`},
		{"reflect_block_hash", `zap.Reflect("block_hash", nil)`, `zap.Reflect("block_hash"`},
	}
	for _, p := range probes {
		t.Run(p.name, func(t *testing.T) {
			dir := t.TempDir()
			src := "package scratch\n\nimport \"go.uber.org/zap\"\n\nfunc bad() zap.Field {\n\treturn " + p.call + "\n}\n"
			if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte(src), 0o600); err != nil {
				t.Fatalf("writing synthetic bad file: %v", err)
			}
			violations, err := scanForViolations(dir)
			if err != nil {
				t.Fatalf("scanForViolations: %v", err)
			}
			if len(violations) != 1 {
				t.Fatalf("found %d violations, want 1: %v", len(violations), violations)
			}
			if !strings.Contains(violations[0], p.want) {
				t.Errorf("violation message = %q, want it to contain %q", violations[0], p.want)
			}
		})
	}

	// A clean file (non-canonical key) must produce zero violations — proves
	// the scanner isn't just always failing.
	t.Run("clean_file_no_violation", func(t *testing.T) {
		dir := t.TempDir()
		good := `package scratch

import "go.uber.org/zap"

func good() zap.Field {
	return zap.String("not_a_canonical_key", "deadbeef")
}
`
		if err := os.WriteFile(filepath.Join(dir, "good.go"), []byte(good), 0o600); err != nil {
			t.Fatalf("writing synthetic good file: %v", err)
		}
		violations, err := scanForViolations(dir)
		if err != nil {
			t.Fatalf("scanForViolations: %v", err)
		}
		if len(violations) != 0 {
			t.Fatalf("found %d violations in clean file, want 0: %v", len(violations), violations)
		}
	})
}
