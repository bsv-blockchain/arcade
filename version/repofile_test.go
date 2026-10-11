package version

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	body, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
