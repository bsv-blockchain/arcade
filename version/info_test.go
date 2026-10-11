package version

import (
	"runtime/debug"
	"strings"
	"testing"
)

func TestParseSettings(t *testing.T) {
	const rev = "0123456789abcdef0123456789abcdef01234567"

	t.Run("applied", func(t *testing.T) {
		got, err := ParseSettings([]debug.BuildSetting{
			{Key: "vcs.revision", Value: rev},
			{Key: "vcs.modified", Value: "false"},
			{Key: "vcs.time", Value: "ignored"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Revision != rev || got.Modified {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("dirty", func(t *testing.T) {
		got, err := ParseSettings([]debug.BuildSetting{
			{Key: "vcs.revision", Value: rev},
			{Key: "vcs.modified", Value: "true"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Modified || got.Revision != rev {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("developer build has no vcs keys", func(t *testing.T) {
		got, err := ParseSettings(nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.Revision != "" || got.Modified {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("rejects malformed revision", func(t *testing.T) {
		for _, bad := range []string{"", "abc", strings.ToUpper(rev), rev + "ff", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"} {
			_, err := ParseSettings([]debug.BuildSetting{{Key: "vcs.revision", Value: bad}})
			if err == nil {
				t.Fatalf("revision %q was accepted", bad)
			}
		}
	})

	t.Run("rejects malformed modified", func(t *testing.T) {
		_, err := ParseSettings([]debug.BuildSetting{
			{Key: "vcs.revision", Value: rev},
			{Key: "vcs.modified", Value: "yes"},
		})
		if err == nil {
			t.Fatal("modified=yes was accepted")
		}
	})

	t.Run("rejects modified without revision", func(t *testing.T) {
		_, err := ParseSettings([]debug.BuildSetting{{Key: "vcs.modified", Value: "false"}})
		if err == nil {
			t.Fatal("modified without revision was accepted")
		}
	})
}

func TestReadAcceptsThisTestBinary(t *testing.T) {
	info, err := Read()
	if err != nil {
		t.Fatal(err)
	}
	if info.Version == "" {
		t.Fatal("version stamp is empty")
	}
	if info.Revision != "" && len(info.Revision) != 40 {
		t.Fatalf("revision %q", info.Revision)
	}
}

func TestFormat(t *testing.T) {
	got := (Info{Version: "v1.2.3", Revision: "0123456789abcdef0123456789abcdef01234567", Modified: false}).Format()
	if got != "version=v1.2.3\nrevision=0123456789abcdef0123456789abcdef01234567\nmodified=false\n" {
		t.Fatalf("format = %q", got)
	}
}

func TestDockerfileDeclaresOCILabels(t *testing.T) {
	// The runtime image is assembled from a prebuilt binary, so the labels
	// have to be on that Dockerfile. An empty build-arg still leaves the
	// label key in place; the provenance build refuses to ship empty values.
	body := readRepoFile(t, "Dockerfile")
	for _, label := range []string{
		"org.opencontainers.image.source",
		"org.opencontainers.image.revision",
		"org.opencontainers.image.created",
		"org.opencontainers.image.version",
	} {
		if !strings.Contains(body, label) {
			t.Fatalf("Dockerfile missing %s", label)
		}
	}
}
