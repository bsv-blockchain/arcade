package version

import (
	"fmt"
	"regexp"
	"runtime/debug"
	"strings"
)

// fullRevision is the git commit Go stamps in vcs.revision. A short or
// non-hex value is rejected so a provenance report cannot treat a truncated
// or rewritten revision as the source commit.
var fullRevision = regexp.MustCompile(`^[0-9a-f]{40}$`)

// Info is the source identity of this binary.
//
// Version is the ldflags stamp (a release tag, or "dev"). Revision and
// Modified come from Go buildinfo (vcs.revision, vcs.modified). Revision is
// empty when the binary was not stamped, which is a developer build, not a
// provenance-qualified one. Modified is true when that stamp recorded a dirty
// tree.
type Info struct {
	Version  string
	Revision string
	Modified bool
}

// Read returns the embedded version and VCS stamp. A present but malformed
// vcs.revision or vcs.modified is an error. A binary with no VCS keys is
// returned as a developer build, not an error.
func Read() (Info, error) {
	info := Info{Version: Version}
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi == nil {
		return info, nil
	}
	parsed, err := ParseSettings(bi.Settings)
	if err != nil {
		return Info{}, err
	}
	parsed.Version = Version
	return parsed, nil
}

// ParseSettings extracts vcs.revision and vcs.modified. It returns an error
// when a key is present but not a value this contract can report.
func ParseSettings(settings []debug.BuildSetting) (Info, error) {
	var info Info
	var sawRevision, sawModified bool
	for _, setting := range settings {
		switch setting.Key {
		case "vcs.revision":
			sawRevision = true
			if !fullRevision.MatchString(setting.Value) {
				return Info{}, fmt.Errorf("vcs.revision %q is not a 40-character lowercase hex commit", setting.Value)
			}
			info.Revision = setting.Value
		case "vcs.modified":
			sawModified = true
			switch setting.Value {
			case "true":
				info.Modified = true
			case "false":
				info.Modified = false
			default:
				return Info{}, fmt.Errorf("vcs.modified %q is not true or false", setting.Value)
			}
		}
	}
	if sawModified && !sawRevision {
		return Info{}, fmt.Errorf("vcs.modified is set without vcs.revision")
	}
	return info, nil
}

// Format is the text printed by `arcade version`.
func (info Info) Format() string {
	return strings.Join([]string{
		"version=" + info.Version,
		"revision=" + info.Revision,
		fmt.Sprintf("modified=%t", info.Modified),
		"",
	}, "\n")
}
