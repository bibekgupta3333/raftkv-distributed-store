// Package docscheck keeps docs/configuration.md in step with the binaries
// (WBS 7.6.1). The commands' tests feed it their -h output and the doc; it
// reports flags the doc misses or lists without the binary having them.
package docscheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// ConfigDoc is the configuration reference, relative to the repository root.
const ConfigDoc = "docs/configuration.md"

// ReadDoc reads the configuration reference from a test in any package below
// the repository root.
func ReadDoc(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		b, err := os.ReadFile(filepath.Join(dir, ConfigDoc))
		if err == nil {
			return string(b)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("%s not found above the test's directory", ConfigDoc)
		}
		dir = parent
	}
}

// helpFlag matches a flag line of Go's flag.PrintDefaults: "  -name type".
var helpFlag = regexp.MustCompile(`(?m)^  -([A-Za-z0-9][A-Za-z0-9-]*)`)

// HelpFlags returns the flag names in a flag set's -h output.
func HelpFlags(help string) []string {
	var out []string
	for _, m := range helpFlag.FindAllStringSubmatch(help, -1) {
		out = append(out, m[1])
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Section returns the body of the markdown section headed exactly heading
// (for example "### raftkvctl cas"), up to the next heading of the same or a
// higher level.
func Section(doc, heading string) (string, error) {
	level := strings.IndexByte(heading, ' ')
	lines := strings.Split(doc, "\n")
	for i, l := range lines {
		if strings.TrimSpace(l) != heading {
			continue
		}
		var body []string
		for _, m := range lines[i+1:] {
			if n := strings.IndexByte(m, ' '); strings.HasPrefix(m, "#") && n > 0 && n <= level && strings.Trim(m[:n], "#") == "" {
				break
			}
			body = append(body, m)
		}
		return strings.Join(body, "\n"), nil
	}
	return "", fmt.Errorf("%s has no section %q", ConfigDoc, heading)
}

// tableKey matches the first cell of a table row: "| `--name` |" or "| `key` |".
var tableKey = regexp.MustCompile("(?m)^\\| `(?:--)?([A-Za-z0-9_][A-Za-z0-9_-]*)` \\|")

// TableKeys returns the names in the first column of the tables in section,
// without a leading "--".
func TableKeys(section string) []string {
	var out []string
	for _, m := range tableKey.FindAllStringSubmatch(section, -1) {
		out = append(out, m[1])
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Compare fails t if want (from the binary) and the keys documented in the
// section differ. ignore names documented entries the binary handles
// outside its flag set (such as --version).
func Compare(t testing.TB, doc, heading string, want []string, ignore ...string) {
	t.Helper()
	sec, err := Section(doc, heading)
	if err != nil {
		t.Fatal(err)
	}
	have := slices.DeleteFunc(TableKeys(sec), func(k string) bool { return slices.Contains(ignore, k) })
	for _, k := range want {
		if !slices.Contains(have, k) {
			t.Errorf("%s, section %q: %q exists in the binary but is not documented", ConfigDoc, heading, k)
		}
	}
	for _, k := range have {
		if !slices.Contains(want, k) {
			t.Errorf("%s, section %q: documents %q, which the binary does not have", ConfigDoc, heading, k)
		}
	}
}
