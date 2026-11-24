package docscheck

import (
	"slices"
	"testing"
)

const help = `Usage of x:
  -alpha string
    	the -beta-like text is not a flag
  -gamma-delta
    	bool flag
`

const doc = "# T\n\n## x\n\n### x flags\n\n| Flag | D |\n|---|---|\n| `--alpha` | a |\n| `--gamma-delta` | b |\n\n" +
	"### x yaml\n\n| `node_id` | uint64 |\n\n## y\n\n| `--other` | c |\n"

func TestHelpFlagsAndSections(t *testing.T) {
	if got := HelpFlags(help); !slices.Equal(got, []string{"alpha", "gamma-delta"}) {
		t.Fatalf("HelpFlags = %v", got)
	}
	sec, err := Section(doc, "### x flags")
	if err != nil {
		t.Fatal(err)
	}
	if got := TableKeys(sec); !slices.Equal(got, []string{"alpha", "gamma-delta"}) {
		t.Fatalf("flags section keys = %v (must stop at the next heading)", got)
	}
	sec, _ = Section(doc, "## x")
	if got := TableKeys(sec); !slices.Equal(got, []string{"alpha", "gamma-delta", "node_id"}) {
		t.Fatalf("## x keys = %v (must include subsections, stop at ## y)", got)
	}
	if _, err := Section(doc, "### missing"); err == nil {
		t.Fatal("a missing section must be an error")
	}
}

func TestCompareReportsBothDirections(t *testing.T) {
	ft := &fakeT{TB: t}
	Compare(ft, doc, "### x flags", []string{"alpha", "new-flag"})
	if ft.errors != 2 { // new-flag undocumented; gamma-delta documented but gone
		t.Fatalf("Compare reported %d problems, want 2", ft.errors)
	}
	ft = &fakeT{TB: t}
	Compare(ft, doc, "### x flags", []string{"alpha", "gamma-delta"})
	if ft.errors != 0 {
		t.Fatalf("Compare reported %d problems for a matching doc", ft.errors)
	}
}

type fakeT struct {
	testing.TB
	errors int
}

func (f *fakeT) Errorf(string, ...any) { f.errors++ }
func (f *fakeT) Helper()               {}
