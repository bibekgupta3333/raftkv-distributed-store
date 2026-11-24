package main

import (
	"bytes"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/docscheck"
)

// WBS 7.6.1: docs/configuration.md lists exactly the flags each raftkvctl
// subcommand prints with -h (`make docs-check`).
func TestConfigurationDocRaftkvctl(t *testing.T) {
	doc := docscheck.ReadDoc(t)
	for heading, invocations := range map[string][][]string{
		"### raftkvctl put, get, del":    {{"put"}, {"get"}, {"del"}},
		"### raftkvctl cas":              {{"cas"}},
		"### raftkvctl range":            {{"range"}},
		"### raftkvctl debug state-hash": {{"debug", "state-hash"}},
		"### raftkvctl debug wal-dump":   {{"debug", "wal-dump"}},
	} {
		var flags []string
		for _, args := range invocations {
			var out, help bytes.Buffer
			if code := run(append(args, "-h"), &out, &help); code != 0 {
				t.Fatalf("%v -h: exit %d", args, code)
			}
			got := docscheck.HelpFlags(help.String())
			if len(got) == 0 {
				t.Fatalf("%v -h printed no flags:\n%s", args, help.String())
			}
			if flags != nil && !equal(flags, got) {
				t.Fatalf("%v -h flags %v differ from %v, which share a doc section", args, got, flags)
			}
			flags = got
		}
		docscheck.Compare(t, doc, heading, flags)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
