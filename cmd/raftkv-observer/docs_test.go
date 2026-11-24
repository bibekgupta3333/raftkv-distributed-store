package main

import (
	"bytes"
	"errors"
	"flag"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/docscheck"
)

// WBS 7.6.1: docs/configuration.md lists exactly the flags in
// `raftkv-observer -h` (`make docs-check`).
func TestConfigurationDocObserver(t *testing.T) {
	var help bytes.Buffer
	if _, err := parseFlags([]string{"-h"}, &help); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-h: %v", err)
	}
	flags := docscheck.HelpFlags(help.String())
	if len(flags) == 0 {
		t.Fatalf("-h printed no flags:\n%s", help.String())
	}
	docscheck.Compare(t, docscheck.ReadDoc(t), "### raftkv-observer flags", flags, "version")
}
