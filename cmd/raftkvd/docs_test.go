package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/docscheck"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
)

// WBS 7.6.1: docs/configuration.md lists exactly the flags in `raftkvd -h`
// and exactly the YAML keys of node.Config (`make docs-check`).
func TestConfigurationDocRaftkvd(t *testing.T) {
	var out, help bytes.Buffer
	if code := run([]string{"-h"}, &out, &help); code != exitOK {
		t.Fatalf("-h: exit %d", code)
	}
	flags := docscheck.HelpFlags(help.String())
	if len(flags) < 10 {
		t.Fatalf("parsed only %v from -h output:\n%s", flags, help.String())
	}
	doc := docscheck.ReadDoc(t)
	docscheck.Compare(t, doc, "### raftkvd flags", flags, "version")
	docscheck.Compare(t, doc, "### raftkvd YAML config file", yamlKeys(reflect.TypeFor[node.Config]()))
}

// yamlKeys returns the yaml tag names of t's fields, recursing into slices of
// structs (peers).
func yamlKeys(t reflect.Type) []string {
	var out []string
	for f := range t.Fields() {
		name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if name == "" || name == "-" {
			continue
		}
		out = append(out, name)
		if ft := f.Type; ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Struct {
			out = append(out, yamlKeys(ft.Elem())...)
		}
	}
	return out
}
