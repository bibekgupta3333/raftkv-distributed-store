package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/snapshot"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// WBS 7.5.2: a data directory holding any on-disk structure in a format this
// build does not read (a newer WAL segment, Pebble schema or snapshot) makes
// raftkvd refuse to start with EX_CONFIG and an error naming the version.
func TestRefusesUnknownOnDiskFormat(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, dir string)
		want    string
	}{
		{"wal segment", func(t *testing.T, dir string) {
			// Only the magic matters: the version is checked before the CRC.
			hdr := make([]byte, 64)
			copy(hdr, "RKVWAL02")
			writeFile(t, filepath.Join(dir, node.WALDir, "wal-0000000000000001-0000000000000001.seg"), hdr)
		}, "segment format version 2"},
		{"pebble schema", func(t *testing.T, dir string) {
			live, err := storage.PrepareStateDir(filepath.Join(dir, node.StateDir))
			if err != nil {
				t.Fatal(err)
			}
			db, err := pebble.Open(live, &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			v := binary.LittleEndian.AppendUint64(nil, storage.SchemaVersion+1)
			if err := db.Set([]byte("m/schemaVersion"), v, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}, "unsupported schema version: have 2"},
		{"snapshot metadata", func(t *testing.T, dir string) {
			m := &snapshot.Meta{
				ClusterId: 1, LastIncludedIndex: 1, LastIncludedTerm: 1,
				Config: &raftkvv1.ClusterConfig{Members: []*raftkvv1.Member{{Id: 1}}},
			}
			m.FormatVersion = snapshot.FormatVersion + 1
			m.Checksum = snapshot.Checksum(m)
			sd := filepath.Join(dir, node.SnapDir, "0000000000000001-0000000000000001")
			if err := os.MkdirAll(sd, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := snapshot.WriteMeta(sd, m); err != nil {
				t.Fatal(err)
			}
		}, "unsupported format version: 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "d")
			if _, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1}); err != nil {
				t.Fatal(err)
			}
			tc.prepare(t, dir)
			var out, errOut bytes.Buffer
			code := run([]string{"-node-id", "1", "-data-dir", dir, "-peers", "1=127.0.0.1:0",
				"-listen", "127.0.0.1:0"}, &out, &errOut)
			if code != exitConfig || !strings.Contains(errOut.String(), tc.want) {
				t.Fatalf("exit %d, stderr %q; want exit %d and %q", code, errOut.String(), exitConfig, tc.want)
			}
		})
	}
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
