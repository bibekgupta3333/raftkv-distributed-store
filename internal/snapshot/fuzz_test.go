package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// WBS 7.5.1: snapshot.meta is read from disk at startup and arrives over the
// network as the first InstallSnapshot message. DecodeMeta must never panic,
// and what it accepts must survive a write/read round trip unchanged.
func FuzzDecodeMeta(f *testing.F) {
	m := &Meta{
		ClusterId: 7, LastIncludedIndex: 100, LastIncludedTerm: 3,
		Config: &raftkvv1.ClusterConfig{Members: []*raftkvv1.Member{
			{Id: 1, PeerUrl: "raftkv-0:7000", Role: raftkvv1.MemberRole_VOTER},
			{Id: 2, Role: raftkvv1.MemberRole_LEARNER},
		}},
		Files: []*raftkvv1.SnapshotFile{{Name: "000001.sst", Size: 10, Crc32C: 0xabc}, {Name: "MANIFEST-000002", Size: 3}},
	}
	Seal(m)
	good, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	newer, _ := proto.Clone(m).(*Meta)
	newer.FormatVersion = FormatVersion + 1
	newer.Checksum = Checksum(newer)
	b, _ := proto.Marshal(newer)
	f.Add(b)
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01})
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := DecodeMeta(b)
		if err != nil {
			if !errors.Is(err, ErrChecksum) && !errors.Is(err, ErrFormat) && got != nil {
				t.Fatalf("error %v with a non-nil result", err)
			}
			return
		}
		if got.GetFormatVersion() != FormatVersion {
			t.Fatalf("accepted format version %d", got.GetFormatVersion())
		}
		enc, err := proto.MarshalOptions{Deterministic: true}.Marshal(got)
		if err != nil {
			t.Fatalf("re-marshal: %v", err)
		}
		back, err := DecodeMeta(enc)
		if err != nil {
			t.Fatalf("re-encoded metadata no longer decodes: %v", err)
		}
		if !proto.Equal(back, got) {
			t.Fatalf("round trip changed the metadata: %v -> %v", got, back)
		}
	})
}

// FuzzParseName: an accepted snapshot directory name is the name dirName
// gives the parsed index and term, up to hex letter case (Open deletes a
// directory whose name is not exactly canonical).
func FuzzParseName(f *testing.F) {
	f.Add("0000000000186a00-0000000000000007")
	f.Add("tmp-0000000000000099")
	f.Add("+000000000186a00-0000000000000007")
	f.Fuzz(func(t *testing.T, name string) {
		i, term, ok := parseName(name)
		if !ok {
			return
		}
		if want := dirName(&Meta{LastIncludedIndex: i, LastIncludedTerm: term}); !strings.EqualFold(want, name) {
			t.Fatalf("accepted %q, which encodes as %q", name, want)
		}
	})
}

// WBS 7.5.2: a complete snapshot whose metadata carries another
// format_version makes Open fail with ErrFormat instead of deleting it as a
// crash leftover, so the node refuses to start.
func TestOpenRefusesUnknownFormat(t *testing.T) {
	data := t.TempDir()
	n := openNode(t, data, nil)
	n.put(5, 8)
	meta, err := n.mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	n.close()
	dir := n.mgr.Dir(meta)
	newer, _ := proto.Clone(meta).(*Meta)
	newer.FormatVersion = FormatVersion + 1
	newer.Checksum = Checksum(newer)
	if err := WriteMeta(dir, newer); err != nil {
		t.Fatal(err)
	}
	live, err := storage.PrepareStateDir(filepath.Join(data, "sm"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(storage.Options{Dir: live})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	_, err = Open(Config{Dir: filepath.Join(data, "snap"), StateDir: filepath.Join(data, "sm"), Store: st, ClusterID: 7})
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Open = %v, want ErrFormat", err)
	}
	if _, err := os.Stat(filepath.Join(dir, MetaFile)); err != nil {
		t.Fatalf("the unreadable snapshot was deleted: %v", err)
	}
}
