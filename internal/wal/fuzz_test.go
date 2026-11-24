package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// WBS 7.5.1: fuzz targets for everything the WAL decodes from disk. Each must
// never panic and must re-encode what it accepts to the same bytes. Run one
// with: go test ./internal/wal -run '^$' -fuzz '^FuzzDecodeSegmentHeader$'.

// FuzzDecodeSegmentHeader: an accepted header re-encodes to the same 64 bytes,
// and anything that does not carry this build's version is refused.
func FuzzDecodeSegmentHeader(f *testing.F) {
	f.Add(segmentHeader{Seq: 1, FirstIndex: 1, ClusterID: 1, MemberID: 1}.encode())
	f.Add(segmentHeader{Seq: 0x29, FirstIndex: 0x4c4b41, ClusterID: 7, MemberID: 3}.encode())
	newer := segmentHeader{Seq: 1, FirstIndex: 1}.encode()
	copy(newer[0:8], "RKVWAL02")
	f.Add(newer)
	f.Add([]byte("RKVWAL01"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := decodeSegmentHeader(b)
		if err != nil {
			return
		}
		if !bytes.Equal(h.encode(), b[:segmentHeaderSize]) {
			t.Fatal("re-encoded header differs from the accepted bytes")
		}
	})
}

// FuzzParseSegmentName: an accepted file name is exactly what segmentName
// produces for the parsed numbers.
func FuzzParseSegmentName(f *testing.F) {
	f.Add(segmentName(1, 1))
	f.Add(segmentName(0x29, 0x4c4b41))
	f.Add("wal-0000000000000001-0000000000000001.seg.tmp")
	f.Add("wal-+000000000000001-0000000000000001.seg")
	f.Fuzz(func(t *testing.T, name string) {
		seq, first, ok := parseSegmentName(name)
		if ok && segmentName(seq, first) != name {
			t.Fatalf("accepted %q but it encodes as %q", name, segmentName(seq, first))
		}
	})
}

// FuzzReplaySegment feeds arbitrary bytes after a valid header through the
// replay scanner, as both a sealed and the last segment. It must never panic,
// and whatever it keeps must be a contiguous log above the log start.
func FuzzReplaySegment(f *testing.F) {
	// Seed with real segments: entries, hard states, a conflicting append,
	// and a trim marker.
	dir := f.TempDir()
	w, _, err := Open(dir, Options{})
	if err != nil {
		f.Fatal(err)
	}
	if err := w.AppendEntries(ents(1, 5, 1)); err != nil {
		f.Fatal(err)
	}
	if err := w.SetHardState(HardState{Term: 2, Vote: 1, Commit: 3}); err != nil {
		f.Fatal(err)
	}
	if err := w.AppendEntries(ents(4, 6, 2)); err != nil {
		f.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		f.Fatal(err)
	}
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	names, err := OSFS{}.List(dir)
	if err != nil || len(names) == 0 {
		f.Fatalf("list: %v %v", names, err)
	}
	seg, err := os.ReadFile(filepath.Join(dir, names[0]))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seg[segmentHeaderSize:])
	f.Add(append(markerRecord(Start{Index: 2, Term: 1}, false).Encode(nil), ents(3, 3, 1)[0].record().Encode(nil)...))
	f.Add(markerRecord(Start{Index: 9, Term: 4}, true).Encode(nil))
	f.Add([]byte{})

	hdr := segmentHeader{Seq: 1, FirstIndex: 1}.encode()
	f.Fuzz(func(t *testing.T, body []byte) {
		data := append(append([]byte(nil), hdr...), body...)
		for _, last := range []bool{false, true} {
			rp := &replayer{}
			res, err := scanSegment("fuzz", data, last, rp)
			if err != nil {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("scan error does not wrap ErrCorrupt: %v", err)
				}
				continue
			}
			if res.goodEnd < segmentHeaderSize || res.goodEnd > len(data) {
				t.Fatalf("goodEnd %d outside [64, %d]", res.goodEnd, len(data))
			}
			if res.torn && !last {
				t.Fatal("torn tail reported for a sealed segment")
			}
			for i, e := range rp.entries {
				if e.Index <= rp.start.Index || (i > 0 && e.Index != rp.entries[i-1].Index+1) {
					t.Fatalf("replayed log is not contiguous above start %d: %v", rp.start.Index, rp.entries)
				}
			}
		}
	})
}

// WBS 7.5.2: a segment written by another WAL format version is refused with
// ErrFormatVersion (not silently read, not reported as a torn tail), so the
// node refuses to start (D-33 style).
func TestSegmentFormatVersionRefused(t *testing.T) {
	if segmentMagic != segmentMagicStem+"01" || SegmentFormatVersion != 1 {
		t.Fatalf("magic %q and version %d disagree", segmentMagic, SegmentFormatVersion)
	}
	for _, magic := range []string{"RKVWAL02", "RKVWAL99", "RKVWAL00"} {
		dir := t.TempDir()
		w, _ := mustOpen(t, dir, Options{})
		mustNil(t, w.AppendEntries(ents(1, 3, 1)))
		mustNil(t, w.Sync())
		mustNil(t, w.Close())
		path := filepath.Join(dir, segFiles(t, dir)[0])
		data, err := os.ReadFile(path)
		mustNil(t, err)
		copy(data[0:8], magic)
		// Re-seal the header CRC so only the version is wrong.
		h, err := decodeSegmentHeader(append([]byte(segmentMagic), data[8:]...))
		mustNil(t, err)
		enc := h.encode()
		copy(enc[0:8], magic)
		copy(data, enc[:60])
		mustNil(t, os.WriteFile(path, data, 0o644))

		_, _, err = Open(dir, Options{})
		if !errors.Is(err, ErrFormatVersion) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: Open = %v, want ErrFormatVersion", magic, err)
		}
		if !strings.Contains(err.Error(), "this build reads version 1") {
			t.Fatalf("%s: error does not say which version this build reads: %v", magic, err)
		}
	}
	// A magic that is not a version at all is still corruption.
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	mustNil(t, w.AppendEntries(ents(1, 1, 1)))
	mustNil(t, w.Close())
	path := filepath.Join(dir, segFiles(t, dir)[0])
	data, err := os.ReadFile(path)
	mustNil(t, err)
	copy(data[0:8], "RKVWALxy")
	mustNil(t, os.WriteFile(path, data, 0o644))
	if _, _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("garbage magic: %v, want ErrCorrupt", err)
	}
}
