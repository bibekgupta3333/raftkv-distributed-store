package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func ents(from, to, term uint64) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Index: i, Term: term, Type: 1, Data: []byte{byte(i), byte(term)}})
	}
	return out
}

func mustOpen(t *testing.T, dir string, opts Options) (*WAL, *Recovered) {
	t.Helper()
	w, rec, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w, rec
}

func mustNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	names, err := OSFS{}.List(dir)
	mustNil(t, err)
	return names
}

func TestAppendSyncReopen(t *testing.T) {
	dir := t.TempDir()
	w, rec := mustOpen(t, dir, Options{})
	if len(rec.Entries) != 0 || !rec.HardState.IsEmpty() {
		t.Fatalf("fresh wal not empty: %+v", rec)
	}
	mustNil(t, w.AppendEntries(ents(1, 5, 1)))
	mustNil(t, w.SetHardState(HardState{Term: 1, Vote: 2, Commit: 3}))
	mustNil(t, w.Sync())
	mustNil(t, w.Close())

	w, rec = mustOpen(t, dir, Options{})
	defer func() { _ = w.Close() }()
	if !reflect.DeepEqual(rec.Entries, ents(1, 5, 1)) {
		t.Fatalf("entries: %+v", rec.Entries)
	}
	if rec.HardState != (HardState{Term: 1, Vote: 2, Commit: 3}) {
		t.Fatalf("hs: %+v", rec.HardState)
	}
	if w.LastIndex() != 5 {
		t.Fatalf("last index %d", w.LastIndex())
	}
	// Keep appending after reopen.
	mustNil(t, w.AppendEntries(ents(6, 7, 1)))
}

func TestLastHardStateWins(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	for i := uint64(1); i <= 3; i++ {
		mustNil(t, w.SetHardState(HardState{Term: i, Vote: i}))
	}
	mustNil(t, w.Close())
	_, rec := mustOpen(t, dir, Options{})
	if rec.HardState.Term != 3 {
		t.Fatalf("hs %+v", rec.HardState)
	}
}

func TestConflictingAppendTruncatesAfter(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	mustNil(t, w.AppendEntries(ents(1, 6, 1)))
	mustNil(t, w.SetHardState(HardState{Term: 2, Commit: 2}))
	mustNil(t, w.AppendEntries(ents(4, 5, 2))) // overwrites 4..6
	mustNil(t, w.Close())
	_, rec := mustOpen(t, dir, Options{})
	want := append(ents(1, 3, 1), ents(4, 5, 2)...)
	if !reflect.DeepEqual(rec.Entries, want) {
		t.Fatalf("got %+v want %+v", rec.Entries, want)
	}
}

func TestConflictOverCommittedIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	mustNil(t, w.AppendEntries(ents(1, 5, 1)))
	mustNil(t, w.SetHardState(HardState{Term: 1, Commit: 4}))
	mustNil(t, w.AppendEntries(ents(3, 3, 2))) // index 3 <= commit 4
	mustNil(t, w.Close())
	if _, _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestOutOfOrderAppendRejected(t *testing.T) {
	w, _ := mustOpen(t, t.TempDir(), Options{})
	mustNil(t, w.AppendEntries(ents(1, 2, 1)))
	if err := w.AppendEntries(ents(5, 5, 1)); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("gap accepted: %v", err)
	}
	bad := []Entry{{Index: 3}, {Index: 5}}
	if err := w.AppendEntries(bad); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("non-contiguous batch accepted: %v", err)
	}
}

func TestRolloverAtBoundary(t *testing.T) {
	dir := t.TempDir()
	rec := Record{Type: RecordEntry, Payload: make([]byte, 4)} // one entry, no data
	one := int64(rec.EncodedLen())
	// Room for exactly 3 entries per segment (header 64 + 3 records).
	w, _ := mustOpen(t, dir, Options{SegmentSize: segmentHeaderSize + 3*one})
	for i := uint64(1); i <= 3; i++ {
		mustNil(t, w.AppendEntries([]Entry{{Index: i, Term: 1}}))
	}
	if w.SegmentCount() != 1 {
		t.Fatalf("rolled too early: %d segments", w.SegmentCount())
	}
	mustNil(t, w.AppendEntries([]Entry{{Index: 4, Term: 1}}))
	if w.SegmentCount() != 2 {
		t.Fatalf("did not roll at boundary: %d segments", w.SegmentCount())
	}
	for i := uint64(5); i <= 10; i++ {
		mustNil(t, w.AppendEntries([]Entry{{Index: i, Term: 1}}))
	}
	mustNil(t, w.Close())
	files := segFiles(t, dir)
	if len(files) != 4 || files[1] != segmentName(2, 4) {
		t.Fatalf("segment files: %v", files)
	}
	_, got := mustOpen(t, dir, Options{SegmentSize: segmentHeaderSize + 3*one})
	if len(got.Entries) != 10 {
		t.Fatalf("replayed %d entries across segments", len(got.Entries))
	}
}

func TestRolloverSyncsDirAndCarriesHardState(t *testing.T) {
	dir := t.TempDir()
	ffs := NewFaultFS(OSFS{})
	w, _ := mustOpen(t, dir, Options{FS: ffs, SegmentSize: 200})
	base := ffs.DirSyncs()
	mustNil(t, w.SetHardState(HardState{Term: 9, Vote: 1, Commit: 0}))
	mustNil(t, w.AppendEntries(ents(1, 8, 1)))
	if w.SegmentCount() < 2 {
		t.Fatal("expected rollover")
	}
	if ffs.DirSyncs() <= base {
		t.Fatal("directory not fsynced on segment create")
	}
	mustNil(t, w.Close())
	// Drop all segments but the last: hard state must live in its preamble.
	names := segFiles(t, dir)
	for _, n := range names[:len(names)-1] {
		mustNil(t, os.Remove(filepath.Join(dir, n)))
	}
	_, rec := mustOpen(t, dir, Options{FS: ffs, SegmentSize: 200})
	if rec.HardState.Term != 9 {
		t.Fatalf("hard state lost with old segments: %+v", rec.HardState)
	}
}

func TestReleaseTo(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 200})
	mustNil(t, w.SetHardState(HardState{Term: 2, Vote: 1}))
	for i := uint64(1); i <= 20; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 2)))
	}
	mustNil(t, w.Sync())
	before := w.SegmentCount()
	if before < 3 {
		t.Fatalf("need several segments, have %d", before)
	}
	mustNil(t, w.ReleaseTo(0))
	if w.SegmentCount() != before {
		t.Fatal("ReleaseTo(0) deleted something")
	}
	mustNil(t, w.ReleaseTo(1000)) // never deletes the active segment
	if w.SegmentCount() != 1 {
		t.Fatalf("want only active segment, have %d", w.SegmentCount())
	}
	mustNil(t, w.AppendEntries(ents(21, 21, 2)))
	mustNil(t, w.Close())
	_, rec := mustOpen(t, dir, Options{SegmentSize: 200})
	if len(rec.Entries) == 0 || rec.Entries[len(rec.Entries)-1].Index != 21 {
		t.Fatalf("entries after release: %+v", rec.Entries)
	}
	if rec.HardState.Term != 2 {
		t.Fatalf("hard state lost: %+v", rec.HardState)
	}
	if len(segFiles(t, dir)) > 2 {
		t.Fatalf("files left: %v", segFiles(t, dir))
	}
}

func TestReleaseToKeepsSegmentWithNeededEntries(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 200})
	for i := uint64(1); i <= 20; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 1)))
	}
	n := w.SegmentCount()
	mustNil(t, w.ReleaseTo(5))
	if w.SegmentCount() >= n || w.SegmentCount() < 2 {
		t.Fatalf("partial release failed: %d -> %d", n, w.SegmentCount())
	}
	mustNil(t, w.Close())
	_, rec := mustOpen(t, dir, Options{SegmentSize: 200})
	if rec.Entries[len(rec.Entries)-1].Index != 20 || rec.Entries[0].Index > 6 {
		t.Fatalf("entries needed after index 5 lost: first=%d", rec.Entries[0].Index)
	}
}

func lastSegPath(t *testing.T, dir string) string {
	t.Helper()
	names := segFiles(t, dir)
	return filepath.Join(dir, names[len(names)-1])
}

func TestTornTailIsCut(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	mustNil(t, w.AppendEntries(ents(1, 4, 1)))
	mustNil(t, w.Close())
	p := lastSegPath(t, dir)
	fi, err := os.Stat(p)
	mustNil(t, err)
	mustNil(t, NewFaultFS(OSFS{}).TruncateFile(p, fi.Size()-5))

	w, rec := mustOpen(t, dir, Options{})
	if len(rec.Entries) != 3 || rec.TruncatedBytes == 0 {
		t.Fatalf("entries=%d truncated=%d", len(rec.Entries), rec.TruncatedBytes)
	}
	// The cut is durable: appending continues cleanly and reopens identically.
	mustNil(t, w.AppendEntries(ents(4, 4, 2)))
	mustNil(t, w.Close())
	_, rec = mustOpen(t, dir, Options{})
	if len(rec.Entries) != 4 || rec.Entries[3].Term != 2 || rec.TruncatedBytes != 0 {
		t.Fatalf("after repair: %+v", rec)
	}
}

func TestMidLogCorruptionRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	mustNil(t, w.AppendEntries(ents(1, 5, 1)))
	mustNil(t, w.Close())
	// Corrupt a byte inside the first record: valid records follow.
	mustNil(t, NewFaultFS(OSFS{}).CorruptByte(lastSegPath(t, dir), segmentHeaderSize+10, 0xff))
	if _, _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestSealedSegmentCorruptionRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 200})
	for i := uint64(1); i <= 12; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 1)))
	}
	mustNil(t, w.Close())
	first := filepath.Join(dir, segFiles(t, dir)[0])
	fi, err := os.Stat(first)
	mustNil(t, err)
	// Even damage to the very last record of a sealed segment is fatal.
	mustNil(t, NewFaultFS(OSFS{}).CorruptByte(first, fi.Size()-9, 0x01))
	if _, _, err := Open(dir, Options{SegmentSize: 200}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestSegmentGapAndHeaderMismatchRefuse(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 200})
	for i := uint64(1); i <= 12; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 1)))
	}
	mustNil(t, w.Close())
	names := segFiles(t, dir)
	mustNil(t, os.Remove(filepath.Join(dir, names[1]))) // seq gap
	if _, _, err := Open(dir, Options{SegmentSize: 200}); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "gap") {
		t.Fatalf("seq gap: %v", err)
	}

	dir2 := t.TempDir()
	w, _ = mustOpen(t, dir2, Options{ClusterID: 1, MemberID: 1})
	mustNil(t, w.Close())
	if _, _, err := Open(dir2, Options{ClusterID: 2, MemberID: 1}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("cluster id mismatch: %v", err)
	}
}

func TestTornSegmentHeaderIsDropped(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 200})
	for i := uint64(1); i <= 6; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 1)))
	}
	mustNil(t, w.Close())
	// Simulate a crash mid-rotation: a new segment with a partial header.
	next := filepath.Join(dir, segmentName(uint64(len(segFiles(t, dir)))+1, 7))
	mustNil(t, os.WriteFile(next, make([]byte, 10), 0o644))
	w, rec := mustOpen(t, dir, Options{SegmentSize: 200})
	defer func() { _ = w.Close() }()
	if len(rec.Entries) != 6 {
		t.Fatalf("entries %d", len(rec.Entries))
	}
	if _, err := os.Stat(next); !os.IsNotExist(err) {
		t.Fatal("torn segment not removed")
	}
}

// D-33: fsync/write errors are fatal, the hook runs once, and nothing is retried.
func TestFsyncErrorIsFatalAndNotRetried(t *testing.T) {
	ffs := NewFaultFS(OSFS{})
	var fatals []error
	w, _ := mustOpen(t, t.TempDir(), Options{FS: ffs, OnFatal: func(err error) { fatals = append(fatals, err) }})
	mustNil(t, w.AppendEntries(ents(1, 2, 1)))
	syncsBefore := ffs.Syncs()
	ffs.FailSyncOn(1)
	err := w.Sync()
	if !errors.Is(err, ErrFailed) || !errors.Is(err, ErrInjected) {
		t.Fatalf("sync err: %v", err)
	}
	if len(fatals) != 1 || !errors.Is(fatals[0], ErrInjected) {
		t.Fatalf("onFatal calls: %v", fatals)
	}
	// Every later call is refused without touching the disk.
	writes := ffs.Writes()
	if err := w.Sync(); !errors.Is(err, ErrFailed) {
		t.Fatalf("retry sync: %v", err)
	}
	if err := w.AppendEntries(ents(3, 3, 1)); !errors.Is(err, ErrFailed) {
		t.Fatalf("append after fatal: %v", err)
	}
	if ffs.Syncs() != syncsBefore+1 || ffs.Writes() != writes || len(fatals) != 1 {
		t.Fatalf("fsync retried: syncs %d->%d writes %d->%d fatals %d",
			syncsBefore, ffs.Syncs(), writes, ffs.Writes(), len(fatals))
	}
}

func TestWriteErrorIsFatal(t *testing.T) {
	ffs := NewFaultFS(OSFS{})
	calls := 0
	w, _ := mustOpen(t, t.TempDir(), Options{FS: ffs, OnFatal: func(error) { calls++ }})
	ffs.FailWriteOn(1)
	if err := w.AppendEntries(ents(1, 1, 1)); !errors.Is(err, ErrFailed) {
		t.Fatalf("append: %v", err)
	}
	if calls != 1 || ffs.Writes() != 2 { // header write at create + the failed one
		t.Fatalf("calls=%d writes=%d", calls, ffs.Writes())
	}
	if err := w.SetHardState(HardState{Term: 1}); !errors.Is(err, ErrFailed) || ffs.Writes() != 2 {
		t.Fatalf("write retried: %v writes=%d", err, ffs.Writes())
	}
}

func TestDirSyncErrorOnRolloverIsFatal(t *testing.T) {
	ffs := NewFaultFS(OSFS{})
	calls := 0
	w, _ := mustOpen(t, t.TempDir(), Options{FS: ffs, SegmentSize: 150, OnFatal: func(error) { calls++ }})
	ffs.FailSyncDirOn(1)
	var err error
	for i := uint64(1); i <= 10 && err == nil; i++ {
		err = w.AppendEntries(ents(i, i, 1))
	}
	if !errors.Is(err, ErrFailed) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestFaultFSShim(t *testing.T) {
	dir := t.TempDir()
	ffs := NewFaultFS(OSFS{})
	p := filepath.Join(dir, "f")
	f, err := ffs.Create(p)
	mustNil(t, err)
	_, err = f.Write([]byte{1, 2, 3, 4})
	mustNil(t, err)
	mustNil(t, f.Close())

	mustNil(t, ffs.CorruptByte(p, 2, 0x0f))
	got, _ := os.ReadFile(p)
	if !reflect.DeepEqual(got, []byte{1, 2, 3 ^ 0x0f, 4}) {
		t.Fatalf("corrupt: %v", got)
	}
	if ffs.CorruptByte(p, 99, 1) == nil || ffs.CorruptByte(p, 0, 0) == nil {
		t.Fatal("bad corrupt requests accepted")
	}
	mustNil(t, ffs.TruncateFile(p, 1))
	if got, _ = os.ReadFile(p); len(got) != 1 {
		t.Fatalf("truncate: %v", got)
	}

	f, err = ffs.OpenAppend(p)
	mustNil(t, err)
	ffs.FailWriteOn(2)
	ffs.FailSyncOn(2)
	_, err = f.Write([]byte{9})
	mustNil(t, err)
	if _, err = f.Write([]byte{9}); !errors.Is(err, ErrInjected) {
		t.Fatalf("write fault: %v", err)
	}
	mustNil(t, f.Sync())
	if err = f.Sync(); !errors.Is(err, ErrInjected) {
		t.Fatalf("sync fault: %v", err)
	}
	mustNil(t, f.Sync()) // only the n-th call fails
	ffs.FailSyncDirOn(1)
	if err = ffs.SyncDir(dir); !errors.Is(err, ErrInjected) {
		t.Fatalf("dir fault: %v", err)
	}
	mustNil(t, f.Close())
}
