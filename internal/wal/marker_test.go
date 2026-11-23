package wal

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

// SNAPSHOT_MARKER records (storage-design 1.4, 1.8, 4.4; D-14, D-30).

func TestMarkerRecordRoundTrip(t *testing.T) {
	for _, reset := range []bool{false, true} {
		rec := markerRecord(Start{Index: 42, Term: 7}, reset)
		got, n, err := DecodeRecord(rec.Encode(nil))
		mustNil(t, err)
		if n != rec.EncodedLen() {
			t.Fatalf("decoded %d bytes, want %d", n, rec.EncodedLen())
		}
		s, r, err := markerFromRecord(got)
		mustNil(t, err)
		if s != (Start{Index: 42, Term: 7}) || r != reset {
			t.Fatalf("marker = %+v reset=%v, want 42/7 reset=%v", s, r, reset)
		}
	}
	bad := Record{Type: RecordSnapshotMarker, Index: 1, Term: 1, Payload: []byte{2, 0, 0, 0, 0, 0, 0, 0}}
	if _, _, err := markerFromRecord(bad); !errors.Is(err, errBadRecord) {
		t.Fatalf("unknown flags: err = %v, want errBadRecord", err)
	}
}

func TestMarkCompactedReleaseAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentSize: 200}
	w, _ := mustOpen(t, dir, opts)
	mustNil(t, w.SetHardState(HardState{Term: 3, Vote: 1, Commit: 20}))
	for i := uint64(1); i <= 20; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 3)))
	}
	before := w.SegmentCount()
	// storage-design 1.8: marker + fsync, then delete whole segments.
	mustNil(t, w.MarkCompacted(Start{Index: 12, Term: 3}))
	mustNil(t, w.Sync())
	mustNil(t, w.ReleaseTo(12))
	if w.SegmentCount() >= before {
		t.Fatalf("nothing released: %d segments", w.SegmentCount())
	}
	// Rollover after the trim: the new segment's preamble carries the start.
	for i := uint64(21); i <= 30; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 3)))
	}
	mustNil(t, w.Close())

	w, rec := mustOpen(t, dir, opts)
	if rec.Start != (Start{Index: 12, Term: 3}) {
		t.Fatalf("Start = %+v, want 12/3", rec.Start)
	}
	if !reflect.DeepEqual(rec.Entries, ents(13, 30, 3)) {
		t.Fatalf("entries = %d..%d, want 13..30", rec.Entries[0].Index, rec.Entries[len(rec.Entries)-1].Index)
	}
	// Trim again and delete everything but the active segment: the start
	// survives in the newest preamble only.
	mustNil(t, w.MarkCompacted(Start{Index: 28, Term: 3}))
	mustNil(t, w.Sync())
	mustNil(t, w.ReleaseTo(28))
	mustNil(t, w.Close())
	_, rec = mustOpen(t, dir, opts)
	if rec.Start.Index != 28 || rec.Entries[0].Index != 29 {
		t.Fatalf("after second trim: start %+v first entry %d", rec.Start, rec.Entries[0].Index)
	}
}

func TestMarkCompactedRejectsOutOfRange(t *testing.T) {
	w, _ := mustOpen(t, t.TempDir(), Options{})
	defer func() { _ = w.Close() }()
	mustNil(t, w.AppendEntries(ents(1, 5, 1)))
	if err := w.MarkCompacted(Start{Index: 6, Term: 1}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("compaction beyond the last entry: err = %v", err)
	}
	mustNil(t, w.MarkCompacted(Start{Index: 3, Term: 1}))
	if err := w.MarkCompacted(Start{Index: 2, Term: 1}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("compaction point going back: err = %v", err)
	}
}

func TestResetToDropsTheWholeLog(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentSize: 200}
	w, _ := mustOpen(t, dir, opts)
	// A follower with a divergent log (term 1) above the snapshot index.
	mustNil(t, w.AppendEntries(ents(1, 15, 1)))
	mustNil(t, w.SetHardState(HardState{Term: 1, Commit: 3}))
	// Install a snapshot at 10 whose term (4) differs: drop everything.
	mustNil(t, w.ResetTo(Start{Index: 10, Term: 4}))
	mustNil(t, w.SetHardState(HardState{Term: 4, Commit: 10}))
	mustNil(t, w.Sync())
	if w.LastIndex() != 10 {
		t.Fatalf("LastIndex after reset = %d, want 10", w.LastIndex())
	}
	if err := w.AppendEntries(ents(12, 12, 4)); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("gap after reset: err = %v", err)
	}
	mustNil(t, w.AppendEntries(ents(11, 14, 4)))
	mustNil(t, w.Close())

	w, rec := mustOpen(t, dir, opts)
	if rec.Start != (Start{Index: 10, Term: 4}) {
		t.Fatalf("Start = %+v", rec.Start)
	}
	if !reflect.DeepEqual(rec.Entries, ents(11, 14, 4)) {
		t.Fatalf("entries = %+v, want 11..14 of term 4 (stale entries dropped)", rec.Entries)
	}
	if rec.HardState.Commit != 10 {
		t.Fatalf("HardState = %+v", rec.HardState)
	}
	// Later, every segment up to the reset is released; the preamble of the
	// next segment still says where the log starts.
	for i := uint64(15); i <= 30; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 4)))
	}
	mustNil(t, w.MarkCompacted(Start{Index: 25, Term: 4}))
	mustNil(t, w.Sync())
	mustNil(t, w.ReleaseTo(25))
	mustNil(t, w.Close())
	_, rec = mustOpen(t, dir, opts)
	if rec.Start.Index != 25 || rec.Entries[0].Index != 26 || rec.Entries[len(rec.Entries)-1].Index != 30 {
		t.Fatalf("after release: start %+v entries %d..%d", rec.Start, rec.Entries[0].Index, rec.Entries[len(rec.Entries)-1].Index)
	}
}

func TestResetOnEmptyLogThenReopen(t *testing.T) {
	// A wiped node installs its first snapshot: the log is empty before.
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	mustNil(t, w.ResetTo(Start{Index: 500, Term: 9}))
	mustNil(t, w.SetHardState(HardState{Term: 9, Commit: 500}))
	mustNil(t, w.Sync())
	mustNil(t, w.Close())
	w, rec := mustOpen(t, dir, Options{})
	defer func() { _ = w.Close() }()
	if rec.Start != (Start{Index: 500, Term: 9}) || len(rec.Entries) != 0 || w.LastIndex() != 500 {
		t.Fatalf("recovered %+v last %d", rec, w.LastIndex())
	}
	mustNil(t, w.AppendEntries(ents(501, 502, 9)))
}

func TestCrashDuringTrimLeavesAValidLog(t *testing.T) {
	// The trim deletes segments one by one in seq order; a crash may stop
	// it anywhere. Simulate by deleting only some segments by hand.
	dir := t.TempDir()
	opts := Options{SegmentSize: 200}
	w, _ := mustOpen(t, dir, opts)
	for i := uint64(1); i <= 30; i++ {
		mustNil(t, w.AppendEntries(ents(i, i, 2)))
	}
	mustNil(t, w.MarkCompacted(Start{Index: 20, Term: 2}))
	mustNil(t, w.Sync())
	mustNil(t, w.Close())
	names := segFiles(t, dir)
	if len(names) < 4 {
		t.Fatalf("want several segments, have %v", names)
	}
	for k := 0; k <= 2; k++ { // crash after deleting k segments
		if k > 0 {
			mustNil(t, os.Remove(dir+"/"+names[k-1]))
		}
		w, rec := mustOpen(t, dir, opts)
		if rec.Start.Index != 20 || rec.Entries[0].Index != 21 || rec.Entries[len(rec.Entries)-1].Index != 30 {
			t.Fatalf("crash after %d deletions: start %+v entries %d..", k, rec.Start, rec.Entries[0].Index)
		}
		mustNil(t, w.Close())
	}
}

func TestEntryBelowLogStartIsCorrupt(t *testing.T) {
	r := &replayer{}
	mustNil(t, r.apply(markerRecord(Start{Index: 10, Term: 2}, true)))
	if err := r.apply(ents(5, 5, 2)[0].record()); err == nil {
		t.Fatal("an entry at or below the log start must be rejected")
	}
	if err := r.apply(markerRecord(Start{Index: 9, Term: 2}, false)); err == nil {
		t.Fatal("a trim marker below the log start must be rejected")
	}
}
