package wal

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// DefaultSegmentSize is the rollover size (D-03, 64 MiB).
const DefaultSegmentSize = 64 << 20

// FatalExitCode is the process exit code for a disk write/fsync failure (D-33).
const FatalExitCode = 75

// Errors returned by WAL operations.
var (
	// ErrFailed is returned by every operation after a fatal I/O error.
	ErrFailed = errors.New("wal: failed after fatal I/O error")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("wal: closed")
	// ErrOutOfOrder is returned for entries that are not contiguous with the log.
	ErrOutOfOrder = errors.New("wal: entries not contiguous with the log")
)

// Options configure Open. The zero value uses the real file system, 64 MiB
// segments and a process-exiting fatal hook.
type Options struct {
	FS FS
	// SegmentSize is the rollover size in bytes (DefaultSegmentSize if 0).
	SegmentSize int64
	// ClusterID and MemberID are stamped into every segment header and
	// checked on replay (§1.2).
	ClusterID, MemberID uint64
	// OnFatal is called once, with the cause, on any write/fsync/dir-fsync
	// error (D-33). It is never retried. The default logs and exits with
	// FatalExitCode; tests inject their own. If it returns, the WAL stays
	// failed and rejects all further calls with ErrFailed.
	OnFatal func(error)
}

func (o Options) withDefaults() Options {
	if o.FS == nil {
		o.FS = OSFS{}
	}
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}
	if o.OnFatal == nil {
		o.OnFatal = defaultOnFatal
	}
	return o
}

func defaultOnFatal(err error) {
	log.Printf("FATAL: wal I/O error, exiting: %v", err)
	os.Exit(FatalExitCode)
}

// WAL is the segmented write-ahead log. Safe for concurrent use.
type WAL struct {
	mu   sync.Mutex
	dir  string
	opts Options

	segs      []segInfo // oldest first; the last one is active
	active    File
	size      int64 // bytes in the active segment
	hasRecs   bool  // active segment holds records beyond header+preamble
	dirty     bool  // unsynced writes exist
	lastIndex uint64
	hs        HardState
	hasHS     bool

	failed error
	closed bool
}

// Open opens (creating if needed) the WAL in dir, replays it and repairs a
// torn tail. Corruption elsewhere returns an error wrapping ErrCorrupt (D-33).
func Open(dir string, opts Options) (*WAL, *Recovered, error) {
	opts = opts.withDefaults()
	if err := opts.FS.MkdirAll(dir); err != nil {
		return nil, nil, err
	}
	segs, rec, err := replayDir(opts.FS, dir, opts)
	if err != nil {
		return nil, nil, err
	}
	w := &WAL{dir: dir, opts: opts, segs: segs, hs: rec.HardState, hasHS: !rec.HardState.IsEmpty()}
	if n := len(rec.Entries); n > 0 {
		w.lastIndex = rec.Entries[n-1].Index
	}
	if len(segs) == 0 {
		if err := w.newSegment(1); err != nil {
			return nil, nil, err
		}
		return w, rec, nil
	}
	tail := segs[len(segs)-1]
	f, err := opts.FS.OpenAppend(w.path(tail.name))
	if err != nil {
		return nil, nil, err
	}
	data, err := opts.FS.ReadFile(w.path(tail.name))
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	w.active, w.size = f, int64(len(data))
	w.hasRecs = len(data) > segmentHeaderSize
	return w, rec, nil
}

func (w *WAL) path(name string) string { return filepath.Join(w.dir, name) }

// fatal records an unrecoverable I/O error, tells the hook exactly once, and
// poisons the WAL. Never retry a failed fsync (D-33).
func (w *WAL) fatal(err error) error {
	if w.failed == nil {
		w.failed = err
		w.opts.OnFatal(err)
	}
	return fmt.Errorf("%w: %w", ErrFailed, err)
}

func (w *WAL) check() error {
	switch {
	case w.failed != nil:
		return fmt.Errorf("%w: %w", ErrFailed, w.failed)
	case w.closed:
		return ErrClosed
	}
	return nil
}

// newSegment creates segment seq starting at lastIndex+1, writes its header and
// preamble (the current HardState, so older segments can be deleted), fsyncs the
// file and then the directory (§1.1). Errors are fatal.
func (w *WAL) newSegment(seq uint64) error {
	first := w.lastIndex + 1
	name := segmentName(seq, first)
	f, err := w.opts.FS.Create(w.path(name))
	if err != nil {
		return w.fatal(err)
	}
	buf := segmentHeader{Seq: seq, FirstIndex: first, ClusterID: w.opts.ClusterID, MemberID: w.opts.MemberID}.encode()
	if w.hasHS {
		buf = w.hs.record().Encode(buf)
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return w.fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return w.fatal(err)
	}
	if err := w.opts.FS.SyncDir(w.dir); err != nil { // D-33: directory fsync on create
		_ = f.Close()
		return w.fatal(err)
	}
	w.segs = append(w.segs, segInfo{seq: seq, firstIndex: first, name: name})
	w.active, w.size, w.hasRecs, w.dirty = f, int64(len(buf)), false, false
	return nil
}

// rotate seals the active segment (fsync) and starts the next one.
func (w *WAL) rotate() error {
	if err := w.active.Sync(); err != nil {
		return w.fatal(err)
	}
	if err := w.active.Close(); err != nil {
		return w.fatal(err)
	}
	return w.newSegment(w.segs[len(w.segs)-1].seq + 1)
}

// write appends an encoded buffer, rotating first if it would pass the size
// limit; a write is never split across segments.
func (w *WAL) write(buf []byte) error {
	if w.hasRecs && w.size+int64(len(buf)) > w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	if _, err := w.active.Write(buf); err != nil {
		return w.fatal(err)
	}
	w.size += int64(len(buf))
	w.hasRecs, w.dirty = true, true
	return nil
}

// AppendEntries writes entries; they are durable only after Sync. Indexes must
// be contiguous. If entries[0].Index <= LastIndex the new entries replace the
// conflicting suffix; replay applies truncate-after semantics (§1.7).
func (w *WAL) AppendEntries(entries []Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.check(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if w.lastIndex != 0 && entries[0].Index > w.lastIndex+1 {
		return fmt.Errorf("%w: entry %d after last index %d", ErrOutOfOrder, entries[0].Index, w.lastIndex)
	}
	var buf []byte
	for i, e := range entries {
		if e.Index != entries[0].Index+uint64(i) {
			return fmt.Errorf("%w: entry %d at position %d", ErrOutOfOrder, e.Index, i)
		}
		if len(e.Data)+4 > MaxPayload {
			return fmt.Errorf("wal: entry %d payload too large", e.Index)
		}
		buf = e.record().Encode(buf)
	}
	if err := w.write(buf); err != nil {
		return err
	}
	last := entries[len(entries)-1].Index
	w.lastIndex = last
	if s := &w.segs[len(w.segs)-1]; last > s.maxIndex {
		s.maxIndex = last
	}
	return nil
}

// SetHardState writes a HARD_STATE record; durable after Sync.
func (w *WAL) SetHardState(hs HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.check(); err != nil {
		return err
	}
	if err := w.write(hs.record().Encode(nil)); err != nil {
		return err
	}
	w.hs, w.hasHS = hs, true
	return nil
}

// Sync fsyncs the active segment. An error is fatal and is never retried (D-33).
// Earlier segments were fsynced when they were sealed.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.check(); err != nil {
		return err
	}
	if !w.dirty {
		return nil
	}
	if err := w.active.Sync(); err != nil {
		return w.fatal(err)
	}
	w.dirty = false
	return nil
}

// ReleaseTo deletes whole old segments whose entries all have index <= index.
// It never deletes the active segment and stops at the first segment that must
// stay, so a crash midway leaves a deleted prefix, which recovery accepts.
// D-30: the caller must pass a bound <= min(snapshotIndex, durableAppliedIndex).
func (w *WAL) ReleaseTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.check(); err != nil {
		return err
	}
	n := 0
	for n < len(w.segs)-1 && w.segs[n].maxIndex <= index {
		if err := w.opts.FS.Remove(w.path(w.segs[n].name)); err != nil {
			return w.fatal(err)
		}
		n++
	}
	if n == 0 {
		return nil
	}
	w.segs = append([]segInfo(nil), w.segs[n:]...)
	if err := w.opts.FS.SyncDir(w.dir); err != nil {
		return w.fatal(err)
	}
	return nil
}

// LastIndex returns the index of the last appended entry (0 if none).
func (w *WAL) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex
}

// SegmentCount returns the number of segment files currently held.
func (w *WAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segs)
}

// Close syncs (unless failed) and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.failed != nil {
		_ = w.active.Close()
		return nil
	}
	if w.dirty {
		if err := w.active.Sync(); err != nil {
			_ = w.active.Close()
			w.closed = false
			return w.fatal(err)
		}
	}
	return w.active.Close()
}
