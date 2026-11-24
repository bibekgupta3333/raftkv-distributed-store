package wal

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
)

// ErrCorrupt is returned (wrapped) when the WAL is damaged anywhere other than
// a torn final record. The node must refuse to start (D-33, exit code 78).
var ErrCorrupt = errors.New("wal: corrupt")

// ErrFormatVersion is returned (wrapped) when a segment was written in a WAL
// format this build does not read, such as a newer release's. The node must
// refuse to start rather than misread it (D-33 style, WBS 7.5.2). It is not
// ErrCorrupt: the data may be fine, the binary is the wrong one.
var ErrFormatVersion = errors.New("wal: unsupported format version")

// Recovered is what Open rebuilt from disk.
type Recovered struct {
	// Entries are the surviving log entries in index order, after applying
	// truncate-after semantics for conflicting appends (§1.7).
	Entries []Entry
	// HardState is the last valid HARD_STATE record (zero if none).
	HardState HardState
	// Start is the log start set by the last SNAPSHOT_MARKER (zero if none).
	// Every entry in Entries is above Start.Index.
	Start Start
	// TruncatedBytes is how many torn tail bytes were cut off (D-33).
	TruncatedBytes int64
}

func corruptf(seg string, off int, format string, args ...any) error {
	return fmt.Errorf("%w: segment %s offset %d: %s", ErrCorrupt, seg, off, fmt.Sprintf(format, args...))
}

// replayer applies records in order, rebuilding entries and hard state.
type replayer struct {
	entries []Entry
	hs      HardState
	start   Start
}

func (r *replayer) lastIndex() uint64 {
	if len(r.entries) == 0 {
		return r.start.Index
	}
	return r.entries[len(r.entries)-1].Index
}

// apply handles one record. An ENTRY with index i discards earlier replayed
// entries with index >= i (§1.7); that must never reach committed entries.
func (r *replayer) apply(rec Record) error {
	switch rec.Type {
	case RecordHardState:
		hs, err := hardStateFromRecord(rec)
		if err != nil {
			return err
		}
		r.hs = hs
	case RecordSnapshotMarker:
		s, reset, err := markerFromRecord(rec)
		if err != nil {
			return err
		}
		switch {
		case reset:
			// A snapshot replaced the log (storage-design 4.4 step 6).
			r.entries = nil
			r.start = s
		case s.Index < r.start.Index:
			// An older log start (a segment preamble written before a
			// later marker cannot exist, so this is corruption).
			return fmt.Errorf("%w: snapshot marker %d below log start %d", errBadRecord, s.Index, r.start.Index)
		default:
			// The log was trimmed to s (D-30), or this is the preamble of a
			// segment whose predecessors were deleted: drop entries <=
			// s.Index. A preamble may name a start below the segment's first
			// entry (the entries between were in deleted segments); the trim
			// marker written before that deletion closes the gap, which
			// replayDir checks at the end.
			if len(r.entries) > 0 && s.Index > r.lastIndex() {
				return fmt.Errorf("%w: trim marker %d beyond last entry %d", errBadRecord, s.Index, r.lastIndex())
			}
			k := 0
			for k < len(r.entries) && r.entries[k].Index <= s.Index {
				k++
			}
			if k > 0 && r.entries[k-1].Index == s.Index && r.entries[k-1].Term != s.Term {
				return fmt.Errorf("%w: trim marker term %d differs from entry %d term %d", errBadRecord, s.Term, s.Index, r.entries[k-1].Term)
			}
			r.entries = r.entries[k:]
			r.start = s
		}
	case RecordEntry:
		e, err := entryFromRecord(rec)
		if err != nil {
			return err
		}
		if r.start.Index > 0 && e.Index <= r.start.Index {
			return fmt.Errorf("%w: entry %d at or below the log start %d", errBadRecord, e.Index, r.start.Index)
		}
		if len(r.entries) > 0 {
			first, last := r.entries[0].Index, r.lastIndex()
			switch {
			case e.Index > last+1:
				return fmt.Errorf("%w: gap, entry %d after %d", errBadRecord, e.Index, last)
			case e.Index <= last:
				if e.Index < first || e.Index <= r.hs.Commit {
					return fmt.Errorf("%w: entry %d overwrites committed or released log (commit %d)", errBadRecord, e.Index, r.hs.Commit)
				}
				r.entries = r.entries[:e.Index-first]
			}
		}
		r.entries = append(r.entries, e)
	default:
		return fmt.Errorf("%w: unknown type", errBadRecord)
	}
	return nil
}

// validRecordFollows reports whether any valid record starts at an aligned
// offset in data[from:]. Used to tell a torn tail from mid-log corruption.
func validRecordFollows(data []byte, from int) bool {
	for p := alignUp(from); p+recordHeaderSize <= len(data); p += recordAlign {
		if _, _, err := DecodeRecord(data[p:]); err == nil {
			return true
		}
	}
	return false
}

// scanResult is the outcome of scanning one segment.
type scanResult struct {
	maxIndex uint64
	goodEnd  int  // offset just after the last good record
	torn     bool // a bad record ended the scan (only legal in the last segment)
}

// scanSegment feeds every record of data (after the header) to rp.
func scanSegment(name string, data []byte, last bool, rp *replayer) (scanResult, error) {
	res := scanResult{goodEnd: segmentHeaderSize}
	off := segmentHeaderSize
	for off < len(data) {
		rec, n, err := DecodeRecord(data[off:])
		if err == nil {
			// A CRC-valid record that breaks log invariants is not a torn
			// write: refuse to start, whichever segment it is in (D-33).
			if aerr := rp.apply(rec); aerr != nil {
				return res, corruptf(name, off, "%v", aerr)
			}
			if rec.Type == RecordEntry && rec.Index > res.maxIndex {
				res.maxIndex = rec.Index
			}
			off += n
			res.goodEnd = off
			continue
		}
		// Bad record at off. Only a bad tail of the last segment is recoverable.
		if !last {
			return res, corruptf(name, off, "%v in sealed segment", err)
		}
		if validRecordFollows(data, off+recordAlign) {
			return res, corruptf(name, off, "%v followed by valid records", err)
		}
		res.torn = true
		return res, nil
	}
	return res, nil
}

type segFile struct {
	info segInfo
}

// listSegments returns the segments in dir sorted by seq, checking that seq
// numbers have no gaps (a gap is fatal, §1.1).
func listSegments(fsys FS, dir string) ([]segFile, error) {
	names, err := fsys.List(dir)
	if err != nil {
		return nil, err
	}
	var segs []segFile
	for _, n := range names {
		seq, first, ok := parseSegmentName(n)
		if !ok {
			continue
		}
		segs = append(segs, segFile{info: segInfo{seq: seq, firstIndex: first, name: n}})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].info.seq < segs[j].info.seq })
	for i := 1; i < len(segs); i++ {
		if segs[i].info.seq != segs[i-1].info.seq+1 {
			return nil, fmt.Errorf("%w: segment seq gap between %d and %d", ErrCorrupt, segs[i-1].info.seq, segs[i].info.seq)
		}
	}
	return segs, nil
}

// replayDir replays all segments. It repairs a torn tail in the last segment
// and returns the surviving segments (a torn, header-less last segment is
// removed), the recovered state and the cut byte count.
func replayDir(fsys FS, dir string, opts Options) ([]segInfo, *Recovered, error) {
	segs, err := listSegments(fsys, dir)
	if err != nil {
		return nil, nil, err
	}
	rp := &replayer{}
	rec := &Recovered{}
	var infos []segInfo
	for i, sf := range segs {
		last := i == len(segs)-1
		path := filepath.Join(dir, sf.info.name)
		data, err := fsys.ReadFile(path)
		if err != nil {
			return nil, nil, err
		}
		if last && len(data) < segmentHeaderSize {
			// Crash during rotation before the header was complete: the
			// segment never held data. Drop it (D-33 tail rule).
			if err := fsys.Remove(path); err != nil {
				return nil, nil, err
			}
			if err := fsys.SyncDir(dir); err != nil {
				return nil, nil, err
			}
			rec.TruncatedBytes += int64(len(data))
			continue
		}
		h, err := decodeSegmentHeader(data)
		if errors.Is(err, ErrFormatVersion) {
			return nil, nil, fmt.Errorf("segment %s: %w", sf.info.name, err)
		}
		if err != nil {
			return nil, nil, corruptf(sf.info.name, 0, "%v", err)
		}
		if h.Seq != sf.info.seq || h.FirstIndex != sf.info.firstIndex {
			return nil, nil, corruptf(sf.info.name, 0, "header seq/first_index do not match file name")
		}
		if h.ClusterID != opts.ClusterID || h.MemberID != opts.MemberID {
			return nil, nil, corruptf(sf.info.name, 0, "header cluster/member id mismatch")
		}
		res, err := scanSegment(sf.info.name, data, last, rp)
		if err != nil {
			return nil, nil, err
		}
		if res.torn {
			if err := cutTail(fsys, path, int64(res.goodEnd)); err != nil {
				return nil, nil, err
			}
			rec.TruncatedBytes += int64(len(data) - res.goodEnd)
		}
		sf.info.maxIndex = res.maxIndex
		infos = append(infos, sf.info)
	}
	if rp.start.Index > 0 && len(rp.entries) > 0 && rp.entries[0].Index != rp.start.Index+1 {
		return nil, nil, fmt.Errorf("%w: log starts after %d but the first entry is %d", ErrCorrupt, rp.start.Index, rp.entries[0].Index)
	}
	rec.Entries = rp.entries
	rec.HardState = rp.hs
	rec.Start = rp.start
	return infos, rec, nil
}

// cutTail truncates a torn tail and fsyncs, so the cut survives a crash.
func cutTail(fsys FS, path string, size int64) error {
	f, err := fsys.OpenAppend(path)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
