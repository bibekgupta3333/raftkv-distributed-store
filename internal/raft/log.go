package raft

import "fmt"

const noLimit = ^uint64(0)

// unstable holds what the core has accepted but the driver has not yet
// confirmed as persisted (via Advance): an incoming snapshot to install and
// entries. entries[i] has index offset+i; with a snapshot, offset is
// snapshot.Index+1.
type unstable struct {
	snapshot *Snapshot
	offset   uint64
	entries  []Entry
}

// maybeFirstIndex is the first log index if a snapshot is pending.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Index + 1, true
	}
	return 0, false
}

func (u *unstable) maybeLastIndex() (uint64, bool) {
	if n := len(u.entries); n > 0 {
		return u.offset + uint64(n) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Index, true
	}
	return 0, false
}

func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Index == i {
			return u.snapshot.Term, true
		}
		return 0, false
	}
	last, ok := u.maybeLastIndex()
	if !ok || i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

// stableTo drops entries up to index i once the driver persisted them. The
// term check ignores a stale confirmation for entries since replaced.
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok || gt != t || i < u.offset {
		return
	}
	u.entries = append([]Entry(nil), u.entries[i+1-u.offset:]...)
	u.offset = i + 1
}

// stableSnapTo drops the pending snapshot once the driver installed it.
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Index == i {
		u.snapshot = nil
	}
}

// replaceWith replaces the unstable log with snapshot s (D-14).
func (u *unstable) replaceWith(s Snapshot) {
	u.offset = s.Index + 1
	u.entries = nil
	u.snapshot = &s
}

// truncateAndAppend appends after, replacing any conflicting unstable suffix.
func (u *unstable) truncateAndAppend(after []Entry) {
	first := after[0].Index
	switch {
	case first == u.offset+uint64(len(u.entries)):
		u.entries = append(u.entries, after...)
	case first <= u.offset:
		u.offset = first
		u.entries = append([]Entry(nil), after...)
	default:
		keep := u.entries[:first-u.offset]
		u.entries = append(append([]Entry(nil), keep...), after...)
	}
}

// slice returns unstable entries in [lo, hi); capacity is clipped so later
// appends can never overwrite a slice handed out in a Ready or Message.
func (u *unstable) slice(lo, hi uint64) []Entry {
	return u.entries[lo-u.offset : hi-u.offset : hi-u.offset]
}

// raftLog is the log as the core sees it: a stable prefix in Storage plus an
// unstable suffix, with committed and applied cursors.
//
// Invariants (checked by checkInvariants in tests):
//
//	applied <= committed <= lastIndex
//	firstIndex-1 <= applied
//	unstable.offset <= (stable last index)+1, with equality when unstable is empty
type raftLog struct {
	storage   Storage
	unstable  unstable
	committed uint64
	applied   uint64
}

func newLog(storage Storage) (*raftLog, error) {
	first, err := storage.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}
	return &raftLog{
		storage:   storage,
		unstable:  unstable{offset: last + 1},
		committed: first - 1,
		applied:   first - 1,
	}, nil
}

func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// snapshot is the newest snapshot the leader can send: a pending incoming one,
// else the storage's (D-14).
func (l *raftLog) snapshot() (Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *raftLog) hasPendingSnapshot() bool { return l.unstable.snapshot != nil }

// installSnapshot replaces the log with snapshot s: everything up to s.Index
// is now committed and covered by the snapshot (D-14). The caller has checked
// that s is newer than the commit index and does not match the log.
func (l *raftLog) installSnapshot(s Snapshot) {
	l.committed = s.Index
	l.unstable.replaceWith(s)
}

// stableIndex is the last index known persisted.
func (l *raftLog) stableIndex() uint64 { return l.unstable.offset - 1 }

// term returns the term of index i, or 0 when i is outside the log.
func (l *raftLog) term(i uint64) (uint64, error) {
	if i+1 < l.firstIndex() { // i < firstIndex-1
		return 0, ErrCompacted
	}
	if i > l.lastIndex() {
		return 0, ErrUnavailable
	}
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}
	return l.storage.Term(i)
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic(fmt.Sprintf("raft: no term for last index: %v", err))
	}
	return t
}

// matchTerm reports whether the log has an entry at i with term t.
func (l *raftLog) matchTerm(i, t uint64) bool {
	got, err := l.term(i)
	return err == nil && got == t
}

// slice returns entries in [lo, hi) limited to maxSize payload bytes (at least
// one entry).
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]Entry, error) {
	if lo >= hi {
		return nil, nil
	}
	if lo < l.firstIndex() {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	var out []Entry
	if lo < l.unstable.offset {
		stored, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err != nil {
			return nil, err
		}
		out = stored
		if uint64(len(stored)) < min(hi, l.unstable.offset)-lo {
			return out, nil // size-limited inside storage
		}
	}
	if hi > l.unstable.offset {
		un := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(out) > 0 {
			out = append(out[:len(out):len(out)], un...)
		} else {
			out = un
		}
	}
	return limitSize(out, maxSize), nil
}

// entries returns up to maxSize bytes of entries starting at i.
func (l *raftLog) entries(i, maxSize uint64) ([]Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxSize)
}

func (l *raftLog) allEntries() []Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err != nil {
		panic(err)
	}
	return ents
}

// unstableEntries are the entries the driver still has to persist.
func (l *raftLog) unstableEntries() []Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries[:len(l.unstable.entries):len(l.unstable.entries)]
}

// append adds leader/follower-chosen entries to the unstable log. It never
// truncates committed entries (that would be a safety bug, system-design 2.2).
func (l *raftLog) append(ents ...Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		panic(fmt.Sprintf("raft: appending at %d would truncate committed index %d", after+1, l.committed))
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// findConflict returns the index of the first entry in ents that is not in
// the log (missing or with a different term), or 0 if all are present.
// Pure with respect to the log.
func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// findConflictByTerm finds the largest index <= index whose term <= term.
// It is the core of fast backtracking (WBS 1.3.3): it skips a whole run of
// entries from a conflicting term in one step. It returns (index, term) of
// that entry; term is 0 if nothing in the log qualifies.
func (l *raftLog) findConflictByTerm(index, term uint64) (uint64, uint64) {
	if last := l.lastIndex(); index > last {
		return index, 0 // caller bug guard: nothing to say about unknown suffix
	}
	for ; ; index-- {
		t, err := l.term(index)
		if err != nil {
			return index, 0
		}
		if t <= term {
			return index, t
		}
		if index == 0 {
			return 0, 0
		}
	}
}

// maybeAppend implements the follower side of AppendEntries (system-design
// 2.2): if the log matches at (index, logTerm), append ents (truncating
// conflicts) and advance commit to min(committed, last new index).
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...Entry) (lastNew uint64, ok bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}
	lastNew = index + uint64(len(ents))
	if ci := l.findConflict(ents); ci != 0 {
		if ci <= l.committed {
			panic(fmt.Sprintf("raft: entry %d conflicts with committed entry %d", ci, l.committed))
		}
		l.append(ents[ci-(index+1):]...)
	}
	l.commitTo(min(committed, lastNew))
	return lastNew, true
}

func (l *raftLog) commitTo(c uint64) {
	if c <= l.committed {
		return
	}
	if c > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit %d is beyond last index %d", c, l.lastIndex()))
	}
	l.committed = c
}

// maybeCommit raises committed to idx if the entry there has the leader's
// current term: the Figure 8 rule (system-design 2.4). It returns whether
// committed changed.
func (l *raftLog) maybeCommit(idx, term uint64) bool {
	if idx <= l.committed {
		return false
	}
	if t, err := l.term(idx); err != nil || t != term {
		return false
	}
	l.commitTo(idx)
	return true
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if i < l.applied || i > l.committed {
		panic(fmt.Sprintf("raft: applied %d out of range (applied %d, committed %d)", i, l.applied, l.committed))
	}
	l.applied = i
}

// stableSnapTo records that the snapshot at index i is installed, which also
// applies it: the state machine now reflects everything up to i.
func (l *raftLog) stableSnapTo(i uint64) {
	if l.unstable.snapshot == nil || l.unstable.snapshot.Index != i {
		return
	}
	l.unstable.stableSnapTo(i)
	if i > l.applied {
		l.appliedTo(i)
	}
}

// stableTo records that entries up to (i, t) are persisted.
func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

// nextCommittedEnts returns committed-but-unapplied entries. Nothing is
// applied while a snapshot is pending: it must be installed first.
func (l *raftLog) nextCommittedEnts() []Entry {
	if l.hasPendingSnapshot() {
		return nil
	}
	lo := max(l.applied+1, l.firstIndex())
	if l.committed+1 <= lo {
		return nil
	}
	ents, err := l.slice(lo, l.committed+1, noLimit)
	if err != nil {
		panic(err)
	}
	return ents
}

func (l *raftLog) hasNextCommittedEnts() bool {
	if l.hasPendingSnapshot() {
		return false
	}
	return l.committed+1 > max(l.applied+1, l.firstIndex())
}

// isUpToDate implements the Raft voting rule: a candidate's log is at least as
// up to date as ours (higher last term wins, then longer log). Pure in effect.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	return isLogUpToDate(lastIndex, lastTerm, l.lastIndex(), l.lastTerm())
}

// isLogUpToDate is the pure form of the vote log check (T3).
func isLogUpToDate(candIndex, candTerm, ourIndex, ourTerm uint64) bool {
	return candTerm > ourTerm || (candTerm == ourTerm && candIndex >= ourIndex)
}

// checkInvariants returns a description of the first violated invariant.
func (l *raftLog) checkInvariants() error {
	switch {
	case l.applied > l.committed:
		return fmt.Errorf("applied %d > committed %d", l.applied, l.committed)
	case l.committed > l.lastIndex():
		return fmt.Errorf("committed %d > last %d", l.committed, l.lastIndex())
	case l.applied+1 < l.firstIndex() && !l.hasPendingSnapshot():
		return fmt.Errorf("applied %d below first index %d", l.applied, l.firstIndex())
	}
	if l.hasPendingSnapshot() {
		return nil // the stable log is replaced when the driver installs it
	}
	last, err := l.storage.LastIndex()
	if err != nil {
		return err
	}
	// After a conflict truncation the stored suffix is stale until the driver
	// persists the replacement, so the offset may sit below last+1.
	if len(l.unstable.entries) == 0 && l.unstable.offset != last+1 {
		return fmt.Errorf("empty unstable: offset %d != stable last %d + 1", l.unstable.offset, last)
	}
	if l.unstable.offset > last+1 {
		return fmt.Errorf("unstable offset %d beyond stable last %d + 1", l.unstable.offset, last)
	}
	return nil
}
