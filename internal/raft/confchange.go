package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// Membership (D-06, D-31, system-design 7).
//
// A configuration is a [ConfState]: the voters, the learners and the IDs that
// were ever removed (the tombstones, D-31, D-38). It changes one server at a
// time through an [EntryConfChange] log entry. The entry carries the change
// and the whole configuration that results from it, so a node can always
// rebuild its configuration from its log alone (a new learner never saw the
// bootstrap configuration).
//
// Rules (system-design 7.1):
//
//  1. A configuration takes effect on a node as soon as the entry is appended
//     to its log, not when it commits. If the entry is later truncated, the
//     node goes back to the configuration before it (D-31).
//  2. One change at a time: a leader refuses a new change while the last one
//     in its log is uncommitted ([ErrConfChangePending]).
//  3. A leader refuses changes until it has committed an entry of its own
//     term ([ErrConfChangeTooEarly]). Without this rule a new leader could
//     append a change on top of a configuration that an earlier leader had
//     already left behind, and two configurations two servers apart could
//     each elect a leader (the single-server-change bug, Ongaro, raft-dev
//     2015).
//  4. Learners replicate but never vote, never campaign and never count in a
//     quorum.

// ConfChangeType is the kind of a single-server membership change.
type ConfChangeType uint8

const (
	// ConfChangeAddLearner adds a new, non-voting member (D-06).
	ConfChangeAddLearner ConfChangeType = iota + 1
	// ConfChangePromoteLearner turns a learner into a voter.
	ConfChangePromoteLearner
	// ConfChangeRemoveNode removes a voter or a learner and remembers its ID
	// forever (D-31, D-38).
	ConfChangeRemoveNode
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfChangeAddLearner:
		return "AddLearner"
	case ConfChangePromoteLearner:
		return "PromoteLearner"
	case ConfChangeRemoveNode:
		return "RemoveNode"
	default:
		return fmt.Sprintf("ConfChangeType(%d)", uint8(t))
	}
}

// ConfChange is one membership change. Context is opaque to the core; the
// driver uses it to carry member metadata (names and addresses) so every
// replica learns them from the log.
type ConfChange struct {
	Type    ConfChangeType
	NodeID  uint64
	Context []byte
}

// ConfState is a cluster configuration. All three lists are sorted and
// disjoint, except that Removed is disjoint from the other two.
type ConfState struct {
	Voters   []uint64
	Learners []uint64
	// Removed are the IDs removed so far. Messages from them are ignored
	// (D-31) and they can never be added again (D-38).
	Removed []uint64
}

// Clone returns a deep copy.
func (cs ConfState) Clone() ConfState {
	return ConfState{Voters: slices.Clone(cs.Voters), Learners: slices.Clone(cs.Learners), Removed: slices.Clone(cs.Removed)}
}

// Equal reports whether cs and o hold the same members.
func (cs ConfState) Equal(o ConfState) bool {
	return slices.Equal(cs.Voters, o.Voters) && slices.Equal(cs.Learners, o.Learners) && slices.Equal(cs.Removed, o.Removed)
}

// IsVoter reports whether id is a voter.
func (cs ConfState) IsVoter(id uint64) bool { _, ok := slices.BinarySearch(cs.Voters, id); return ok }

// IsLearner reports whether id is a learner.
func (cs ConfState) IsLearner(id uint64) bool {
	_, ok := slices.BinarySearch(cs.Learners, id)
	return ok
}

// IsRemoved reports whether id was removed.
func (cs ConfState) IsRemoved(id uint64) bool {
	_, ok := slices.BinarySearch(cs.Removed, id)
	return ok
}

// IsMember reports whether id is a voter or a learner.
func (cs ConfState) IsMember(id uint64) bool { return cs.IsVoter(id) || cs.IsLearner(id) }

// Members returns voters and learners, sorted.
func (cs ConfState) Members() []uint64 {
	return slices.Sorted(slices.Values(append(slices.Clone(cs.Voters), cs.Learners...)))
}

func (cs ConfState) String() string {
	return fmt.Sprintf("voters=%v learners=%v removed=%v", cs.Voters, cs.Learners, cs.Removed)
}

// ErrInvalidConfChange is matched (errors.Is) by every error that explains why
// a change cannot be applied to a configuration.
var ErrInvalidConfChange = errors.New("raft: invalid configuration change")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfChange, fmt.Sprintf(format, args...))
}

func insertSorted(s []uint64, id uint64) []uint64 {
	i, found := slices.BinarySearch(s, id)
	if found {
		return s
	}
	return slices.Insert(s, i, id)
}

func deleteSorted(s []uint64, id uint64) []uint64 {
	if i, found := slices.BinarySearch(s, id); found {
		return slices.Delete(s, i, i+1)
	}
	return s
}

// Apply returns the configuration that results from applying cc to cs, or an
// error wrapping [ErrInvalidConfChange]. cs is not modified.
func (cs ConfState) Apply(cc ConfChange) (ConfState, error) {
	id := cc.NodeID
	if id == None {
		return ConfState{}, invalid("node ID 0")
	}
	out := cs.Clone()
	switch cc.Type {
	case ConfChangeAddLearner:
		switch {
		case cs.IsRemoved(id):
			return ConfState{}, invalid("node %d was removed and its ID is never reused (D-38)", id)
		case cs.IsMember(id):
			return ConfState{}, invalid("node %d is already a member", id)
		}
		out.Learners = insertSorted(out.Learners, id)
	case ConfChangePromoteLearner:
		if !cs.IsLearner(id) {
			return ConfState{}, invalid("node %d is not a learner", id)
		}
		out.Learners = deleteSorted(out.Learners, id)
		out.Voters = insertSorted(out.Voters, id)
	case ConfChangeRemoveNode:
		switch {
		case !cs.IsMember(id):
			return ConfState{}, invalid("node %d is not a member", id)
		case cs.IsVoter(id) && len(cs.Voters) == 1:
			return ConfState{}, invalid("node %d is the last voter", id)
		}
		out.Voters = deleteSorted(out.Voters, id)
		out.Learners = deleteSorted(out.Learners, id)
		out.Removed = insertSorted(out.Removed, id)
	default:
		return ConfState{}, invalid("unknown change type %d", cc.Type)
	}
	return out, nil
}

// confChangeVersion is the encoding version written in the first byte of every
// conf-change entry (WBS 6.1.1). Decoding refuses unknown versions instead of
// guessing.
const confChangeVersion = 1

// EncodeConfChange encodes a conf-change entry's payload: the change and the
// configuration it produces. Version 1 layout:
//
//	byte    version (1)
//	byte    change type
//	uvarint node ID
//	3 x     uvarint count, then count uvarint IDs (voters, learners, removed)
//	uvarint context length, then the context bytes
func EncodeConfChange(cc ConfChange, result ConfState) []byte {
	b := []byte{confChangeVersion, byte(cc.Type)}
	b = binary.AppendUvarint(b, cc.NodeID)
	for _, ids := range [][]uint64{result.Voters, result.Learners, result.Removed} {
		b = binary.AppendUvarint(b, uint64(len(ids)))
		for _, id := range ids {
			b = binary.AppendUvarint(b, id)
		}
	}
	b = binary.AppendUvarint(b, uint64(len(cc.Context)))
	return append(b, cc.Context...)
}

// ErrBadConfChange means a conf-change payload cannot be decoded.
var ErrBadConfChange = errors.New("raft: malformed conf-change entry")

// DecodeConfChange decodes a payload written by [EncodeConfChange].
func DecodeConfChange(data []byte) (ConfChange, ConfState, error) {
	if len(data) < 2 {
		return ConfChange{}, ConfState{}, fmt.Errorf("%w: %d bytes", ErrBadConfChange, len(data))
	}
	if data[0] != confChangeVersion {
		return ConfChange{}, ConfState{}, fmt.Errorf("%w: unknown version %d", ErrBadConfChange, data[0])
	}
	cc := ConfChange{Type: ConfChangeType(data[1])}
	p := data[2:]
	next := func() (uint64, bool) {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return 0, false
		}
		p = p[n:]
		return v, true
	}
	var ok bool
	if cc.NodeID, ok = next(); !ok {
		return ConfChange{}, ConfState{}, fmt.Errorf("%w: node ID", ErrBadConfChange)
	}
	var lists [3][]uint64
	for i := range lists {
		n, ok := next()
		if !ok || n > uint64(len(p)) {
			return ConfChange{}, ConfState{}, fmt.Errorf("%w: list %d", ErrBadConfChange, i)
		}
		for range n {
			id, ok := next()
			if !ok {
				return ConfChange{}, ConfState{}, fmt.Errorf("%w: list %d", ErrBadConfChange, i)
			}
			lists[i] = append(lists[i], id)
		}
	}
	n, ok := next()
	if !ok || n != uint64(len(p)) {
		return ConfChange{}, ConfState{}, fmt.Errorf("%w: context", ErrBadConfChange)
	}
	if n > 0 {
		cc.Context = slices.Clone(p)
	}
	return cc, ConfState{Voters: lists[0], Learners: lists[1], Removed: lists[2]}, nil
}

// confAt is a configuration that took effect at a log index.
type confAt struct {
	index uint64
	cs    ConfState
}

// QuorumsOverlap reports whether every majority of voter set a intersects
// every majority of voter set b. Two single-server-apart configurations always
// overlap; the simulator checks this for every pair of configurations that
// may be in use at once ("no two disjoint majorities").
func QuorumsOverlap(a, b []uint64) bool {
	inB := make(map[uint64]bool, len(b))
	for _, id := range b {
		inB[id] = true
	}
	shared := 0
	for _, id := range a {
		if inB[id] {
			shared++
		}
	}
	onlyA, onlyB := len(a)-shared, len(b)-shared
	// The most disjoint pair of majorities takes members outside the other
	// set first; they are disjoint iff what is left to take from the shared
	// members fits.
	needA := max(0, quorumSize(len(a))-onlyA)
	needB := max(0, quorumSize(len(b))-onlyB)
	return needA+needB > shared
}
