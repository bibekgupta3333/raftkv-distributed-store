package raft

import (
	"errors"
	"fmt"
)

// None is the zero node ID. It means "no node": no leader, no vote.
const None uint64 = 0

// EntryType distinguishes ordinary commands from cluster-configuration entries.
type EntryType uint8

const (
	// EntryNormal carries an opaque state-machine command (or nothing, for the
	// leader's no-op, see system-design T6).
	EntryNormal EntryType = iota
	// EntryConfChange is reserved for membership changes (M6, D-06, D-31). The
	// core stores and replicates it like any entry but attaches no meaning yet.
	EntryConfChange
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "EntryNormal"
	case EntryConfChange:
		return "EntryConfChange"
	default:
		return fmt.Sprintf("EntryType(%d)", uint8(t))
	}
}

// Entry is one log entry. Index starts at 1; Term 0 is never used by a real
// entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// Size is the payload size used for message-size and uncommitted-size limits
// (D-29). Empty entries, like the leader's no-op, have size 0 and are never
// rejected by the limits.
func (e Entry) Size() uint64 { return uint64(len(e.Data)) }

// HardState is the state that must survive a restart (system-design section 0).
// Term and Vote must be durable before any message that depends on them is
// sent. Commit is persisted lazily; it is not needed for safety.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// IsEmpty reports whether hs is the zero HardState.
func (hs HardState) IsEmpty() bool { return hs == HardState{} }

// StateType is the role of a node.
type StateType uint8

const (
	// StateFollower is the default role.
	StateFollower StateType = iota
	// StateCandidate is a node running a real election at term+1.
	StateCandidate
	// StatePreCandidate is a node running a PreVote round; its term is not
	// bumped until the round succeeds (D-08).
	StatePreCandidate
	// StateLeader is the leader of the current term.
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StatePreCandidate:
		return "PreCandidate"
	case StateLeader:
		return "Leader"
	default:
		return fmt.Sprintf("StateType(%d)", uint8(s))
	}
}

// SoftState is volatile, non-persisted state that drivers may expose.
type SoftState struct {
	Lead  uint64
	State StateType
}

// MessageType is the kind of a peer message.
//
// Types for leadership transfer (M6) will be appended after the existing ones.
type MessageType uint8

const (
	// MsgApp is AppendEntries. Index/LogTerm are prevIndex/prevTerm.
	MsgApp MessageType = iota + 1
	// MsgAppResp answers MsgApp. On success Index is the follower's match
	// index. On Reject, Index is the rejected prevIndex, RejectHint is the
	// follower's conflict hint index and LogTerm the term at that index.
	MsgAppResp
	// MsgHeartbeat is the leader's per-tick liveness message. It carries
	// Commit (clamped to what the follower is known to have) and Context.
	MsgHeartbeat
	// MsgHeartbeatResp answers MsgHeartbeat and echoes Context.
	MsgHeartbeatResp
	// MsgVote is RequestVote at the candidate's new term. Index/LogTerm are
	// the candidate's last log index and term.
	MsgVote
	// MsgVoteResp answers MsgVote; Reject is true when the vote is denied.
	MsgVoteResp
	// MsgPreVote is a PreVote request (D-08) at term+1 without a term bump.
	MsgPreVote
	// MsgPreVoteResp answers MsgPreVote.
	MsgPreVoteResp
	// MsgReadIndex is a follower's ReadIndex request to the leader (D-05,
	// D-36). Context is the caller's request context. It is term-less (Term
	// 0) and never changes the receiver's term or role.
	MsgReadIndex
	// MsgReadIndexResp answers MsgReadIndex: Index is the read index and
	// Context echoes the request. Reject means the receiver was not the
	// leader (or lost leadership before confirming). It never changes the
	// receiver's term or role.
	MsgReadIndexResp
	// MsgSnap is InstallSnapshot (D-14): the leader tells a follower whose
	// next index was compacted away to replace its state with Snapshot. The
	// core carries only the metadata; the driver streams the snapshot files
	// out of band and reports the outcome with [RawNode.ReportSnapshot]. The
	// follower answers with a MsgAppResp whose Index is its new match index.
	MsgSnap
)

func (t MessageType) String() string {
	names := [...]string{
		MsgApp: "MsgApp", MsgAppResp: "MsgAppResp",
		MsgHeartbeat: "MsgHeartbeat", MsgHeartbeatResp: "MsgHeartbeatResp",
		MsgVote: "MsgVote", MsgVoteResp: "MsgVoteResp",
		MsgPreVote: "MsgPreVote", MsgPreVoteResp: "MsgPreVoteResp",
		MsgReadIndex: "MsgReadIndex", MsgReadIndexResp: "MsgReadIndexResp",
		MsgSnap: "MsgSnap",
	}
	if int(t) < len(names) && names[t] != "" {
		return names[t]
	}
	return fmt.Sprintf("MessageType(%d)", uint8(t))
}

func (t MessageType) valid() bool { return t >= MsgApp && t <= MsgSnap }

// Message is a peer-to-peer message. It is a plain Go value; the transport
// (M2) is responsible for encoding it. Fields not used by a given Type are
// zero. Messages are immutable once produced by the core: Entries must be
// treated as read-only.
type Message struct {
	Type MessageType
	To   uint64
	From uint64
	// Term is the sender's term (for PreVote and its granted response, the
	// prospective term, see D-08).
	Term uint64

	// LogTerm and Index have type-specific meaning, see the MessageType docs.
	LogTerm uint64
	Index   uint64
	Entries []Entry
	// Commit is the leader's commit index (MsgApp, MsgHeartbeat).
	Commit uint64

	// Reject marks a denied vote or a rejected append.
	Reject bool
	// RejectHint is the conflict hint for a rejected MsgApp (fast
	// backtracking, system-design 2.1).
	RejectHint uint64

	// Context is opaque. On MsgHeartbeat it is the ReadIndex round being
	// confirmed and is echoed by MsgHeartbeatResp; on MsgReadIndex and
	// MsgReadIndexResp it is the caller's request context (D-05).
	Context []byte

	// Snapshot is the snapshot a MsgSnap asks the receiver to install
	// (D-14). Nil on every other type.
	Snapshot *Snapshot
}

// Snapshot describes the state machine as of log index Index, whose entry has
// term Term (D-14). The core handles only this metadata; the state itself (a
// Pebble checkpoint in the real node) lives outside the core, which stays free
// of I/O (D-01). Membership joins the metadata with M6.
type Snapshot struct {
	Index uint64
	Term  uint64
}

// IsEmpty reports whether s is the zero Snapshot (no snapshot).
func (s Snapshot) IsEmpty() bool { return s.Index == 0 }

// Ready is everything the driver must handle after the last Advance. Slices
// are owned by the core's view of the log and must not be modified.
//
// Handling order (D-01, architecture 4.2): persist Entries and HardState (with
// an fsync when MustSync), then send Messages, then apply CommittedEntries,
// then call [RawNode.Advance].
type Ready struct {
	// SoftState is non-nil only when the leader or role changed.
	SoftState *SoftState
	// HardState is the zero value when unchanged since the last Ready.
	HardState HardState
	// Entries are unpersisted log entries to append to stable storage. If
	// the first entry's index is at or below the stored last index, storage
	// must truncate the conflicting suffix first.
	Entries []Entry
	// CommittedEntries are committed entries ready to apply, in index order.
	CommittedEntries []Entry
	// Messages must be sent only after Entries and HardState are durable.
	Messages []Message
	// ReadStates answer ReadIndex requests made on this node (D-05). Serve
	// a successful one once the applied index reaches its Index; it needs
	// nothing persisted first.
	ReadStates []ReadState
	// MustSync is true when Term/Vote changed, Entries is non-empty or a
	// Snapshot must be installed.
	MustSync bool
	// Snapshot, when non-nil, is a snapshot received from the leader that
	// the driver must install before persisting Entries (D-14,
	// storage-design 4.4): replace the state machine with it, reset the
	// stable log so it starts after Snapshot.Index, and make both durable.
	// CommittedEntries never include entries at or below it.
	Snapshot *Snapshot
}

// Errors returned by the core.
var (
	// ErrProposalDropped means the proposal was not accepted: there is no
	// known leader, this node is a candidate, or a size limit (D-29) is hit.
	// It is safe to retry later.
	ErrProposalDropped = errors.New("raft: proposal dropped")
	// ErrReadIndexDropped means a ReadIndex request was not accepted
	// because this node knows no leader. It is safe to retry later.
	ErrReadIndexDropped = errors.New("raft: read index dropped, no known leader")
	// ErrNotLeader is matched (errors.Is) by *NotLeaderError.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrUnknownMessage is returned by Step for an invalid message type.
	ErrUnknownMessage = errors.New("raft: unknown message type")
	// ErrCompacted means the requested index is below the first stored entry.
	ErrCompacted = errors.New("raft: requested index is unavailable due to compaction")
	// ErrUnavailable means the requested index is beyond the stored entries.
	ErrUnavailable = errors.New("raft: requested entry at index is unavailable")
	// ErrSnapOutOfDate means a snapshot is not newer than the one the
	// storage already holds.
	ErrSnapOutOfDate = errors.New("raft: requested snapshot is older than the existing snapshot")
	// ErrSnapshotTemporarilyUnavailable means the storage has no snapshot
	// to send right now; the leader tries again later.
	ErrSnapshotTemporarilyUnavailable = errors.New("raft: snapshot is temporarily unavailable")
)

// NotLeaderError is returned by Propose on a follower that knows the leader.
// Forwarding is added in M4 (D-28); until then callers redirect using Leader.
type NotLeaderError struct {
	// Leader is the node believed to lead the current term (never None).
	Leader uint64
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("raft: not leader, leader is %d", e.Leader)
}

// Is makes errors.Is(err, ErrNotLeader) true.
func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// Rand is the injectable randomness source (D-24). *rand.Rand from
// math/rand/v2 satisfies it. The core calls it only to draw election timeouts.
type Rand interface {
	// IntN returns a value in [0, n).
	IntN(n int) int
}
