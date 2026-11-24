package raft

import (
	"errors"
	"fmt"
)

// Default limits (D-08, D-29).
const (
	// DefaultElectionTick is the base election timeout in ticks. The actual
	// timeout is random in [ElectionTick, 2*ElectionTick) (D-08: 10-20 ticks
	// of 100 ms).
	DefaultElectionTick = 10
	// DefaultHeartbeatTick sends a heartbeat every tick (D-08).
	DefaultHeartbeatTick = 1
	// DefaultMaxInflight caps unacknowledged appends per follower (D-29).
	DefaultMaxInflight = 256
	// DefaultMaxUncommittedBytes caps uncommitted log bytes on a leader (D-29).
	DefaultMaxUncommittedBytes = 64 << 20
	// DefaultMaxMsgBytes caps the entry payload in one MsgApp.
	DefaultMaxMsgBytes = 1 << 20
)

// Config configures a RawNode. Build one with [DefaultConfig] and override
// fields, then call [Config.Validate] ([NewRawNode] does so).
type Config struct {
	// ID is this node's ID. It must be non-zero.
	ID uint64
	// Peers lists the bootstrap voters: the configuration before the first
	// conf-change entry, used only when Storage holds no snapshot carrying a
	// configuration (D-06). Order is irrelevant. A node joining an existing
	// cluster (D-32) passes none: it is not a voter, never campaigns, and
	// learns the configuration from the leader's log or snapshot.
	Peers []uint64

	// ElectionTick is the base election timeout in ticks; the timeout is
	// redrawn from Rand in [ElectionTick, 2*ElectionTick) (D-08, D-24). It is
	// also the CheckQuorum window and the "heard from a leader" lease.
	ElectionTick int
	// HeartbeatTick is the number of ticks between leader heartbeats. It must
	// be smaller than ElectionTick.
	HeartbeatTick int

	// Storage holds the stable log and HardState. Required.
	Storage Storage
	// Rand supplies election-timeout randomness. Required; seed it for
	// deterministic runs.
	Rand Rand

	// PreVote enables the PreVote phase (D-08). Production uses true.
	PreVote bool
	// CheckQuorum makes leaders step down without a quorum of recent
	// responses and makes followers ignore votes while they have a live
	// leader (D-08). Production uses true.
	CheckQuorum bool

	// MaxUncommittedBytes bounds the payload bytes a leader holds uncommitted;
	// further proposals get ErrProposalDropped (D-29). 0 means unlimited.
	MaxUncommittedBytes uint64
	// MaxMsgBytes bounds the entry payload of one MsgApp; at least one entry
	// is always sent. 0 means one entry per message.
	MaxMsgBytes uint64
	// MaxInflight bounds in-flight MsgApp per follower in Replicate state
	// (D-29). Must be positive.
	MaxInflight int

	// Applied is the index the state machine has already applied at start
	// (for restart). 0 means nothing applied.
	Applied uint64

	// Unsafe switches off membership safety rules. It exists only so the
	// simulator can plant the bugs those rules prevent (WBS 6.1.3, 6.3.1);
	// production leaves it zero.
	Unsafe UnsafeOptions
}

// UnsafeOptions break Raft on purpose, for planted-bug tests only.
type UnsafeOptions struct {
	// AllowConcurrentConfChanges lets a leader append a change while
	// another is still uncommitted.
	AllowConcurrentConfChanges bool
	// AllowEarlyConfChange lets a new leader change the configuration
	// before it has committed an entry of its own term.
	AllowEarlyConfChange bool
}

// DefaultConfig returns a Config with the D-08 / D-29 defaults (PreVote and
// CheckQuorum on). The caller supplies the identity, membership, storage and
// randomness.
func DefaultConfig(id uint64, peers []uint64, storage Storage, rng Rand) Config {
	return Config{
		ID:                  id,
		Peers:               peers,
		ElectionTick:        DefaultElectionTick,
		HeartbeatTick:       DefaultHeartbeatTick,
		Storage:             storage,
		Rand:                rng,
		PreVote:             true,
		CheckQuorum:         true,
		MaxUncommittedBytes: DefaultMaxUncommittedBytes,
		MaxMsgBytes:         DefaultMaxMsgBytes,
		MaxInflight:         DefaultMaxInflight,
	}
}

// Validate reports the first problem with c, or nil.
func (c Config) Validate() error {
	if c.ID == None {
		return errors.New("raft: config: ID must be non-zero")
	}
	seen := make(map[uint64]bool, len(c.Peers))
	for _, p := range c.Peers {
		if p == None {
			return errors.New("raft: config: Peers contains the zero ID")
		}
		if seen[p] {
			return fmt.Errorf("raft: config: duplicate peer %d", p)
		}
		seen[p] = true
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("raft: config: HeartbeatTick must be positive")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("raft: config: ElectionTick must be greater than HeartbeatTick")
	}
	if c.Storage == nil {
		return errors.New("raft: config: Storage is required")
	}
	if c.Rand == nil {
		return errors.New("raft: config: Rand is required (D-24: no hidden randomness)")
	}
	if c.MaxInflight <= 0 {
		return errors.New("raft: config: MaxInflight must be positive")
	}
	return nil
}
