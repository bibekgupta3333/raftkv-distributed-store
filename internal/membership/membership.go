// Package membership translates between the Raft core's configuration
// ([raft.ConfState]: IDs only) and the cluster's member table
// ([raftkvv1.ClusterConfig]: IDs, names, addresses and roles), D-06, D-23.
//
// A conf-change log entry carries both: the core's change and resulting
// ConfState, and as its opaque context the member table after the change, so
// every replica (a new learner included) learns every member's address from
// the log or a snapshot. The core's ConfState is authoritative for roles;
// the table adds the addresses.
package membership

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// BootstrapIDs is the member table a bootstrapped state machine records: every
// static peer is a voter. It holds no addresses, because each node may see its
// peers at different addresses and the state machine must be identical on
// every replica (D-04, D-18); nodes fill addresses in from their own peer list
// ([FillAddrs]).
func BootstrapIDs(ids []uint64) *raftkvv1.ClusterConfig {
	cfg := &raftkvv1.ClusterConfig{}
	for _, id := range ids {
		cfg.Members = append(cfg.Members, &raftkvv1.Member{Id: id, Role: raftkvv1.MemberRole_VOTER})
	}
	sortMembers(cfg)
	return cfg
}

// FillAddrs returns a copy of cfg in which every member listed in addrs (this
// node's own view of its configured peers) takes that peer address: a node
// reaches its configured peers the way it was told to.
func FillAddrs(cfg *raftkvv1.ClusterConfig, addrs map[uint64]string) *raftkvv1.ClusterConfig {
	out := proto.CloneOf(cfg)
	for _, m := range out.GetMembers() {
		if a, ok := addrs[m.GetId()]; ok {
			if m.GetClientUrl() == "" || m.GetClientUrl() == m.GetPeerUrl() {
				m.ClientUrl = a
			}
			m.PeerUrl = a
		}
	}
	return out
}

// Bootstrap returns the member table of a cluster created from static peers
// (node ID -> peer address): every peer is a voter.
func Bootstrap(peers map[uint64]string) *raftkvv1.ClusterConfig {
	cfg := &raftkvv1.ClusterConfig{}
	for id, addr := range peers {
		cfg.Members = append(cfg.Members, &raftkvv1.Member{Id: id, PeerUrl: addr, ClientUrl: addr, Role: raftkvv1.MemberRole_VOTER})
	}
	sortMembers(cfg)
	return cfg
}

func sortMembers(cfg *raftkvv1.ClusterConfig) {
	slices.SortFunc(cfg.Members, func(a, b *raftkvv1.Member) int {
		switch {
		case a.GetId() < b.GetId():
			return -1
		case a.GetId() > b.GetId():
			return 1
		}
		return 0
	})
	slices.Sort(cfg.Removed)
}

// ConfState returns the core's view of a member table.
func ConfState(cfg *raftkvv1.ClusterConfig) raft.ConfState {
	var cs raft.ConfState
	for _, m := range cfg.GetMembers() {
		if m.GetRole() == raftkvv1.MemberRole_LEARNER {
			cs.Learners = append(cs.Learners, m.GetId())
		} else {
			cs.Voters = append(cs.Voters, m.GetId())
		}
	}
	slices.Sort(cs.Voters)
	slices.Sort(cs.Learners)
	cs.Removed = slices.Sorted(slices.Values(cfg.GetRemoved()))
	return cs
}

// Find returns the member with id, or nil.
func Find(cfg *raftkvv1.ClusterConfig, id uint64) *raftkvv1.Member {
	for _, m := range cfg.GetMembers() {
		if m.GetId() == id {
			return m
		}
	}
	return nil
}

// FindPeerURL returns the member whose peer URL is url, or nil.
func FindPeerURL(cfg *raftkvv1.ClusterConfig, url string) *raftkvv1.Member {
	for _, m := range cfg.GetMembers() {
		if m.GetPeerUrl() == url {
			return m
		}
	}
	return nil
}

// Next returns the member table after a change: roles and membership follow
// result (the core's ConfState after the change), addresses come from cur, and
// added is the new member's description for an AddLearner (ignored otherwise).
func Next(cur *raftkvv1.ClusterConfig, result raft.ConfState, added *raftkvv1.Member) *raftkvv1.ClusterConfig {
	out := &raftkvv1.ClusterConfig{Removed: slices.Clone(result.Removed)}
	add := func(id uint64, role raftkvv1.MemberRole) {
		var m *raftkvv1.Member
		if old := Find(cur, id); old != nil {
			m = proto.CloneOf(old)
		} else if added != nil && added.GetId() == id {
			m = proto.CloneOf(added)
		} else {
			m = &raftkvv1.Member{Id: id}
		}
		m.Role = role
		out.Members = append(out.Members, m)
	}
	for _, id := range result.Voters {
		add(id, raftkvv1.MemberRole_VOTER)
	}
	for _, id := range result.Learners {
		add(id, raftkvv1.MemberRole_LEARNER)
	}
	sortMembers(out)
	return out
}

// EncodeContext is the context of a conf-change entry: the member table after
// the change.
func EncodeContext(cfg *raftkvv1.ClusterConfig) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(cfg)
}

// FromEntry decodes the member table a conf-change entry establishes. The
// core's ConfState in the entry is authoritative for who is a voter, a learner
// or removed; addresses come from the entry's context, else from known (the
// table before the entry, may be nil).
func FromEntry(e raft.Entry, known *raftkvv1.ClusterConfig) (*raftkvv1.ClusterConfig, raft.ConfChange, error) {
	if e.Type != raft.EntryConfChange {
		return nil, raft.ConfChange{}, fmt.Errorf("membership: entry %d is not a conf change", e.Index)
	}
	cc, cs, err := raft.DecodeConfChange(e.Data)
	if err != nil {
		return nil, raft.ConfChange{}, fmt.Errorf("membership: entry %d: %w", e.Index, err)
	}
	table := known
	if len(cc.Context) > 0 {
		table = &raftkvv1.ClusterConfig{}
		if err := proto.Unmarshal(cc.Context, table); err != nil {
			return nil, raft.ConfChange{}, fmt.Errorf("membership: entry %d context: %w", e.Index, err)
		}
	}
	out := Next(table, cs, nil)
	out.ConfigIndex = e.Index
	return out, cc, nil
}

// RoleOf returns id's role in cfg (MEMBER_ROLE_UNSPECIFIED if absent).
func RoleOf(cfg *raftkvv1.ClusterConfig, id uint64) raftkvv1.MemberRole {
	if m := Find(cfg, id); m != nil {
		return m.GetRole()
	}
	return raftkvv1.MemberRole_MEMBER_ROLE_UNSPECIFIED
}
