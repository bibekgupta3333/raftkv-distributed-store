package main

import (
	"fmt"
	"math"
	"time"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// Endpoint is one configured node: a display name and its gRPC address.
type Endpoint struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

// Poll is the outcome of one Cluster.Status call.
type Poll struct {
	Endpoint
	Resp *raftkvv1.StatusResponse
	Err  error
}

// NodeView is one node card. When the node is down the Raft fields keep their
// last-known values so the card can show them greyed out.
type NodeView struct {
	Endpoint
	Up       bool      `json:"up"`
	Seen     bool      `json:"seen"` // answered at least once
	Err      string    `json:"err,omitempty"`
	LastSeen time.Time `json:"lastSeen"`
	MemberID uint64    `json:"memberId,string"` // IDs are 64-bit (D-38); JSON numbers lose precision
	Role     string    `json:"role"`
	Term     uint64    `json:"term"`
	LeaderID uint64    `json:"leaderId,string"`
	Commit   uint64    `json:"commit"`
	Applied  uint64    `json:"applied"`
	LastLog  uint64    `json:"lastLog"`
	Snapshot uint64    `json:"snapshot"`
	// Lag is how far the node's match index trails the leader's last log
	// index, taken from the leader's followers[] (-1 when unknown: no leader
	// in view, or this node is the leader).
	Lag int64 `json:"lag"`
}

// Event is one timeline line, inferred by diffing two polls.
type Event struct {
	Time time.Time `json:"time"`
	Kind string    `json:"kind"`
	Node string    `json:"node,omitempty"`
	Text string    `json:"text"`
}

// Event kinds.
const (
	EventUnreachable = "unreachable"
	EventReachable   = "reachable"
	EventLeader      = "leader" // term and/or leader change
	EventSnapshot    = "snapshot"
)

// State is what the page renders; it is pushed whole on every poll.
type State struct {
	Time       time.Time  `json:"time"`
	IntervalMS int64      `json:"intervalMs"`
	ClusterID  uint64     `json:"clusterId,string"`
	Term       uint64     `json:"term"`
	LeaderID   uint64     `json:"leaderId,string"`
	LeaderName string     `json:"leaderName"`
	Nodes      []NodeView `json:"nodes"`
	// WritesPerSec is the leader's applied-index delta per second, one sample
	// per poll, oldest first.
	WritesPerSec []float64 `json:"writesPerSec"`
	Events       []Event   `json:"events"` // oldest first
}

const maxEvents = 200

// Observer folds successive polls into a State. It is not safe for concurrent
// use; the poll loop is its only caller.
type Observer struct {
	interval time.Duration
	maxRates int
	prev     *State
	rates    []float64
	events   []Event
}

// NewObserver keeps about 60 s of write-rate samples at the given interval.
func NewObserver(interval time.Duration) *Observer {
	n := int(math.Ceil(float64(60*time.Second) / float64(interval)))
	return &Observer{interval: interval, maxRates: max(n, 2)}
}

// Update folds one round of polls (in endpoint order) into a new State and
// appends the events inferred from the previous one.
func (o *Observer) Update(now time.Time, polls []Poll) State {
	cur := State{Time: now, IntervalMS: o.interval.Milliseconds(), Nodes: make([]NodeView, len(polls))}
	for i, p := range polls {
		var nv NodeView
		if o.prev != nil && i < len(o.prev.Nodes) && o.prev.Nodes[i].Endpoint == p.Endpoint {
			nv = o.prev.Nodes[i] // last-known values survive a failed poll
		}
		nv.Endpoint = p.Endpoint
		nv.Lag = -1
		if p.Err != nil || p.Resp == nil {
			nv.Up = false
			nv.Err = errText(p.Err)
		} else {
			r := p.Resp
			nv.Up, nv.Seen, nv.Err, nv.LastSeen = true, true, "", now
			nv.MemberID = r.GetHeader().GetMemberId()
			nv.Role = roleName(r.GetState())
			nv.Term, nv.LeaderID = r.GetTerm(), r.GetLeaderId()
			nv.Commit, nv.Applied, nv.LastLog = r.GetCommitIndex(), r.GetAppliedIndex(), r.GetLastLogIndex()
			nv.Snapshot = r.GetSnapshotIndex()
			if cur.ClusterID == 0 {
				cur.ClusterID = r.GetHeader().GetClusterId()
			}
		}
		cur.Nodes[i] = nv
	}
	cur.Term, cur.LeaderID = clusterView(cur.Nodes)
	if o.prev != nil && o.prev.Term > cur.Term {
		// Terms never go backwards; a lower maximum only means the nodes that
		// saw the newest term are unreachable right now.
		cur.Term = o.prev.Term
	}
	cur.LeaderName = nameOf(cur.Nodes, cur.LeaderID)
	if cur.ClusterID == 0 && o.prev != nil {
		cur.ClusterID = o.prev.ClusterID
	}
	fillLag(cur.Nodes, polls, cur.LeaderID)

	o.rates = append(o.rates, writeRate(o.prev, &cur))
	if len(o.rates) > o.maxRates {
		o.rates = o.rates[len(o.rates)-o.maxRates:]
	}
	o.events = append(o.events, Diff(o.prev, &cur)...)
	if len(o.events) > maxEvents {
		o.events = o.events[len(o.events)-maxEvents:]
	}
	cur.WritesPerSec = append([]float64(nil), o.rates...)
	cur.Events = append([]Event(nil), o.events...)
	o.prev = &cur
	return cur
}

// clusterView is the header's "majority view": the highest term any reachable
// node reports, and the leader most reachable nodes at that term agree on.
// A deposed leader stuck in a minority partition reports a lower term and is
// ignored.
func clusterView(nodes []NodeView) (term, leader uint64) {
	for _, n := range nodes {
		if n.Up && n.Term > term {
			term = n.Term
		}
	}
	votes := map[uint64]int{}
	for _, n := range nodes {
		if n.Up && n.Term == term && n.LeaderID != 0 {
			votes[n.LeaderID]++
		}
	}
	best := 0
	for id, v := range votes {
		if v > best || (v == best && id < leader) {
			leader, best = id, v
		}
	}
	return term, leader
}

// fillLag sets each node's lag from the leader's followers[] match index.
func fillLag(nodes []NodeView, polls []Poll, leader uint64) {
	if leader == 0 {
		return
	}
	var lr *raftkvv1.StatusResponse
	for i, n := range nodes {
		if n.Up && n.MemberID == leader && polls[i].Resp.GetState() == raftkvv1.RaftState_LEADER {
			lr = polls[i].Resp
		}
	}
	if lr == nil {
		return
	}
	match := map[uint64]uint64{}
	for _, f := range lr.GetFollowers() {
		match[f.GetMemberId()] = f.GetMatchIndex()
	}
	for i := range nodes {
		if m, ok := match[nodes[i].MemberID]; ok && nodes[i].Seen {
			nodes[i].Lag = int64(lr.GetLastLogIndex()) - int64(m)
		}
	}
}

// writeRate is the current leader's applied-index delta per second since the
// previous poll. It is 0 when the leader is unknown or was not reachable in
// the previous poll (a fresh leader has no baseline yet).
func writeRate(prev, cur *State) float64 {
	if prev == nil || cur.LeaderID == 0 {
		return 0
	}
	dt := cur.Time.Sub(prev.Time).Seconds()
	if dt <= 0 {
		return 0
	}
	for i, n := range cur.Nodes {
		if !n.Up || n.MemberID != cur.LeaderID || i >= len(prev.Nodes) {
			continue
		}
		p := prev.Nodes[i]
		if !p.Up || p.MemberID != n.MemberID || n.Applied < p.Applied {
			return 0
		}
		return float64(n.Applied-p.Applied) / dt
	}
	return 0
}

// Diff infers timeline events from two successive states (prev may be nil on
// the first poll). Polls are 500 ms apart, so a blip shorter than that can be
// missed (D-48).
func Diff(prev, cur *State) []Event {
	var out []Event
	add := func(kind, node, format string, args ...any) {
		out = append(out, Event{Time: cur.Time, Kind: kind, Node: node, Text: fmt.Sprintf(format, args...)})
	}
	for i, n := range cur.Nodes {
		var p *NodeView
		if prev != nil && i < len(prev.Nodes) && prev.Nodes[i].Endpoint == n.Endpoint {
			p = &prev.Nodes[i]
		}
		switch {
		case p == nil && !n.Up:
			add(EventUnreachable, n.Name, "%s unreachable", n.Name)
		case p != nil && p.Up && !n.Up:
			add(EventUnreachable, n.Name, "%s unreachable", n.Name)
		case p != nil && !p.Up && n.Up:
			add(EventReachable, n.Name, "%s back (term %d, applied %d)", n.Name, n.Term, n.Applied)
		}
		if p != nil && p.Seen && n.Up && n.Snapshot != p.Snapshot && n.Snapshot != 0 {
			add(EventSnapshot, n.Name, "%s snapshot @ %d", n.Name, n.Snapshot)
		}
	}
	var pTerm, pLead uint64
	pName := "none"
	if prev != nil {
		pTerm, pLead, pName = prev.Term, prev.LeaderID, orNone(prev.LeaderName)
	}
	cName := orNone(cur.LeaderName)
	switch {
	case cur.Term != pTerm && cur.LeaderID != pLead:
		add(EventLeader, cur.LeaderName, "term %d -> %d, leader %s -> %s", pTerm, cur.Term, pName, cName)
	case cur.Term != pTerm:
		add(EventLeader, cur.LeaderName, "term %d -> %d (leader %s)", pTerm, cur.Term, cName)
	case cur.LeaderID != pLead:
		add(EventLeader, cur.LeaderName, "leader %s -> %s (term %d)", pName, cName, cur.Term)
	}
	return out
}

func nameOf(nodes []NodeView, id uint64) string {
	if id == 0 {
		return ""
	}
	for _, n := range nodes {
		if n.Seen && n.MemberID == id {
			return n.Name
		}
	}
	return fmt.Sprintf("member %d", id)
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func roleName(s raftkvv1.RaftState) string {
	switch s {
	case raftkvv1.RaftState_FOLLOWER:
		return "FOLLOWER"
	case raftkvv1.RaftState_PRE_CANDIDATE:
		return "PRE-CANDIDATE"
	case raftkvv1.RaftState_CANDIDATE:
		return "CANDIDATE"
	case raftkvv1.RaftState_LEADER:
		return "LEADER"
	case raftkvv1.RaftState_LEARNER_STATE:
		return "LEARNER"
	default:
		return "UNKNOWN"
	}
}

func errText(err error) string {
	if err == nil {
		return "no response"
	}
	return err.Error()
}
