package node

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/lease"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// The read path (D-05, system-design 3): ReadIndex on the leader and on
// followers, an optional leader lease (D-25), and waiting for the applied
// index. Requests reach the core through the driver loop like proposals; the
// answers come back in Ready.ReadStates and are matched by request context.

// ErrReadRejected means the ReadIndex request failed without an answer: the
// node that ran it lost (or never had) leadership, or the leader could not be
// reached. Retrying is always safe; [Node.LinearizableRead] does so itself.
var ErrReadRejected = errors.New("node: read index rejected")

// Request-context kinds: a client read, or the node's own lease renewal.
const (
	ctxRead  = 'R'
	ctxLease = 'L'
)

type readRequest struct {
	ctx []byte
}

type readResult struct {
	index uint64
	lease bool
	err   error
}

func readContext(kind byte, id uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{kind}, id)
}

// leaseState is the leader lease (D-25). The loop extends and revokes it;
// readers check it after a lease read. mu guards l.
type leaseState struct {
	l *lease.Lease
	// The loop's single renewal round in flight, if any. Loop-owned.
	renewing  bool
	renewCtx  string
	renewAt   time.Duration
	renewTerm uint64
}

// monotonic is the node's lease clock: time since start on the monotonic
// clock, never the wall clock (D-24, D-25).
func (n *Node) monotonic() time.Duration { return time.Since(n.epoch) }

// ReadIndex returns an index at which a linearizable read may be served on
// this node once its applied index reaches it (D-05). On the leader it costs
// one heartbeat round shared with concurrent readers (or nothing, with a
// valid lease); on a follower one round trip to the leader. viaLease reports
// that the index came from the lease and must be re-checked after the read.
//
// Errors: [raft.ErrReadIndexDropped] when no leader is known,
// [ErrReadRejected] (retry), [ErrStopped], or ctx's error.
func (n *Node) ReadIndex(ctx context.Context) (index uint64, viaLease bool, err error) {
	key := readContext(ctxRead, n.readSeq.Add(1))
	ch := make(chan readResult, 1)
	n.readMu.Lock()
	n.readWait[string(key)] = ch
	n.readMu.Unlock()
	defer func() {
		n.readMu.Lock()
		delete(n.readWait, string(key))
		n.readMu.Unlock()
	}()
	select {
	case n.readCh <- readRequest{ctx: key}:
	case <-ctx.Done():
		return 0, false, ctx.Err()
	case <-n.stopping:
		return 0, false, ErrStopped
	}
	select {
	case res := <-ch:
		return res.index, res.lease, res.err
	case <-ctx.Done():
		return 0, false, ctx.Err()
	case <-n.stopping:
		return 0, false, ErrStopped
	}
}

// WaitApplied blocks until this node has applied index, ctx is done, or the
// node stops (WBS 4.2.1).
func (n *Node) WaitApplied(ctx context.Context, index uint64) error {
	for {
		n.appliedMu.Lock()
		ch := n.appliedCh // taken before the check, so no wakeup is missed
		n.appliedMu.Unlock()
		if n.applier.Applied() >= index {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopping:
			return ErrStopped
		}
	}
}

// LinearizableRead runs read once it is safe to serve a linearizable read from
// local state (D-05): it gets a read index (retrying rejections with backoff
// until ctx ends), waits until the applied index reaches it, and runs read. A
// lease read is checked again after read and redone if the lease expired
// meanwhile (D-25: a pause during the read must not return stale data).
func (n *Node) LinearizableRead(ctx context.Context, read func() error) error {
	backoff := 5 * time.Millisecond
	for {
		index, viaLease, err := n.ReadIndex(ctx)
		if errors.Is(err, ErrReadRejected) {
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
			backoff = min(2*backoff, 200*time.Millisecond)
			continue
		}
		if err != nil {
			return err
		}
		if err := n.WaitApplied(ctx, index); err != nil {
			return err
		}
		if err := read(); err != nil {
			return err
		}
		if viaLease && !n.LeaseValid() {
			continue
		}
		return nil
	}
}

// LeaseValid reports whether this node is the leader and holds a valid read
// lease right now. It is always false when lease reads are off (D-25).
func (n *Node) LeaseValid() bool {
	if n.lease == nil {
		return false
	}
	st := n.Status()
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	return st.State == raft.StateLeader && n.lease.l.Valid(st.Term, n.monotonic())
}

// handleRead runs on the loop: answer from the lease if one is held, else ask
// the core.
func (n *Node) handleRead(rq readRequest) {
	if n.lease != nil {
		st := n.rn.Status()
		n.leaseMu.Lock()
		ok := st.State == raft.StateLeader && n.lease.l.Valid(st.Term, n.monotonic())
		n.leaseMu.Unlock()
		if ok {
			// D-25: the lease proves no other leader exists, and it was
			// earned by a ReadIndex round, so this term has committed an
			// entry and the commit index is a valid read index.
			n.deliverRead(string(rq.ctx), readResult{index: st.Commit, lease: true})
			return
		}
	}
	if err := n.rn.ReadIndex(rq.ctx); err != nil {
		n.deliverRead(string(rq.ctx), readResult{err: err})
	}
}

func (n *Node) deliverRead(key string, res readResult) {
	n.readMu.Lock()
	ch := n.readWait[key]
	delete(n.readWait, key)
	n.readMu.Unlock()
	if ch != nil {
		ch <- res // buffered, one result per request
	}
}

// handleReadStates hands the core's answers to their readers, then ends the
// lease if this node stopped leading. Answers first: a round that finished
// before a step-down in the same Ready was confirmed while we led, and the
// lease's term check keeps it from outliving the step-down.
func (n *Node) handleReadStates(rd raft.Ready) {
	for _, rs := range rd.ReadStates {
		key := string(rs.RequestCtx)
		if n.lease != nil && n.lease.renewing && key == n.lease.renewCtx {
			n.lease.renewing = false
			if !rs.Rejected {
				n.leaseMu.Lock()
				n.lease.l.Extend(n.lease.renewTerm, n.lease.renewAt)
				n.leaseMu.Unlock()
			}
			continue
		}
		res := readResult{index: rs.Index}
		if rs.Rejected {
			res = readResult{err: ErrReadRejected}
		}
		n.deliverRead(key, res)
	}
	if n.lease != nil && rd.SoftState != nil && rd.SoftState.State != raft.StateLeader {
		n.leaseMu.Lock()
		n.lease.l.Revoke()
		n.leaseMu.Unlock()
		n.lease.renewing = false
	}
}

// maybeRenewLease runs on every tick: a leader keeps one renewal round in
// flight. The lease counts from the moment the round is requested, which is
// no later than the heartbeats that confirm it are sent (D-25).
func (n *Node) maybeRenewLease() {
	if n.lease == nil || n.lease.renewing {
		return
	}
	st := n.rn.Status()
	if st.State != raft.StateLeader {
		return
	}
	key := readContext(ctxLease, n.readSeq.Add(1))
	at := n.monotonic()
	if n.rn.ReadIndex(key) == nil {
		n.lease.renewing, n.lease.renewCtx, n.lease.renewAt, n.lease.renewTerm = true, string(key), at, st.Term
	}
}

// notifyApplied wakes every WaitApplied caller after an apply.
func (n *Node) notifyApplied() {
	n.appliedMu.Lock()
	close(n.appliedCh)
	n.appliedCh = make(chan struct{})
	n.appliedMu.Unlock()
}
