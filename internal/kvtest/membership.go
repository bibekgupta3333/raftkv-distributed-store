package kvtest

import (
	"context"
	"errors"
	"time"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Membership helpers (D-06, D-23). They drive the leader's node API directly
// and retry the transient refusals (a change in flight, a leader change, a
// learner still catching up) until timeout.

// onLeader runs f on the current leader until it returns nil or a permanent
// error, or timeout passes.
func (c *Cluster) onLeader(timeout time.Duration, what string, f func(ctx context.Context, n *node.Node) error) {
	c.tb.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		lead := c.Leader(time.Until(deadline))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := f(ctx, c.Node(lead).Node)
		cancel()
		if err == nil {
			return
		}
		last = err
		var nl *raft.NotLeaderError
		retry := errors.As(err, &nl) || errors.Is(err, raft.ErrConfChangePending) || errors.Is(err, raft.ErrConfChangeTooEarly) ||
			errors.Is(err, raft.ErrProposalDropped) || errors.Is(err, raft.ErrTransferInProgress) ||
			errors.Is(err, node.ErrLearnerNotCaughtUp) || errors.Is(err, node.ErrStopped) ||
			errors.Is(err, node.ErrProposalLost) || errors.Is(err, context.DeadlineExceeded)
		if !retry {
			c.tb.Fatalf("%s: %v", what, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.tb.Fatalf("%s: still failing after %v: %v", what, timeout, last)
}

// AddLearner reserves an address, adds a learner there through the leader and
// starts it with -join under the ID the leader assigned (D-38). It returns
// the new ID.
func (c *Cluster) AddLearner(timeout time.Duration) uint64 {
	c.tb.Helper()
	addr := c.Reserve()
	var id uint64
	c.onLeader(timeout, "AddLearner", func(ctx context.Context, n *node.Node) error {
		m, err := n.AddLearner(ctx, &raftkvv1.Member{PeerUrl: addr})
		if err == nil {
			id = m.GetId()
		}
		return err
	})
	c.StartJoined(id, addr)
	return id
}

// Promote promotes learner id once it has caught up.
func (c *Cluster) Promote(id uint64, timeout time.Duration) {
	c.tb.Helper()
	c.onLeader(timeout, "PromoteLearner", func(ctx context.Context, n *node.Node) error {
		return n.PromoteLearner(ctx, id)
	})
}

// RemoveMember removes member id through the leader (it may be the leader
// itself) and then forgets the node.
func (c *Cluster) RemoveMember(id uint64, timeout time.Duration) {
	c.tb.Helper()
	c.onLeader(timeout, "RemoveMember", func(ctx context.Context, n *node.Node) error {
		err := n.RemoveMember(ctx, id, true)
		if errors.Is(err, node.ErrUnknownMember) {
			return nil // a retry after an unknown outcome: it worked
		}
		return err
	})
	c.Forget(id)
}

// Replace replaces member old by a new voter (system-design 7.2): add a
// learner, let it catch up, promote it, remove old. It returns the new ID.
func (c *Cluster) Replace(old uint64, timeout time.Duration) uint64 {
	c.tb.Helper()
	id := c.AddLearner(timeout)
	c.Promote(id, timeout)
	c.RemoveMember(old, timeout)
	return id
}
