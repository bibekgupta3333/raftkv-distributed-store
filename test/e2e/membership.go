//go:build e2e

// Membership support for the e2e harness (M6, D-06, D-23, D-32, D-38).
//
// The compose file has five fixed container slots (raftkv-nodek, hostname
// nodek, ports 1700k/1900k). A slot runs member ID k until it is rejoined: the
// harness removes the old member, asks the cluster to add a learner at
// nodek:7000 (the leader assigns a random ID), and recreates the slot's
// container on a fresh volume with RAFTKV_NODEk_ID=<that ID> and
// RAFTKV_NODEk_JOIN=true. Leader discovery still works on slots.
package e2e

import (
	"context"
	"fmt"
	"slices"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// Live returns the slots whose member is currently in the configuration.
func (c *Cluster) Live() []int {
	var out []int
	for k := 1; k <= NumNodes; k++ {
		if !c.removed[k] {
			out = append(out, k)
		}
	}
	return out
}

// MemberID returns the member ID slot k runs.
func (c *Cluster) MemberID(k int) uint64 { return c.ids[k] }

// SlotOf returns the slot running member id, or 0.
func (c *Cluster) SlotOf(id uint64) int {
	for k, v := range c.ids {
		if v == id && !c.removed[k] {
			return k
		}
	}
	return 0
}

// errReason returns the RaftKV ErrorInfo reason of a gRPC error, or "".
func errReason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

// transientAdmin are the refusals an admin call retries (api-design 9).
var transientAdmin = []string{"NO_LEADER", "NOT_LEADER", "CONF_CHANGE_IN_PROGRESS", "LEARNER_NOT_CAUGHT_UP", "OUTCOME_UNKNOWN", ""}

// Admin runs f against the Cluster service of the live slots in turn (any
// member forwards admin calls to the leader) until it succeeds, fails with a
// permanent error, or timeout passes; then it fails the test. f must be
// idempotent for retries after an unknown outcome; done may treat a specific
// error as success.
func (c *Cluster) Admin(timeout time.Duration, what string, f func(ctx context.Context, cc raftkvv1.ClusterClient) error, done func(error) bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		live := c.Live()
		slot := live[attempt%len(live)]
		if lead, ok := c.Leader(); ok {
			slot = lead
		}
		conn, err := c.conn(slot)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			err = f(ctx, raftkvv1.NewClusterClient(conn))
			cancel()
		}
		if err == nil || (done != nil && done(err)) {
			c.Faults.Logf("%s ok (via slot %d)", what, slot)
			return
		}
		last = err
		if !slices.Contains(transientAdmin, errReason(err)) {
			c.t.Fatalf("%s: %v (reason %s)", what, err, errReason(err))
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("%s: still failing after %v: %v", what, timeout, last)
}

// Members lists the applied member table (linearizable).
func (c *Cluster) Members() []*raftkvv1.Member {
	c.t.Helper()
	var out []*raftkvv1.Member
	c.Admin(60*time.Second, "ListMembers", func(ctx context.Context, cc raftkvv1.ClusterClient) error {
		r, err := cc.ListMembers(ctx, &raftkvv1.ListMembersRequest{})
		out = r.GetMembers()
		return err
	}, nil)
	return out
}

// RemoveSlot removes slot k's member (with force: the caller keeps a majority
// alive) and stops its container. A removed member also stops by itself once
// it applies its removal (D-31).
func (c *Cluster) RemoveSlot(k int) {
	c.t.Helper()
	id := c.ids[k]
	c.Admin(90*time.Second, fmt.Sprintf("RemoveMember(%d, slot %d)", id, k), func(ctx context.Context, cc raftkvv1.ClusterClient) error {
		_, err := cc.RemoveMember(ctx, &raftkvv1.RemoveMemberRequest{MemberId: id, Force: true})
		return err
	}, func(err error) bool { return errReason(err) == "MEMBER_NOT_FOUND" }) // a retry after it worked
	c.removed[k] = true
	if out, err := c.composeCmd("stop", "-t", "10", fmt.Sprintf("node%d", k)); err != nil {
		c.t.Fatalf("stop removed slot %d: %v\n%s", k, err, out)
	}
	c.Faults.Logf("removed member %d; slot %d stopped", id, k)
}

// AddLearnerSlot adds a learner at slot k's address (the leader assigns a new
// random ID, D-38) and rejoins the slot under that ID. It returns the ID.
func (c *Cluster) AddLearnerSlot(k int) uint64 {
	c.t.Helper()
	if !c.removed[k] {
		c.t.Fatalf("AddLearnerSlot(%d): slot still holds member %d", k, c.ids[k])
	}
	var id uint64
	c.Admin(90*time.Second, fmt.Sprintf("AddLearner(slot %d)", k), func(ctx context.Context, cc raftkvv1.ClusterClient) error {
		r, err := cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{Name: fmt.Sprintf("node%d", k), PeerUrl: fmt.Sprintf("node%d:7000", k)})
		id = r.GetMember().GetId()
		return err
	}, nil)
	c.Rejoin(k, id)
	return id
}

// Rejoin recreates slot k's container on a fresh volume as a node joining
// under member id (--join, D-32).
func (c *Cluster) Rejoin(k int, id uint64) {
	c.t.Helper()
	svc := fmt.Sprintf("node%d", k)
	if out, err := c.composeCmd("rm", "-s", "-f", svc); err != nil {
		c.fault("rejoin", k, "rm", fmt.Errorf("%w: %s", err, out))
	}
	if _, err := c.docker("volume", "rm", fmt.Sprintf("raftkv_data%d", k)); err != nil {
		c.fault("rejoin", k, "volume rm", err)
	}
	c.slotEnv[k] = []string{fmt.Sprintf("RAFTKV_NODE%d_ID=%d", k, id), fmt.Sprintf("RAFTKV_NODE%d_JOIN=true", k)}
	if conn := c.conns[k]; conn != nil {
		_ = conn.Close()
		delete(c.conns, k)
	}
	if out, err := c.composeCmd("up", "-d", "--no-build", svc); err != nil {
		c.fault("rejoin", k, "up", fmt.Errorf("%w: %s", err, out))
	}
	c.ids[k], c.removed[k] = id, false
	c.fault("rejoin", k, fmt.Sprintf("joined as learner %d on a fresh volume", id), nil)
}

// PromoteSlot promotes slot k's learner once it has caught up.
func (c *Cluster) PromoteSlot(k int) {
	c.t.Helper()
	id := c.ids[k]
	c.Admin(180*time.Second, fmt.Sprintf("PromoteLearner(%d, slot %d)", id, k), func(ctx context.Context, cc raftkvv1.ClusterClient) error {
		_, err := cc.PromoteLearner(ctx, &raftkvv1.PromoteLearnerRequest{MemberId: id})
		return err
	}, nil)
}

// ReplaceSlot replaces slot old by a new voter in the free slot spare
// (system-design 7.2): add a learner, let it catch up, promote it, remove old.
func (c *Cluster) ReplaceSlot(old, spare int) uint64 {
	c.t.Helper()
	id := c.AddLearnerSlot(spare)
	c.PromoteSlot(spare)
	c.RemoveSlot(old)
	return id
}

// TransferTo moves leadership to slot k's member and returns the new leader ID.
func (c *Cluster) TransferTo(k int) uint64 {
	c.t.Helper()
	var leader uint64
	c.Admin(60*time.Second, fmt.Sprintf("TransferLeadership(slot %d)", k), func(ctx context.Context, cc raftkvv1.ClusterClient) error {
		r, err := cc.TransferLeadership(ctx, &raftkvv1.TransferLeadershipRequest{TargetMemberId: c.ids[k]})
		leader = r.GetNewLeaderId()
		return err
	}, nil)
	return leader
}

// RequireRefused checks for d that slot k (restarted empty under its old ID)
// is refused by its peers (D-32): it applies nothing and installs no snapshot.
func (c *Cluster) RequireRefused(k int, d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if s, err := c.State(k); err == nil && (s.Applied > 0 || s.Commit > 0) {
			c.t.Fatalf("slot %d caught up under its old ID %d after a wipe (D-32): %+v", k, c.ids[k], s)
		}
		if v, err := c.Metric(k, "raftkv_snapshot_installs_total"); err == nil && v > 0 {
			c.t.Fatalf("slot %d installed a snapshot under its old ID %d after a wipe (D-32)", k, c.ids[k])
		}
		time.Sleep(250 * time.Millisecond)
	}
	c.Faults.Logf("slot %d (member %d) refused after its wipe, as D-32 requires", k, c.ids[k])
}
