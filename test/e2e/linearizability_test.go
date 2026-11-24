//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
	"github.com/bibekgupta3333/raftkv-distributed-store/test/linearizability"
)

// clientAddr is node id's KV endpoint as published to the host (the node
// serves KV, Cluster and Raft on one port).
func clientAddr(id int) string { return peerAddr(id) }

// linFault is one fault of the linearizability nemesis. It runs on the test
// goroutine (the fault driver may call t.Fatal) and undoes itself.
type linFault struct {
	name string
	run  func(c *Cluster)
}

func linFaults(hold time.Duration) []linFault {
	others := func(except ...int) []int {
		var out []int
	next:
		for id := 1; id <= NumNodes; id++ {
			for _, e := range except {
				if id == e {
					continue next
				}
			}
			out = append(out, id)
		}
		return out
	}
	return []linFault{
		{"kill -9 leader", func(c *Cluster) {
			lead := c.WaitLeader(30 * time.Second)
			c.Kill(lead)
			time.Sleep(hold)
			c.Restart(lead)
		}},
		{"SIGSTOP leader", func(c *Cluster) {
			// The paused leader is deposed while frozen; when it wakes up it
			// must not serve a stale linearizable read (D-05, D-08).
			lead := c.WaitLeader(30 * time.Second)
			c.Pause(lead)
			time.Sleep(hold + time.Second)
			c.Resume(lead)
		}},
		{"SIGSTOP follower", func(c *Cluster) {
			lead := c.WaitLeader(30 * time.Second)
			f := others(lead)[0]
			c.Pause(f)
			time.Sleep(hold)
			c.Resume(f)
		}},
		{"partition leader alone", func(c *Cluster) {
			lead := c.WaitLeader(30 * time.Second)
			c.Partition(lead)
			time.Sleep(hold + time.Second)
			c.Heal()
		}},
		{"partition minority (leader + follower)", func(c *Cluster) {
			lead := c.WaitLeader(30 * time.Second)
			c.Partition(lead, others(lead)[0])
			time.Sleep(hold + time.Second)
			c.Heal()
		}},
	}
}

// WBS 4.6.2, M4 exit criterion 1: concurrent clients run a mix of Put, Get,
// Delete, CompareAndSwap and paged Range against the 5-node compose cluster
// while the nemesis kills the leader (kill -9), freezes the leader and a
// follower (SIGSTOP), and partitions the leader alone and in a two-node
// minority. Half the workers send their reads (Get and Range pages) to one
// fixed member, so linearizable reads are served by followers as well as by
// the leader (ReadIndex, D-05); the other half read through the leader the
// client has cached. The whole history must check as linearizable with
// Porcupine (testing.md 3.3): 0 violations, and a checker timeout is a failure.
//
// pr: one cycle of the five faults. nightly: E2E_LIN_CYCLES (default 6)
// cycles, each after the first in a seeded random order and on fresh keys
// (Workload.NextEpoch), so each cycle's keys are checked independently.
// Artifacts in out/e2e/linearizability: history.jsonl, fault-log.txt, node
// logs and, on failure, the Porcupine visualization.
func TestLinearizableKVUnderFaults(t *testing.T) {
	cycles := pick(1, 6, "E2E_LIN_CYCLES")
	c := Start(t, "linearizability")
	runLinearizable(t, c, 200, func(wl *linearizability.Workload) {
		const hold = 3 * time.Second
		r := rand.New(rand.NewPCG(seed(), 0x4c494e)) // "LIN"
		for cycle := range cycles {
			faults := linFaults(hold)
			if cycle > 0 {
				epoch := wl.NextEpoch()
				r.Shuffle(len(faults), func(i, j int) { faults[i], faults[j] = faults[j], faults[i] })
				c.Faults.Logf("linearizability cycle %d/%d epoch=%d", cycle+1, cycles, epoch)
				time.Sleep(time.Second)
			}
			for _, f := range faults {
				c.Faults.Logf("linearizability fault: %s (ops so far %d)", f.name, wl.Ops())
				f.run(c)
				time.Sleep(3 * time.Second) // quiet gap: the cluster recovers, clients catch up
			}
		}
	})
}

// runLinearizable runs the recorded Porcupine workload against the cluster's
// five client endpoints while nemesis runs on the test goroutine, then checks
// the history (0 violations, at least minOK successful operations, every
// operation kind present) and that the live members still commit and
// converge. Removed slots simply stop answering; the clients move on.
func runLinearizable(t *testing.T, c *Cluster, minOK int, nemesis func(wl *linearizability.Workload)) {
	t.Helper()
	c.WaitLeader(30 * time.Second)

	eps := make([]string, NumNodes)
	for i := range eps {
		eps[i] = clientAddr(i + 1)
	}
	newClientWith := func(noSessions bool, endpoints ...string) *client.Client {
		cl, err := client.New(client.Config{Endpoints: endpoints, AttemptTimeout: 1500 * time.Millisecond,
			ConnectTimeout: 300 * time.Millisecond, DisableSessions: noSessions})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	newClient := func(endpoints ...string) *client.Client { return newClientWith(false, endpoints...) }
	// Wait until the cluster serves KV writes from the host.
	warm := newClient(eps...)
	Eventually(t, 60*time.Second, "first KV write", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := warm.Put(ctx, []byte("warmup"), []byte("1"))
		return err == nil
	})

	const workers = 8
	rec := linearizability.NewRecorder()
	wl := linearizability.NewWorkload(rec, linearizability.WorkloadConfig{
		Workers: workers, Groups: 3, Keys: 3, OpTime: 6 * time.Second, Seed: seed()})
	kvs := make([]linearizability.KV, workers)
	for i := range kvs {
		// Workers 1 and 5 write without a session: a write whose outcome
		// is unknown is not retried (it could apply twice) but surfaces as
		// INDETERMINATE, so the history has "info" writes that the model
		// treats as may-or-may-not-have-happened (testing.md 3.2). The
		// others get exactly-once retries (D-09).
		cl := newClientWith(i%4 == 1, eps...)
		kvs[i] = cl
		if i%2 == 0 {
			member := (i/2)%NumNodes + 1
			kvs[i] = linearizability.PinnedReads{KV: cl, Reads: newClient(clientAddr(member))}
		}
	}

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	wlDone := make(chan struct{})
	go func() {
		defer close(wlDone)
		wl.Run(runCtx, func(i int) linearizability.KV { return kvs[i] })
	}()

	time.Sleep(2 * time.Second)
	nemesis(wl)
	time.Sleep(2 * time.Second) // some load on the final configuration
	stop()
	<-wlDone
	c.Heal()

	events := rec.Events()
	if f, err := os.Create(filepath.Join(c.OutDir, "history.jsonl")); err == nil {
		_ = rec.WriteJSONL(f)
		_ = f.Close()
	} else {
		t.Errorf("save history: %v", err)
	}
	start := time.Now()
	rep := linearizability.Check(events, linearizability.DefaultTimeout)
	mix := linearizability.Breakdown(events)
	t.Logf("history: %d events, %d ops checked in %v, %s, verdict %s", len(events), rep.Ops,
		time.Since(start).Round(time.Millisecond), rep.Stats, rep.Verdict)
	t.Logf("checked operations by kind: %v", mix)
	if !rep.OK() {
		html := filepath.Join(c.OutDir, "linearizability-failure.html")
		if err := rep.SaveHTML(html); err != nil {
			t.Errorf("save visualization: %v", err)
		}
		t.Fatalf("history is not linearizable (%s): see %s and %s", rep.Verdict, html,
			filepath.Join(c.OutDir, "history.jsonl"))
	}
	if rep.Stats.OK < minOK {
		t.Fatalf("only %d operations succeeded; the run proves little", rep.Stats.OK)
	}
	for _, kind := range []string{"put", "get", "delete", "cas-swapped", "cas-failed", "range-page"} {
		if mix[kind] == 0 {
			t.Errorf("no successful %s in the history", kind)
		}
	}

	// The cluster is whole again and still commits.
	idx := c.MustPropose(fmt.Sprintf("post-linearizability-%d", seed()))
	if got := c.WaitConverged(90 * time.Second); got < idx {
		t.Fatalf("converged at %d, below %d", got, idx)
	}
}
