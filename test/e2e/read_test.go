//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// M4 exit criterion 2 (e2e; the sim and in-process versions are
// TestSimReadPartitionedOldLeader and TestPartitionedOldLeaderServesNoStaleRead):
// an old leader never serves a linearizable read once a new leader exists.
// Two ways to depose it: an iptables partition (it stays up, cut off) and
// SIGSTOP (it is frozen while the others elect, then wakes up still believing
// it leads). In both cases the majority overwrites the key, and every
// linearizable Get sent to the old leader afterwards must either fail or
// return the new value (D-05 ReadIndex, D-08 CheckQuorum).
func TestOldLeaderServesNoStaleRead(t *testing.T) {
	c := Start(t, "stale-leader")
	c.WaitLeader(30 * time.Second)
	all := make([]string, NumNodes)
	for i := range all {
		all[i] = clientAddr(i + 1)
	}
	newClient := func(eps ...string) *client.Client {
		cl, err := client.New(client.Config{Endpoints: eps, AttemptTimeout: time.Second,
			ConnectTimeout: 300 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	put := func(cl *client.Client, val string) {
		t.Helper()
		Eventually(t, 60*time.Second, "put "+val, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := cl.Put(ctx, []byte("k"), []byte(val))
			return err == nil
		})
	}
	// readOld sends linearizable Gets to the old leader for a while; none may
	// return anything but want.
	readOld := func(old int, want string) (served, failed int) {
		t.Helper()
		cl := newClient(clientAddr(old))
		for end := time.Now().Add(4 * time.Second); time.Now().Before(end); {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			g, err := cl.Get(ctx, []byte("k"))
			cancel()
			switch {
			case err != nil:
				failed++
			case !g.Found || string(g.KV.Value) != want:
				val := "<absent>"
				if g.Found {
					val = string(g.KV.Value)
				}
				t.Fatalf("old leader %d served a stale linearizable read: %q, want %q", old, val, want)
			default:
				served++
			}
			time.Sleep(100 * time.Millisecond)
		}
		return served, failed
	}
	others := func(old int) []int {
		var out []int
		for id := 1; id <= NumNodes; id++ {
			if id != old {
				out = append(out, id)
			}
		}
		return out
	}
	addrs := func(ids []int) []string {
		var out []string
		for _, id := range ids {
			out = append(out, clientAddr(id))
		}
		return out
	}

	// 1. Partition: the old leader is up but cut off from the majority.
	put(newClient(all...), "v1")
	old := c.WaitLeader(30 * time.Second)
	rest := others(old)
	c.Partition(old)
	nl := c.WaitNewLeader(30*time.Second, old, rest...)
	put(newClient(addrs(rest)...), "v2")
	served, failed := readOld(old, "v2")
	t.Logf("partition: old leader %d, new leader %d; reads on the old leader: %d failed, %d served (all v2)",
		old, nl, failed, served)
	if served > 0 {
		t.Fatalf("a partitioned old leader served %d linearizable reads", served)
	}
	c.Heal()

	// 2. SIGSTOP: the old leader is frozen through the election, then wakes up.
	old = c.WaitLeader(30 * time.Second)
	put(newClient(all...), "v3")
	rest = others(old)
	c.Pause(old)
	nl = c.WaitNewLeader(30*time.Second, old, rest...)
	put(newClient(addrs(rest)...), "v4")
	c.Resume(old)
	served, failed = readOld(old, "v4")
	t.Logf("sigstop: old leader %d, new leader %d; reads on the woken old leader: %d failed, %d served (all v4)",
		old, nl, failed, served)
}
