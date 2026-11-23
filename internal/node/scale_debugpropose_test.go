//go:build debugpropose

package node_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
)

// WBS 2.3.2: five nodes commit 10,000 entries proposed through the debug
// Propose RPC, and every replica applies them in the same order.
func TestFiveNodesCommit10kEntriesViaDebugRPC(t *testing.T) {
	const total = 10_000
	// A slower tick keeps elections from firing under -race while 64 clients
	// saturate the leader's loop.
	c := newTestCluster(t, 5, func(o *clusterOptions) { o.tick = 100 * time.Millisecond })
	lead := c.leader()

	conn, err := grpc.NewClient("passthrough:///"+lead.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	var next, failures atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1)
				if i > total {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if _, err := node.DebugPropose(ctx, conn, []byte(fmt.Sprintf("entry-%05d", i))); err != nil {
					failures.Add(1)
					t.Errorf("DebugPropose(%d): %v", i, err)
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	if failures.Load() > 0 {
		t.FailNow()
	}

	var ref []string
	for id, n := range c.nodes {
		waitUntil(t, 60*time.Second, fmt.Sprintf("node %d to apply %d entries", id, total), func() bool {
			return len(commandsOf(n)) >= total
		})
		got := commandsOf(n)
		if len(got) != total {
			t.Fatalf("node %d applied %d commands, want %d", id, len(got), total)
		}
		if ref == nil {
			ref = got
			seen := map[string]bool{}
			for _, s := range got {
				seen[s] = true
			}
			if len(seen) != total {
				t.Fatalf("expected %d distinct commands, got %d", total, len(seen))
			}
			continue
		}
		if !reflect.DeepEqual(got, ref) {
			t.Fatalf("node %d applied a different sequence than the others", id)
		}
	}
}
