package node_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// WBS 6.2.3, 7.4.5: a leader stopped under load hands leadership over through
// TimeoutNow, not through an election timeout. Under load the chosen target
// usually lags by the last appends, so the stopping leader must keep
// replicating to it after its Run context is cancelled. When the transport
// was started with that context, the catch-up appends were dropped and the
// cluster only moved on after a follower's election timeout.
func TestShutdownHandoverUnderLoad(t *testing.T) {
	const tick = 50 * time.Millisecond // election timeout 500 ms - 1 s
	c := newTestCluster(t, 3, func(o *clusterOptions) { o.tick = tick })
	for round := range 3 {
		lead := c.leader()
		oldID := lead.ID()
		ctx, stopLoad := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil {
					_, _, _ = lead.Propose(ctx, []byte("load"))
				}
			}()
		}
		time.Sleep(300 * time.Millisecond)

		c.stop(oldID)
		stopLoad()
		wg.Wait()

		// The two log records bracket the handover (node.go startHandover and
		// loop). Timed out, the old leader still leads (leader = itself). Fallen
		// back to an election, it stepped down only after a follower's election
		// timeout: at least 10 ticks. A TimeoutNow handover takes a few round
		// trips and the tick on which the loop notices it no longer leads.
		var began, ended time.Time
		for _, line := range strings.Split(strings.TrimSpace(c.logs[oldID].String()), "\n") {
			var rec struct {
				Time   time.Time `json:"time"`
				Msg    string    `json:"msg"`
				Leader uint64    `json:"leader"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil {
				continue
			}
			switch rec.Msg {
			case "handing leadership over before shutdown":
				began = rec.Time
			case "shutdown handover finished":
				ended = rec.Time
				if rec.Leader == oldID {
					t.Fatalf("round %d: node %d timed out handing over: %s", round, oldID, line)
				}
			}
		}
		if began.IsZero() || ended.IsZero() {
			t.Fatalf("round %d: node %d logged no complete handover:\n%s", round, oldID, c.logs[oldID])
		}
		took := ended.Sub(began)
		if took > 6*tick {
			t.Fatalf("round %d: leader %d handed over in %v: it fell back to an election timeout", round, oldID, took)
		}
		nl := c.leader()
		if nl.ID() == oldID {
			t.Fatalf("round %d: stopped node %d still leads", round, oldID)
		}
		t.Logf("round %d: leader %d -> %d, handover took %v", round, oldID, nl.ID(), took)
		c.start(oldID, clusterOptions{tick: tick})
	}
}
