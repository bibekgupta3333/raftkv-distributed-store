package observability

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// WBS 7.3.1: the committed Grafana dashboard is valid JSON and every metric it
// queries is one this package registers, so it cannot drift silently.
func TestDashboardUsesRegisteredMetrics(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/compose/grafana/dashboards/raftkv.json")
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		UID    string `json:"uid"`
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}
	if dash.UID == "" || len(dash.Panels) == 0 {
		t.Fatalf("dashboard has no uid or panels")
	}

	// Touch every vector so it shows up in Gather.
	m := NewMetrics()
	m.ObserveStatus(raft.Status{Term: 1, Lead: 1})
	m.ObserveFsync(time.Millisecond, errors.New("x"))
	m.MessageSent(1, raft.MsgApp)
	m.SendFailure(1)
	m.Reconnect(1)
	m.Dropped(1)
	m.rpcRequests.WithLabelValues("/m", "OK").Inc()
	m.rpcDuration.WithLabelValues("/m").Observe(0)
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, f := range families {
		known[f.GetName()] = true
	}

	name := regexp.MustCompile(`raftkv_[a-z_]+`)
	for _, p := range dash.Panels {
		for _, tg := range p.Targets {
			for _, n := range name.FindAllString(tg.Expr, -1) {
				base := n
				for _, suf := range []string{"_bucket", "_count", "_sum"} {
					if b, ok := strings.CutSuffix(n, suf); ok && known[b] {
						base = b
					}
				}
				if !known[base] {
					t.Errorf("panel %q queries %s, which is not registered", p.Title, n)
				}
			}
		}
	}
}
