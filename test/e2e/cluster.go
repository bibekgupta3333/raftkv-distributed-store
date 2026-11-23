//go:build e2e

// WBS 2.6.1: the Go e2e framework. It drives the 5-node docker-compose cluster
// in deploy/compose from the host: start/stop, leader discovery through each
// node's /metrics, wait helpers, and proposals through the debug Propose RPC.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
)

const (
	// NumNodes is the fixed size of the compose cluster.
	NumNodes = 5

	// debugProposeMethod mirrors internal/node.DebugProposeMethod. It is
	// repeated here so this package builds without -tags debugpropose; only the
	// node image needs that tag.
	debugProposeMethod = "/raftkv.debug.v1.Debug/Propose"

	stateLeader = 3 // raftkv_raft_state value for a leader
)

// NodeState is one node's view, scraped from /metrics.
type NodeState struct {
	ID                                      int
	Term, State, Commit, Applied, LastIndex uint64
}

// Cluster is a handle on the compose cluster for one test.
type Cluster struct {
	t       testing.TB
	root    string
	compose string
	image   string
	OutDir  string
	Faults  *FaultLog
	conns   map[int]*grpc.ClientConn
	http    *http.Client
}

// repoRoot finds the directory holding go.mod.
func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("e2e: go.mod not found above " + dir)
		}
		dir = parent
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// ContainerName is the fixed container name of node id (set in the compose file).
func ContainerName(id int) string { return fmt.Sprintf("raftkv-node%d", id) }

// Start brings up a fresh cluster (empty volumes) and registers cleanup that
// saves node logs and tears everything down. The image must already be built
// (`make e2e-image`).
func Start(t testing.TB, name string) *Cluster {
	t.Helper()
	root := repoRoot()
	c := &Cluster{
		t:       t,
		root:    root,
		compose: filepath.Join(root, "deploy", "compose", "docker-compose.yml"),
		image:   envOr("IMAGE", "raftkvd:e2e"),
		OutDir:  filepath.Join(envOr("E2E_OUT", filepath.Join(root, "out", "e2e")), name),
		conns:   map[int]*grpc.ClientConn{},
		http:    &http.Client{Timeout: 2 * time.Second},
	}
	if err := os.MkdirAll(c.OutDir, 0o755); err != nil {
		t.Fatal(err)
	}
	c.Faults = NewFaultLog(t, filepath.Join(c.OutDir, "fault-log.txt"))
	t.Cleanup(c.teardown)

	c.down() // leftovers from an earlier run
	if out, err := c.composeCmd("up", "-d", "--no-build",
		"node1", "node2", "node3", "node4", "node5"); err != nil {
		t.Fatalf("compose up: %v\n%s", err, out)
	}
	return c
}

func (c *Cluster) composeCmd(args ...string) (string, error) {
	cmd := exec.Command("docker", append([]string{"compose", "-f", c.compose}, args...)...)
	cmd.Env = append(os.Environ(), "IMAGE="+c.image)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (c *Cluster) down() {
	_, _ = c.composeCmd("down", "-v", "--remove-orphans", "-t", "2")
}

func (c *Cluster) teardown() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	for id := 1; id <= NumNodes; id++ {
		out, _ := exec.Command("docker", "logs", ContainerName(id)).CombinedOutput()
		_ = os.WriteFile(filepath.Join(c.OutDir, fmt.Sprintf("node%d.log", id)), out, 0o644)
	}
	if os.Getenv("E2E_KEEP") == "" {
		c.down()
	}
}

// StopAll gracefully stops every node (SIGTERM), keeping the volumes.
func (c *Cluster) StopAll() {
	c.t.Helper()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = map[int]*grpc.ClientConn{}
	if out, err := c.composeCmd("stop", "-t", "10",
		"node1", "node2", "node3", "node4", "node5"); err != nil {
		c.t.Fatalf("compose stop: %v\n%s", err, out)
	}
}

// --- scraping -------------------------------------------------------------

func metricsURL(id int) string { return fmt.Sprintf("http://127.0.0.1:%d/metrics", 19000+id) }
func peerAddr(id int) string   { return fmt.Sprintf("127.0.0.1:%d", 17000+id) }

// State scrapes node id's /metrics. It errors if the node is unreachable.
func (c *Cluster) State(id int) (NodeState, error) {
	resp, err := c.http.Get(metricsURL(id))
	if err != nil {
		return NodeState{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return NodeState{}, fmt.Errorf("node %d /metrics: %s", id, resp.Status)
	}
	return parseState(id, resp.Body)
}

func parseState(id int, r io.Reader) (NodeState, error) {
	s := NodeState{ID: id}
	got := 0
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 2 || strings.HasPrefix(f[0], "#") {
			continue
		}
		v, err := strconv.ParseFloat(f[1], 64)
		if err != nil {
			continue
		}
		dst := map[string]*uint64{
			"raftkv_raft_term": &s.Term, "raftkv_raft_state": &s.State,
			"raftkv_raft_commit_index": &s.Commit, "raftkv_raft_applied_index": &s.Applied,
			"raftkv_raft_last_index": &s.LastIndex,
		}[f[0]]
		if dst != nil {
			*dst = uint64(v)
			got++
		}
	}
	if got < 5 {
		return NodeState{}, fmt.Errorf("node %d: only %d of 5 raft metrics found", id, got)
	}
	return s, sc.Err()
}

// States scrapes every node in ids; unreachable nodes are left out.
func (c *Cluster) States(ids ...int) map[int]NodeState {
	if len(ids) == 0 {
		ids = []int{1, 2, 3, 4, 5}
	}
	out := map[int]NodeState{}
	for _, id := range ids {
		if s, err := c.State(id); err == nil {
			out[id] = s
		}
	}
	return out
}

// Leader returns the leader among ids as seen right now: among nodes that
// report the leader state, the one with the highest term, provided it is
// strictly higher than every other reachable node's term or equal only to
// followers (a stale leader in a lower term loses). ok is false if there is
// none.
func (c *Cluster) Leader(ids ...int) (id int, ok bool) {
	var best NodeState
	for _, s := range c.States(ids...) {
		if s.State == stateLeader && (!ok || s.Term > best.Term) {
			best, ok = s, true
		}
	}
	return best.ID, ok
}

// --- wait helpers ---------------------------------------------------------

// Eventually polls cond every 250 ms until it is true or timeout passes, then
// fails the test. Every wait in the suite goes through it, so each is bounded.
func Eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// WaitLeader waits for a leader among ids (all nodes if none given).
func (c *Cluster) WaitLeader(timeout time.Duration, ids ...int) int {
	c.t.Helper()
	var lead int
	Eventually(c.t, timeout, fmt.Sprintf("a leader among %v", ids), func() bool {
		var ok bool
		lead, ok = c.Leader(ids...)
		return ok
	})
	return lead
}

// WaitNewLeader waits for a leader among ids other than old.
func (c *Cluster) WaitNewLeader(timeout time.Duration, old int, ids ...int) int {
	c.t.Helper()
	var lead int
	Eventually(c.t, timeout, fmt.Sprintf("a leader other than node %d", old), func() bool {
		var ok bool
		lead, ok = c.Leader(ids...)
		return ok && lead != old
	})
	return lead
}

// WaitConverged waits until every node in ids (all if none) reports the same
// applied index, equal to its last and commit index, then returns that index.
func (c *Cluster) WaitConverged(timeout time.Duration, ids ...int) uint64 {
	c.t.Helper()
	if len(ids) == 0 {
		ids = []int{1, 2, 3, 4, 5}
	}
	var idx uint64
	Eventually(c.t, timeout, fmt.Sprintf("nodes %v to converge", ids), func() bool {
		st := c.States(ids...)
		if len(st) != len(ids) {
			return false
		}
		first := true
		for _, s := range st {
			if s.Applied != s.Commit || s.Applied != s.LastIndex {
				return false
			}
			if first {
				idx, first = s.Applied, false
			} else if s.Applied != idx {
				return false
			}
		}
		return true
	})
	return idx
}

// --- proposals ------------------------------------------------------------

func (c *Cluster) conn(id int) (*grpc.ClientConn, error) {
	if cc, ok := c.conns[id]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient("passthrough:///"+peerAddr(id), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[id] = cc
	return cc, nil
}

// encodeEntry wraps a test payload in the log entry format the node applies:
// an encoded Put command (key "e2e/<data>", value data). Since M3 the nodes
// run the Pebble state machine, which treats an undecodable entry as fatal
// (D-33), so raw bytes can no longer be proposed. The request ID is 0: no
// client waits on the result.
func encodeEntry(data []byte) ([]byte, error) {
	cmd, err := command.Put(0, 0, append([]byte("e2e/"), data...), data)
	if err != nil {
		return nil, err
	}
	return command.Marshal(cmd)
}

// decodeEntry is the inverse of encodeEntry for a wal-dump payload: it returns
// the Put value, or raw unchanged if it is not a Put command.
func decodeEntry(raw string) string {
	cmd, err := command.Unmarshal([]byte(raw))
	if err != nil || command.KindOf(cmd) != command.KindPut {
		return raw
	}
	return string(cmd.GetPut().GetValue())
}

// Propose sends data (as a Put command, see encodeEntry) to node id's debug
// Propose RPC and returns the log index at which it was applied. An error
// means the proposal is not acknowledged; it may or may not eventually commit.
func (c *Cluster) Propose(ctx context.Context, id int, data []byte) (uint64, error) {
	cc, err := c.conn(id)
	if err != nil {
		return 0, err
	}
	entry, err := encodeEntry(data)
	if err != nil {
		return 0, err
	}
	resp := new(wrapperspb.UInt64Value)
	if err := cc.Invoke(ctx, debugProposeMethod, wrapperspb.Bytes(entry), resp); err != nil {
		return 0, err
	}
	return resp.GetValue(), nil
}

// ProposeAmong retries data against the current leader among ids until one
// proposal is acknowledged or ctx ends. Each attempt is bounded by attempt.
func (c *Cluster) ProposeAmong(ctx context.Context, attempt time.Duration, data []byte, ids ...int) (uint64, error) {
	var last error
	for ctx.Err() == nil {
		lead, ok := c.Leader(ids...)
		if !ok {
			last = errors.New("no leader")
			time.Sleep(200 * time.Millisecond)
			continue
		}
		actx, cancel := context.WithTimeout(ctx, attempt)
		idx, err := c.Propose(actx, lead, data)
		cancel()
		if err == nil {
			return idx, nil
		}
		last = err
		time.Sleep(200 * time.Millisecond)
	}
	return 0, fmt.Errorf("propose %q: %w (last: %v)", data, ctx.Err(), last)
}

// MustPropose is ProposeAmong with a 30 s budget that fails the test.
func (c *Cluster) MustPropose(data string, ids ...int) uint64 {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	idx, err := c.ProposeAmong(ctx, 5*time.Second, []byte(data), ids...)
	if err != nil {
		c.t.Fatal(err)
	}
	return idx
}

// --- WAL inspection -------------------------------------------------------

// WALEntry is one line of `raftkvctl debug wal-dump`.
type WALEntry struct {
	Index uint64
	Data  string
}

// WALDump is the parsed wal-dump of one stopped node.
type WALDump struct {
	Entries map[uint64]string // index -> payload (the Put value for e2e entries)
	Summary string            // the "summary ..." line, which includes the log digest
}

// DumpWALs runs `raftkvctl debug wal-dump` against each node's volume. The
// cluster must be stopped (StopAll).
func (c *Cluster) DumpWALs() map[int]WALDump {
	c.t.Helper()
	out := map[int]WALDump{}
	for id := 1; id <= NumNodes; id++ {
		cmd := exec.Command("docker", "run", "--rm",
			"-v", fmt.Sprintf("raftkv_data%d:/data", id),
			"--entrypoint", "/usr/local/bin/raftkvctl", c.image,
			"debug", "wal-dump", "-data-dir", "/data", "-max-data", "256")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		b, err := cmd.Output()
		if err != nil {
			c.t.Fatalf("wal-dump node %d: %v\n%s", id, err, stderr.String())
		}
		_ = os.WriteFile(filepath.Join(c.OutDir, fmt.Sprintf("wal-node%d.txt", id)), b, 0o644)
		d := WALDump{Entries: map[uint64]string{}}
		for _, line := range strings.Split(string(b), "\n") {
			switch {
			case strings.HasPrefix(line, "summary "):
				d.Summary = line
			case strings.HasPrefix(line, "entry index="):
				idx, data, ok := parseEntryLine(line)
				if ok {
					d.Entries[idx] = decodeEntry(data)
				}
			}
		}
		if d.Summary == "" {
			c.t.Fatalf("wal-dump node %d: no summary line:\n%s", id, b)
		}
		out[id] = d
	}
	return out
}

func parseEntryLine(line string) (idx uint64, data string, ok bool) {
	rest := strings.TrimPrefix(line, "entry index=")
	idxStr, rest, found := strings.Cut(rest, " ")
	if !found {
		return 0, "", false
	}
	idx, err := strconv.ParseUint(idxStr, 10, 64)
	if err != nil {
		return 0, "", false
	}
	_, quoted, found := strings.Cut(rest, `data=`)
	if !found {
		return 0, "", false
	}
	// %q payload, optionally followed by a truncation marker after the closing quote.
	for end := len(quoted); end > 0; end-- {
		if s, err := strconv.Unquote(quoted[:end]); err == nil {
			return idx, s, true
		}
	}
	return idx, "", false
}

// digestOf strips the entry/first/last counters that legitimately agree and
// returns the comparable part of a summary line.
func (d WALDump) digestOf() string { return d.Summary }
