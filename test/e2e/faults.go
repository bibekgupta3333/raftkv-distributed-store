//go:build e2e

package e2e

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- fault log (WBS 2.6.4) ------------------------------------------------

// FaultLog records every injected fault with a wall-clock timestamp, to the
// test log and to a file CI uploads as an artifact.
type FaultLog struct {
	t  testing.TB
	mu sync.Mutex
	f  *os.File
}

// NewFaultLog creates (truncating) the log file at path.
func NewFaultLog(t testing.TB, path string) *FaultLog {
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return &FaultLog{t: t, f: f}
}

// Logf appends one timestamped line.
func (l *FaultLog) Logf(format string, args ...any) {
	line := fmt.Sprintf("%s %s", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), fmt.Sprintf(format, args...))
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = fmt.Fprintln(l.f, line)
	_ = l.f.Sync()
	l.t.Log("FAULT " + line)
}

// --- fault driver (WBS 2.6.2, 2.6.3) --------------------------------------

// docker runs the docker CLI. On failure the error carries the CLI's output
// (e.g. "container ... is not running"), not just the exit status.
func (c *Cluster) docker(args ...string) (string, error) {
	out, err := exec.Command("docker", args...).CombinedOutput()
	msg := strings.TrimSpace(string(out))
	if err != nil {
		return msg, fmt.Errorf("docker %s: %w: %s", args[0], err, msg)
	}
	return msg, nil
}

// execRoot runs a shell command as root inside node id. Nodes run as a
// non-root user, but the container holds NET_ADMIN, which root may use.
func (c *Cluster) execRoot(id int, script string) error {
	if _, err := c.docker("exec", "-u", "0", ContainerName(id), "sh", "-c", script); err != nil {
		return fmt.Errorf("exec on node %d %q: %w", id, script, err)
	}
	return nil
}

func (c *Cluster) fault(action string, id int, detail string, err error) {
	c.t.Helper()
	if err != nil {
		c.Faults.Logf("%s node=%d %s FAILED: %v", action, id, detail, err)
		c.t.Fatalf("fault %s node %d: %v", action, id, err)
	}
	c.Faults.Logf("%s node=%d %s", action, id, detail)
}

// Kill sends SIGKILL (kill -9) to node id's container.
func (c *Cluster) Kill(id int) {
	_, err := c.docker("kill", "-s", "KILL", ContainerName(id))
	c.fault("kill-9", id, "", err)
}

// Restart starts a killed (or stopped) node's container again, keeping its volume.
func (c *Cluster) Restart(id int) {
	_, err := c.docker("start", ContainerName(id))
	c.fault("restart", id, "", err)
}

// Pause sends SIGSTOP to node id's container (the node freezes but its sockets stay open).
func (c *Cluster) Pause(id int) {
	_, err := c.docker("kill", "-s", "STOP", ContainerName(id))
	c.fault("sigstop", id, "", err)
}

// Resume sends SIGCONT to a paused node.
func (c *Cluster) Resume(id int) {
	_, err := c.docker("kill", "-s", "CONT", ContainerName(id))
	c.fault("sigcont", id, "", err)
}

func (c *Cluster) containerIP(id int) (string, error) {
	return c.docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", ContainerName(id))
}

const partChain = "RAFTKV_PART"

// Partition cuts the nodes in group off from every node outside it, in both
// directions, with iptables DROP rules inside the group's containers (the
// harness's own host-to-container traffic is unaffected). It replaces any
// earlier partition.
func (c *Cluster) Partition(group ...int) {
	c.t.Helper()
	c.Heal()
	in := map[int]bool{}
	for _, id := range group {
		in[id] = true
	}
	var others []string
	for id := 1; id <= NumNodes; id++ {
		if !in[id] {
			ip, err := c.containerIP(id)
			if err != nil || ip == "" {
				c.fault("partition", id, "ip lookup", fmt.Errorf("%v %q", err, ip))
			}
			others = append(others, ip)
		}
	}
	for _, id := range group {
		script := fmt.Sprintf("iptables -w -N %[1]s && iptables -w -I INPUT -j %[1]s && iptables -w -I OUTPUT -j %[1]s", partChain)
		for _, ip := range others {
			script += fmt.Sprintf(" && iptables -w -A %s -s %s -j DROP && iptables -w -A %[1]s -d %s -j DROP", partChain, ip, ip)
		}
		c.fault("partition", id, fmt.Sprintf("isolated-group=%v blocked=%v", group, others), c.execRoot(id, script))
	}
}

// Heal removes every partition rule from every running node.
func (c *Cluster) Heal() {
	c.t.Helper()
	for id := 1; id <= NumNodes; id++ {
		script := fmt.Sprintf("iptables -w -D INPUT -j %[1]s 2>/dev/null; iptables -w -D OUTPUT -j %[1]s 2>/dev/null; iptables -w -F %[1]s 2>/dev/null; iptables -w -X %[1]s 2>/dev/null; true", partChain)
		if err := c.execRoot(id, script); err != nil {
			// A killed node has nothing to heal.
			c.Faults.Logf("heal node=%d skipped: %v", id, err)
			continue
		}
	}
	c.Faults.Logf("heal all")
}

// NetemDelay adds delay (and optional jitter) to everything node id sends.
func (c *Cluster) NetemDelay(id int, delay, jitter time.Duration) {
	script := fmt.Sprintf("tc qdisc replace dev eth0 root netem delay %dms %dms", delay.Milliseconds(), jitter.Milliseconds())
	c.fault("netem-delay", id, fmt.Sprintf("delay=%v jitter=%v", delay, jitter), c.execRoot(id, script))
}

// NetemLoss makes node id drop pct percent of the packets it sends.
func (c *Cluster) NetemLoss(id int, pct int) {
	script := fmt.Sprintf("tc qdisc replace dev eth0 root netem loss %d%%", pct)
	c.fault("netem-loss", id, fmt.Sprintf("loss=%d%%", pct), c.execRoot(id, script))
}

// NetemClear removes any netem qdisc from node id.
func (c *Cluster) NetemClear(id int) {
	_ = c.execRoot(id, "tc qdisc del dev eth0 root 2>/dev/null; true")
	c.Faults.Logf("netem-clear node=%d", id)
}

// --- seeded schedule (WBS 2.6.4) ------------------------------------------

// FaultKind enumerates schedulable faults.
type FaultKind string

const (
	FaultKillFollower FaultKind = "kill-follower"
	FaultKillLeader   FaultKind = "kill-leader"
	FaultPause        FaultKind = "pause-follower"
	FaultPartition    FaultKind = "partition-minority"
	FaultDelay        FaultKind = "netem-delay"
	FaultLoss         FaultKind = "netem-loss"
)

// Step is one fault of a schedule. Targets are resolved when the step runs
// ("leader" and "follower k" depend on who is leader then); the choices that do
// not depend on cluster state are fixed by the seed.
type Step struct {
	Kind      FaultKind
	Pick      int           // seeded index into the candidate list
	Hold      time.Duration // how long the fault stays before it is undone
	Gap       time.Duration // quiet time after healing
	DelayMs   int
	LossPct   int
	Minority2 bool // partition: 2 nodes instead of 1
}

// Schedule returns n steps derived only from seed: the same seed always gives
// the same plan (real timing still varies between runs).
func Schedule(seed uint64, n int) []Step {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	kinds := []FaultKind{FaultKillFollower, FaultKillLeader, FaultPause, FaultPartition, FaultDelay, FaultLoss}
	steps := make([]Step, n)
	for i := range steps {
		steps[i] = Step{
			Kind:      kinds[r.IntN(len(kinds))],
			Pick:      r.IntN(NumNodes),
			Hold:      time.Duration(2+r.IntN(4)) * time.Second,
			Gap:       time.Duration(3+r.IntN(3)) * time.Second,
			DelayMs:   100 + 50*r.IntN(5),
			LossPct:   10 + 10*r.IntN(4),
			Minority2: r.IntN(2) == 0,
		}
	}
	return steps
}

// Run executes one step against the cluster and undoes it, leaving all nodes
// running and connected. At most 2 nodes are ever faulted at once, so a
// majority always exists.
func (c *Cluster) Run(s Step) {
	c.t.Helper()
	lead := c.WaitLeader(30 * time.Second)
	var followers []int
	for id := 1; id <= NumNodes; id++ {
		if id != lead {
			followers = append(followers, id)
		}
	}
	f := followers[s.Pick%len(followers)]
	c.Faults.Logf("step kind=%s leader=%d target=%d hold=%v", s.Kind, lead, f, s.Hold)
	switch s.Kind {
	case FaultKillFollower:
		c.Kill(f)
		time.Sleep(s.Hold)
		c.Restart(f)
	case FaultKillLeader:
		c.Kill(lead)
		time.Sleep(s.Hold)
		c.Restart(lead)
	case FaultPause:
		c.Pause(f)
		time.Sleep(s.Hold)
		c.Resume(f)
	case FaultPartition:
		group := []int{f}
		if s.Minority2 {
			group = []int{lead, f}
		}
		c.Partition(group...)
		time.Sleep(s.Hold)
		c.Heal()
	case FaultDelay:
		c.NetemDelay(f, time.Duration(s.DelayMs)*time.Millisecond, 20*time.Millisecond)
		time.Sleep(s.Hold)
		c.NetemClear(f)
	case FaultLoss:
		c.NetemLoss(f, s.LossPct)
		time.Sleep(s.Hold)
		c.NetemClear(f)
	}
	time.Sleep(s.Gap)
}
