//go:build chaos

package chaos

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/test/linearizability"
)

const (
	namespace  = "raftkv"
	clientPod  = "raftkv-chaos-client"
	clientBin  = "/usr/local/bin/raftkv-chaos"
	numMembers = 5
)

// fault is one Chaos Mesh experiment and how long it is held before the
// driver deletes it. Static faults come from manifests/; leader faults are
// generated per step for the member that currently leads (docs/testing.md
// fault catalogue: "PodChaos targeting the current leader"). Pod kills are
// one-shot: kills is how many pods must be replaced (new UID) before the
// driver waits for recovery, which proves the kill fired. The network faults
// last until deleted (their manifests carry a longer duration as a safety
// net) and must report AllInjected before the hold starts.
type fault struct {
	name               string // manifest file name, or a leaderManifests key
	kind, object       string // Chaos Mesh kind and metadata.name
	kills              int
	minHold, extraHold time.Duration
}

var faults = []fault{
	{"pod-kill", "podchaos", "raftkv-pod-kill", 1, 5 * time.Second, 10 * time.Second},
	{"pod-kill-two", "podchaos", "raftkv-pod-kill-two", 2, 5 * time.Second, 10 * time.Second},
	{"pod-kill-leader", "podchaos", "raftkv-pod-kill-leader", 1, 5 * time.Second, 10 * time.Second},
	{"network-partition", "networkchaos", "raftkv-partition", 0, 10 * time.Second, 15 * time.Second},
	{"network-partition-leader", "networkchaos", "raftkv-partition-leader", 0, 10 * time.Second, 15 * time.Second},
	{"network-delay", "networkchaos", "raftkv-delay", 0, 15 * time.Second, 25 * time.Second},
	{"network-loss", "networkchaos", "raftkv-loss", 0, 15 * time.Second, 25 * time.Second},
}

// leaderManifests are the leader faults; %[1]s is the leader's pod name.
var leaderManifests = map[string]string{
	"pod-kill-leader": `apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata: {name: raftkv-pod-kill-leader, namespace: raftkv}
spec:
  action: pod-kill
  mode: all
  gracePeriod: 0
  selector:
    pods: {raftkv: [%[1]s]}
`,
	"network-partition-leader": `apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata: {name: raftkv-partition-leader, namespace: raftkv}
spec:
  action: partition
  mode: all
  selector:
    pods: {raftkv: [%[1]s]}
  direction: both
  target:
    mode: all
    selector:
      namespaces: [raftkv]
      labelSelectors: {app.kubernetes.io/name: raftkv}
  duration: 120s
`,
}

// WBS 7.4.4, D-47, docs/testing.md 2.6: the chaos driver. It runs against the
// kind cluster `make chaos-up` created and needs the raftkv-chaos:kind image
// loaded into it (`make chaos` does both steps).
//
//  1. Starts the in-cluster workload pod (test/chaos/client): concurrent
//     clients run the mixed Put/Get/Delete/CompareAndSwap/Range workload of
//     test/linearizability against raftkv-N.raftkv-peer:7000 and record the
//     history, moving to fresh keys every minute.
//  2. Runs a seeded schedule of Chaos Mesh experiments for CHAOS_DURATION:
//     the five manifests plus a pod kill and a partition aimed at the current
//     leader. Each step applies one, checks it took effect (AllInjected, or
//     the killed pods came back under new UIDs), holds it, deletes it, waits
//     until all five members are Ready again, then leaves a quiet gap. Every
//     cycle runs each fault once, in a seeded order.
//  3. Stops the workload, fetches the history and checks it with Porcupine:
//     any violation or a checker timeout fails the run.
//
// Artifacts (CHAOS_OUT, default out/chaos): seed.txt, fault-log.txt,
// history.jsonl, summary.txt, pod logs (current and previous container),
// events and pod listings, and on failure the Porcupine visualization.
//
// Environment: CHAOS_DURATION (default 10m), CHAOS_SEED (default: from the
// clock, logged), CHAOS_KUBE_CONTEXT (kind-raftkv), CHAOS_IMAGE
// (raftkv-chaos:kind), CHAOS_OUT.
func TestChaos(t *testing.T) {
	dur := envDuration(t, "CHAOS_DURATION", 10*time.Minute)
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("CHAOS_SEED: %v", err)
		}
		seed = n
	}
	k := &kube{t: t, context: envOr("CHAOS_KUBE_CONTEXT", "kind-raftkv")}
	out := envOr("CHAOS_OUT", filepath.Join(repoRoot(t), "out", "chaos"))
	if err := os.RemoveAll(out); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(out, "seed.txt"), fmt.Sprintf("CHAOS_SEED=%d CHAOS_DURATION=%v\n", seed, dur))
	t.Logf("CHAOS_SEED=%d CHAOS_DURATION=%v artifacts in %s", seed, dur, out)
	flog := newFaultLog(t, filepath.Join(out, "fault-log.txt"))

	// Leftovers from an earlier run, then a healthy cluster to start from.
	k.clearFaults()
	k.must("delete", "pod", clientPod, "--ignore-not-found", "--wait=true", "--grace-period=0", "--force")
	k.waitMembersReady(5 * time.Minute)

	// Artifacts are collected whatever happens; Cleanups run last-in first-out,
	// so faults are cleared before the logs are taken.
	t.Cleanup(func() { k.collect(out) })
	t.Cleanup(k.clearFaults)

	k.startClient(envOr("CHAOS_IMAGE", "raftkv-chaos:kind"), seed)
	flog.Logf("workload running")

	rng := rand.New(rand.NewPCG(seed, 0x43484153)) // "CHAS"
	deadline := time.Now().Add(dur)
	steps := 0
	for cycle := 1; time.Now().Before(deadline); cycle++ {
		order := rng.Perm(len(faults))
		for _, i := range order {
			if !time.Now().Before(deadline) {
				break
			}
			f := faults[i]
			hold := f.minHold + time.Duration(rng.Int64N(int64(f.extraHold)+1))
			gap := 5*time.Second + time.Duration(rng.Int64N(int64(10*time.Second)+1))
			steps++
			lead := k.leader()
			manifest, target := k.manifest(f, lead)
			before := k.podUIDs()
			flog.Logf("cycle %d step %d: apply %s%s (leader %s), hold %v", cycle, steps, f.name, target, lead,
				hold.Round(time.Second))
			k.apply(manifest)
			if f.kills == 0 {
				k.must("wait", "--for=condition=AllInjected", f.kind+"/"+f.object, "--timeout=60s")
				flog.Logf("cycle %d step %d: %s injected", cycle, steps, f.name)
			}
			time.Sleep(hold)
			if f.kills > 0 {
				killed := k.waitReplaced(before, f.kills, time.Minute)
				flog.Logf("cycle %d step %d: killed %v", cycle, steps, killed)
			}
			k.must("delete", f.kind, f.object, "--ignore-not-found", "--wait=true")
			flog.Logf("cycle %d step %d: cleared %s", cycle, steps, f.name)
			took := k.waitMembersReady(5 * time.Minute)
			flog.Logf("cycle %d step %d: all %d members Ready after %v; quiet %v",
				cycle, steps, numMembers, took.Round(time.Second), gap.Round(time.Second))
			time.Sleep(gap)
		}
	}
	flog.Logf("schedule done: %d faults; stopping the workload", steps)

	k.exec(clientBin, "stop")
	k.waitFile("done", 2*time.Minute)
	history := filepath.Join(out, "history.jsonl")
	k.fetch("history.jsonl", history)
	k.fetch("summary.txt", filepath.Join(out, "summary.txt"))

	f, err := os.Open(history)
	if err != nil {
		t.Fatal(err)
	}
	events, err := linearizability.ReadJSONL(f)
	_ = f.Close()
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	start := time.Now()
	rep := linearizability.Check(events, 10*time.Minute)
	mix := linearizability.Breakdown(events)
	t.Logf("history: %d events, %d ops checked in %v, %s, verdict %s", len(events), rep.Ops,
		time.Since(start).Round(time.Millisecond), rep.Stats, rep.Verdict)
	t.Logf("checked operations by kind: %v", mix)
	if !rep.OK() {
		html := filepath.Join(out, "linearizability-failure.html")
		if err := rep.SaveHTML(html); err != nil {
			t.Errorf("save visualization: %v", err)
		}
		t.Fatalf("history is not linearizable (%s) with CHAOS_SEED=%d: see %s and %s", rep.Verdict, seed, html, history)
	}
	if rep.Stats.OK < 500 {
		t.Fatalf("only %d operations succeeded; the run proves little", rep.Stats.OK)
	}
	for _, kind := range []string{"put", "get", "delete", "cas-swapped", "cas-failed", "range-page"} {
		if mix[kind] == 0 {
			t.Errorf("no successful %s in the history", kind)
		}
	}
}

// --- kubectl ---------------------------------------------------------------

type kube struct {
	t       *testing.T
	context string
}

func (k *kube) cmd(args ...string) *exec.Cmd {
	return exec.Command("kubectl", append([]string{"--context", k.context, "-n", namespace}, args...)...)
}

// run runs kubectl and returns its combined output.
func (k *kube) run(args ...string) (string, error) {
	out, err := k.cmd(args...).CombinedOutput()
	msg := strings.TrimSpace(string(out))
	if err != nil {
		return msg, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, msg)
	}
	return msg, nil
}

func (k *kube) must(args ...string) string {
	k.t.Helper()
	out, err := k.run(args...)
	if err != nil {
		k.t.Fatal(err)
	}
	return out
}

func (k *kube) exec(args ...string) string {
	k.t.Helper()
	return k.must(append([]string{"exec", clientPod, "--"}, args...)...)
}

// manifest returns the YAML for f and, for a leader fault, a note naming
// its target.
func (k *kube) manifest(f fault, lead string) (yaml, target string) {
	k.t.Helper()
	if tmpl, ok := leaderManifests[f.name]; ok {
		if lead == "" {
			k.t.Fatalf("%s: no leader found", f.name)
		}
		return fmt.Sprintf(tmpl, lead), " on " + lead
	}
	b, err := os.ReadFile(filepath.Join("manifests", f.name+".yaml"))
	if err != nil {
		k.t.Fatal(err)
	}
	return string(b), ""
}

func (k *kube) apply(manifest string) {
	k.t.Helper()
	cmd := k.cmd("apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		k.t.Fatalf("kubectl apply: %v\n%s\n%s", err, out, manifest)
	}
}

// leader asks the workload pod which member leads, retrying through an
// election. "" if none was found within 30 s.
func (k *kube) leader() string {
	for start := time.Now(); time.Since(start) < 30*time.Second; time.Sleep(time.Second) {
		if out, err := k.run("exec", clientPod, "--", clientBin, "leader"); err == nil && out != "" {
			return out
		}
	}
	return ""
}

// podUIDs maps each member pod name to its UID.
func (k *kube) podUIDs() map[string]string {
	k.t.Helper()
	out := k.must("get", "pods", "-l", "app.kubernetes.io/name=raftkv",
		"-o", `jsonpath={range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}`)
	m := map[string]string{}
	for _, line := range strings.Fields(out) {
		if name, uid, ok := strings.Cut(line, "="); ok {
			m[name] = uid
		}
	}
	return m
}

// waitReplaced waits until at least n member pods were killed (gone, or back
// under a new UID): proof that a PodChaos actually fired.
func (k *kube) waitReplaced(before map[string]string, n int, timeout time.Duration) []string {
	k.t.Helper()
	var killed []string
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(time.Second) {
		now := k.podUIDs()
		killed = killed[:0]
		for name, uid := range before {
			if now[name] != uid {
				killed = append(killed, name)
			}
		}
		if len(killed) >= n {
			return killed
		}
	}
	k.t.Fatalf("pod kill: only %v of %d pods replaced after %v", killed, n, timeout)
	return nil
}

func (k *kube) clearFaults() {
	if out, err := k.run("delete", "podchaos,networkchaos", "--all", "--ignore-not-found", "--wait=true"); err != nil {
		k.t.Errorf("clear chaos experiments: %v\n%s", err, out)
	}
}

// waitMembersReady waits until the StatefulSet reports all members Ready
// (D-46: each knows the leader and has applied what it knows is committed)
// and returns how long that took.
func (k *kube) waitMembersReady(timeout time.Duration) time.Duration {
	k.t.Helper()
	start := time.Now()
	want := strconv.Itoa(numMembers)
	var last string
	for time.Since(start) < timeout {
		out, err := k.run("get", "statefulset", "raftkv", "-o", "jsonpath={.status.readyReplicas}")
		if err == nil && out == want {
			return time.Since(start)
		}
		last = out
		time.Sleep(2 * time.Second)
	}
	pods, _ := k.run("get", "pods", "-o", "wide")
	k.t.Fatalf("only %q of %d members Ready after %v\n%s", last, numMembers, timeout, pods)
	return 0
}

// startClient creates the workload pod and waits until its first write succeeded.
func (k *kube) startClient(image string, seed uint64) {
	k.t.Helper()
	eps := make([]string, numMembers)
	for i := range eps {
		eps[i] = fmt.Sprintf("raftkv-%d.raftkv-peer:7000", i)
	}
	manifest := fmt.Sprintf(clientManifest, clientPod, image, clientBin, strings.Join(eps, ","), seed)
	cmd := k.cmd("apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		k.t.Fatalf("create %s: %v\n%s", clientPod, err, out)
	}
	k.must("wait", "--for=condition=Ready", "pod/"+clientPod, "--timeout=120s")
	k.waitFile("started", 4*time.Minute)
}

// waitFile waits until the workload pod has created name in its work directory.
func (k *kube) waitFile(name string, timeout time.Duration) {
	k.t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		if _, err := k.run("exec", clientPod, "--", clientBin, "cat", name); err == nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
	logs, _ := k.run("logs", clientPod, "--tail=50")
	k.t.Fatalf("workload pod: no %s after %v\n%s", name, timeout, logs)
}

// fetch copies a file from the workload pod's work directory to path.
func (k *kube) fetch(name, path string) {
	k.t.Helper()
	f, err := os.Create(path)
	if err != nil {
		k.t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var stderr bytes.Buffer
	cmd := k.cmd("exec", clientPod, "--", clientBin, "cat", name)
	cmd.Stdout, cmd.Stderr = f, &stderr
	if err := cmd.Run(); err != nil {
		k.t.Fatalf("fetch %s: %v: %s", name, err, stderr.String())
	}
}

// collect saves pod logs (including the previous container of a killed
// member), events and pod listings. Best effort: it runs during cleanup.
func (k *kube) collect(out string) {
	save := func(name string, args ...string) {
		text, err := k.run(args...)
		if err != nil {
			text += "\n" + err.Error()
		}
		_ = os.WriteFile(filepath.Join(out, name), []byte(text+"\n"), 0o644)
	}
	save("pods.txt", "get", "pods", "-o", "wide")
	save("events.txt", "get", "events", "--sort-by=.lastTimestamp")
	save("client.log", "logs", clientPod)
	for i := range numMembers {
		pod := fmt.Sprintf("raftkv-%d", i)
		save(pod+".log", "logs", pod)
		save(pod+".previous.log", "logs", pod, "--previous")
	}
}

// clientManifest is the workload pod. Its labels do not match the Chaos Mesh
// selectors (app.kubernetes.io/name=raftkv), so it is never a fault target.
const clientManifest = `apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  labels: {app.kubernetes.io/name: raftkv-chaos-client, app.kubernetes.io/part-of: raftkv}
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: workload
      image: %[2]s
      imagePullPolicy: Never
      command: [%[3]s, run, --dir=/work, --endpoints=%[4]s, --seed=%[5]d]
      resources:
        requests: {cpu: 200m, memory: 128Mi}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: {drop: [ALL]}
      volumeMounts:
        - {name: work, mountPath: /work}
  volumes:
    - name: work
      emptyDir: {}
`

// --- helpers ---------------------------------------------------------------

type faultLog struct {
	t  *testing.T
	mu sync.Mutex
	f  *os.File
}

func newFaultLog(t *testing.T, path string) *faultLog {
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return &faultLog{t: t, f: f}
}

// Logf appends one timestamped line to the file and the test log.
func (l *faultLog) Logf(format string, args ...any) {
	line := fmt.Sprintf("%s %s", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), fmt.Sprintf(format, args...))
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = fmt.Fprintln(l.f, line)
	_ = l.f.Sync()
	l.t.Log("FAULT " + line)
}

func write(t *testing.T, path, s string) {
	if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
		t.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(t *testing.T, key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	return d
}

func repoRoot(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
