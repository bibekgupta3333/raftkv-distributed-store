// Command raftkv-chaos is the in-cluster half of the chaos driver (WBS 7.4.4,
// D-47). It runs as a pod next to the RaftKV StatefulSet, so the clients reach
// every member by its stable DNS name (raftkv-N.raftkv-peer:7000) and no
// port-forward sits between the workload and the cluster. Chaos Mesh only
// selects pods labelled app.kubernetes.io/name=raftkv, so this pod is never a
// fault target.
//
// The host side (test/chaos, build tag chaos) drives it with kubectl exec,
// since the image is FROM scratch and has no shell:
//
//	raftkv-chaos run  --endpoints=... --seed=N   start the workload, record the history
//	raftkv-chaos stop                            ask a running workload to finish
//	raftkv-chaos cat  <file>                     print a file from the work directory
//	raftkv-chaos leader                          print the leader's pod name (from /metrics)
//
// run records the mixed Put/Get/Delete/CompareAndSwap/Range workload of
// test/linearizability. It moves to fresh keys every --epoch (Workload.NextEpoch),
// which bounds the checker's work on long runs. When the stop file appears (or
// --max elapses) it stops the workers, waits for every in-flight operation,
// writes history.jsonl and summary.txt, creates the done file and then idles
// until the pod is deleted, so the host can fetch the files with `cat`.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
	"github.com/bibekgupta3333/raftkv-distributed-store/test/linearizability"
)

// Files in the work directory (an emptyDir: the root filesystem is read-only).
const (
	startedFile = "started"       // the first write succeeded and the workload runs
	stopFile    = "stop"          // created by `stop`
	historyFile = "history.jsonl" // the recorded history (testing.md 3.1)
	summaryFile = "summary.txt"   // op counts by outcome and kind
	doneFile    = "done"          // history.jsonl and summary.txt are complete
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "stop":
		err = stop(os.Args[2:])
	case "cat":
		err = cat(os.Args[2:])
	case "leader":
		err = leader(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "raftkv-chaos:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: raftkv-chaos run|stop|cat|leader [flags]")
	os.Exit(2)
}

func workDir(fs *flag.FlagSet) *string {
	return fs.String("dir", "/work", "work directory (control and result files)")
}

func stop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	dir := workDir(fs)
	_ = fs.Parse(args)
	return os.WriteFile(filepath.Join(*dir, stopFile), nil, 0o644)
}

func cat(args []string) error {
	fs := flag.NewFlagSet("cat", flag.ExitOnError)
	dir := workDir(fs)
	_ = fs.Parse(args)
	if fs.NArg() != 1 || strings.ContainsAny(fs.Arg(0), `/\`) {
		return errors.New("cat takes one file name from the work directory")
	}
	f, err := os.Open(filepath.Join(*dir, fs.Arg(0)))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(os.Stdout, f)
	return err
}

// leader prints the pod name of the member that reports itself leader
// (raftkv_raft_state 3) with the highest term on its metrics port. It fails if
// no member does, e.g. during an election.
func leader(args []string) error {
	fs := flag.NewFlagSet("leader", flag.ExitOnError)
	pods := fs.String("pods", "raftkv-0,raftkv-1,raftkv-2,raftkv-3,raftkv-4", "member pod names")
	service := fs.String("service", "raftkv-peer", "headless Service of the members")
	port := fs.Int("metrics-port", 9100, "metrics port")
	_ = fs.Parse(args)
	hc := &http.Client{Timeout: 2 * time.Second}
	best, bestTerm := "", -1.0
	for _, pod := range strings.Split(*pods, ",") {
		m, err := scrape(hc, fmt.Sprintf("http://%s.%s:%d/metrics", pod, *service, *port))
		if err != nil {
			continue // a dead or partitioned member is not the leader we want
		}
		if m["raftkv_raft_state"] == stateLeader && m["raftkv_raft_term"] > bestTerm {
			best, bestTerm = pod, m["raftkv_raft_term"]
		}
	}
	if best == "" {
		return errors.New("no member reports itself leader")
	}
	fmt.Println(best)
	return nil
}

const stateLeader = 3 // raftkv_raft_state of a leader

// scrape reads the unlabelled gauges of a Prometheus text page.
func scrape(hc *http.Client, url string) (map[string]float64, error) {
	resp, err := hc.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", url, resp.Status)
	}
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 2 || strings.HasPrefix(f[0], "#") {
			continue
		}
		if v, err := strconv.ParseFloat(f[1], 64); err == nil {
			out[f[0]] = v
		}
	}
	return out, sc.Err()
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dir := workDir(fs)
	endpoints := fs.String("endpoints", "", "comma-separated member addresses")
	seed := fs.Uint64("seed", 1, "workload seed")
	workers := fs.Int("workers", 8, "concurrent clients")
	epoch := fs.Duration("epoch", time.Minute, "move to fresh keys this often")
	maxRun := fs.Duration("max", 3*time.Hour, "stop on its own after this long")
	_ = fs.Parse(args)
	eps := strings.Split(*endpoints, ",")
	if *endpoints == "" || len(eps) == 0 {
		return errors.New("--endpoints is required")
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	logf("seed=%d workers=%d epoch=%v endpoints=%v", *seed, *workers, *epoch, eps)

	var clients []*client.Client
	newClient := func(noSessions bool, endpoints ...string) (*client.Client, error) {
		cl, err := client.New(client.Config{Endpoints: endpoints, AttemptTimeout: 1500 * time.Millisecond,
			ConnectTimeout: 300 * time.Millisecond, DisableSessions: noSessions})
		if err == nil {
			clients = append(clients, cl)
		}
		return cl, err
	}
	defer func() {
		for _, cl := range clients {
			_ = cl.Close()
		}
	}()

	// Wait until the cluster serves writes from inside the cluster.
	warm, err := newClient(false, eps...)
	if err != nil {
		return err
	}
	for deadline := time.Now().Add(3 * time.Minute); ; {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := warm.Put(ctx, []byte("chaos-warmup"), []byte("1"))
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no successful write in 3m: %w", err)
		}
		time.Sleep(time.Second)
	}

	// Same client mix as the compose e2e (test/e2e TestLinearizableKVUnderFaults):
	// workers 1 and 5 write without a session, so indeterminate writes reach
	// the history as "info" (testing.md 3.2); even workers send their reads to
	// one fixed member, so followers serve linearizable reads too (D-05).
	rec := linearizability.NewRecorder()
	wl := linearizability.NewWorkload(rec, linearizability.WorkloadConfig{
		Workers: *workers, Groups: 3, Keys: 3, OpTime: 6 * time.Second, Seed: *seed})
	kvs := make([]linearizability.KV, *workers)
	for i := range kvs {
		cl, err := newClient(i%4 == 1, eps...)
		if err != nil {
			return err
		}
		kvs[i] = cl
		if i%2 == 0 {
			reads, err := newClient(false, eps[(i/2)%len(eps)])
			if err != nil {
				return err
			}
			kvs[i] = linearizability.PinnedReads{KV: cl, Reads: reads}
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wlDone := make(chan struct{})
	go func() {
		defer close(wlDone)
		wl.Run(runCtx, func(i int) linearizability.KV { return kvs[i] })
	}()
	if err := os.WriteFile(filepath.Join(*dir, startedFile), nil, 0o644); err != nil {
		return err
	}
	logf("workload started")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	limit := time.After(*maxRun)
	epochs := time.NewTicker(*epoch)
	defer epochs.Stop()
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	progress := time.NewTicker(30 * time.Second)
	defer progress.Stop()
loop:
	for {
		select {
		case <-epochs.C:
			logf("epoch %d (ops so far %d)", wl.NextEpoch(), wl.Ops())
		case <-progress.C:
			logf("ops %d, leader (as cached by the warm-up client) %q", wl.Ops(), warm.Leader())
		case <-poll.C:
			if _, err := os.Stat(filepath.Join(*dir, stopFile)); err == nil {
				logf("stop requested")
				break loop
			}
		case <-limit:
			logf("--max %v reached", *maxRun)
			break loop
		case s := <-sigs:
			return fmt.Errorf("stopped by %v before the history was written", s)
		}
	}
	cancel()
	<-wlDone // every started operation has finished or timed out (OpTime)

	events := rec.Events()
	if err := writeFile(filepath.Join(*dir, historyFile), rec.WriteJSONL); err != nil {
		return err
	}
	ops, stats := linearizability.ConvertEvents(events)
	summary := fmt.Sprintf("events=%d ops=%d %s\nkinds=%v\n", len(events), len(ops), stats,
		linearizability.Breakdown(events))
	if err := os.WriteFile(filepath.Join(*dir, summaryFile), []byte(summary), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*dir, doneFile), nil, 0o644); err != nil {
		return err
	}
	logf("history written: %s", strings.TrimSpace(summary))

	// Idle until the pod is deleted: the host fetches the files with `cat`.
	<-sigs
	return nil
}

// writeFile writes through a temporary file so a reader never sees half a file.
func writeFile(path string, write func(io.Writer) error) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func logf(format string, args ...any) {
	fmt.Printf("%s %s\n", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), fmt.Sprintf(format, args...))
}
