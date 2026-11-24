package sim

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
)

// Flags (WBS 1.7.1). Examples:
//
//	go test ./test/sim/ -run Sim -seeds=500 -start=1 -faults=full
//	go test ./test/sim/ -run TestSimSeeds -seeds=1 -start=8472 -faults=partition -v
//	go test -race ./test/sim/ -run TestSimSeeds -seeds=10000 -timeout=0
//
// -tracedir is where the JSONL trace of a failing seed is written (default:
// the OS temp dir). Print one with: go run ./test/sim/cmd/simtrace <file>.
var (
	flagSeeds    = flag.Int("seeds", 25, "number of seeds to run")
	flagStart    = flag.Uint64("start", 1, "first seed")
	flagFaults   = flag.String("faults", "full", "fault profile: none, lossy, partition, crash, full, lease, snapshot, membership")
	flagTraceDir = flag.String("tracedir", "", "directory for failing-seed traces (default: temp dir)")
)

func mustProfile(t testing.TB, name string) Profile {
	t.Helper()
	p, err := LookupProfile(name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// reproCommand is what a failing seed prints (WBS 1.7.1).
func reproCommand(seed uint64, profile string) string {
	return fmt.Sprintf("go test ./test/sim/ -run 'TestSimSeeds$' -seeds=1 -start=%d -faults=%s -v   (or: make sim SEEDS=1 SEED=%d FAULTS=%s)",
		seed, profile, seed, profile)
}

// runSeeds runs seeds start..start+n-1 on all CPUs. Each seed is independent
// and deterministic, so parallelism does not affect results. Results come
// back in seed order.
func runSeeds(start uint64, n int, p Profile) []Result {
	out := make([]Result, n)
	var wg sync.WaitGroup
	next := make(chan int)
	for range min(runtime.GOMAXPROCS(0), max(n, 1)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				out[i] = Run(start+uint64(i), RunOpts{Profile: p})
			}
		}()
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
	return out
}

// reportFailure re-runs the failing seed with recording on (same seed, same
// trace), dumps the JSONL trace and logs the repro command (WBS 1.7.2).
func reportFailure(t *testing.T, r Result, opts RunOpts) {
	t.Helper()
	opts.Record = true
	again := Run(r.Seed, opts)
	if again.TraceHash != r.TraceHash {
		t.Errorf("seed %d is NOT deterministic: trace hash %016x then %016x (simulator bug)", r.Seed, r.TraceHash, again.TraceHash)
	}
	dir := *flagTraceDir
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Logf("could not create %s: %v", dir, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("raftkv-sim-seed-%d-%s.jsonl", r.Seed, r.Profile))
	if f, err := os.Create(path); err != nil {
		t.Logf("could not write trace: %v", err)
	} else {
		if err := again.WriteTrace(f); err != nil {
			t.Logf("could not write trace: %v", err)
		}
		_ = f.Close()
	}
	kind := "SAFETY"
	if !r.Violation.IsSafety() {
		kind = "LIVENESS"
	}
	t.Errorf("%s failure: %s\n  repro: %s\n  trace: %s (pretty: go run ./test/sim/cmd/simtrace -tail 200 %s)\n  add to test/sim/regressions/seeds.txt once fixed: %d %s",
		kind, r, reproCommand(r.Seed, r.Profile), path, path, r.Seed, r.Profile)
}

func TestSimSeeds(t *testing.T) {
	p := mustProfile(t, *flagFaults)
	results := runSeeds(*flagStart, *flagSeeds, p)
	var failed, events int
	var maxLeader, maxCommit, sumLeader int64
	for _, r := range results {
		events += int(r.Stats.Events)
		if testing.Verbose() {
			t.Log(r)
		}
		if r.OK() {
			maxLeader = max(maxLeader, r.Stats.LeaderAfterMs)
			maxCommit = max(maxCommit, r.Stats.CommitAfterMs)
			sumLeader += r.Stats.LeaderAfterMs
			continue
		}
		failed++
		if failed <= 3 {
			reportFailure(t, r, RunOpts{Profile: p})
		} else {
			t.Errorf("seed %d failed: %v (repro: %s)", r.Seed, r.Violation, reproCommand(r.Seed, r.Profile))
		}
	}
	t.Logf("profile=%s seeds=%d..%d failures=%d events=%d; after heal: leader worst %dms avg %dms, commit worst %dms (limit %dms)",
		p.Name, *flagStart, *flagStart+uint64(*flagSeeds)-1, failed, events,
		maxLeader, sumLeader/int64(max(len(results)-failed, 1)), maxCommit, livenessMs)
}

// TestSimDeterminism: one seed run twice gives the same trace hash (WBS
// 1.5.1); different seeds give different traces.
func TestSimDeterminism(t *testing.T) {
	p := mustProfile(t, "full")
	var hashes []uint64
	for _, seed := range []uint64{1, 2, 3, 99} {
		a := Run(seed, RunOpts{Profile: p})
		b := Run(seed, RunOpts{Profile: p, Record: true})
		if a.TraceHash != b.TraceHash || a.Stats.Events != b.Stats.Events {
			t.Fatalf("seed %d: hash %016x (%d events) vs %016x (%d events)", seed, a.TraceHash, a.Stats.Events, b.TraceHash, b.Stats.Events)
		}
		if a.Stats.Events < 1000 {
			t.Errorf("seed %d: only %d events; the run is suspiciously short", seed, a.Stats.Events)
		}
		hashes = append(hashes, a.TraceHash)
	}
	slices.Sort(hashes)
	if len(slices.Compact(hashes)) != 4 {
		t.Errorf("different seeds produced equal trace hashes: %x", hashes)
	}
}

// TestSimProfiles runs a few seeds of every profile, so a profile that
// breaks is caught even when -faults selects another one.
func TestSimProfiles(t *testing.T) {
	for _, name := range ProfileNames() {
		t.Run(name, func(t *testing.T) {
			p := mustProfile(t, name)
			for _, r := range runSeeds(1000, 6, p) {
				if !r.OK() {
					reportFailure(t, r, RunOpts{Profile: p})
				}
			}
		})
	}
}

// TestSimRegressions replays every seed in test/sim/regressions/ (WBS 1.7.3).
func TestSimRegressions(t *testing.T) {
	seeds, err := LoadRegressions("regressions")
	if err != nil {
		t.Fatal(err)
	}
	if len(seeds) == 0 {
		t.Fatal("test/sim/regressions/ has no seeds; at least the sanity seed must exist")
	}
	for _, rs := range seeds {
		p := mustProfile(t, rs.Profile)
		r := Run(rs.Seed, RunOpts{Profile: p})
		if !r.OK() {
			t.Errorf("regression %s (%s): %s", rs.Source, rs.Comment, r)
			reportFailure(t, r, RunOpts{Profile: p})
		}
	}
}
