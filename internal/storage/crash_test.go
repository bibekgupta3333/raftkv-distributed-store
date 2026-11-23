package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// WBS 3.1.6 / 3.1.7: crash the in-memory file system at many points while
// applying with random flushes, reopen the crash image, and check (1) the
// store holds exactly a prefix of the log (state hash == reference at its
// appliedIndex, D-04) and (2) replaying the rest from the "WAL" (genRange)
// reaches the same final state as a run with no crash.
func TestCrashCloneRecoveryByReplay(t *testing.T) {
	const total = 400
	finalRef := referenceHash(t, total)
	rounds := 150
	if testing.Short() {
		rounds = 30
	}
	for round := range rounds {
		rng := rand.New(rand.NewPCG(uint64(round), 7))
		fs := vfs.NewCrashableMem()
		s := openMem(t, fs)
		a := NewApplier(s, nil)

		crashAt := uint64(1 + rng.IntN(total))
		var clone *vfs.MemFS
		for i := uint64(1); i <= total; i++ {
			applyAll(t, a, genRange(i, i), 1)
			if rng.IntN(25) == 0 {
				if err := s.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if i == crashAt {
				// Unsynced data may or may not have reached disk at the crash.
				clone = fs.CrashClone(vfs.CrashCloneCfg{
					UnsyncedDataPercent: []int{0, 50, 100}[rng.IntN(3)],
					RNG:                 rand.New(rand.NewPCG(uint64(round), 3)),
				})
				break
			}
		}
		_ = s.db.Close()

		r := openMem(t, clone)
		got := r.Applied()
		if got > crashAt {
			t.Fatalf("round %d: recovered applied %d beyond crash point %d", round, got, crashAt)
		}
		if _, h, err := r.StateHash(); err != nil || h != referenceHash(t, got) {
			t.Fatalf("round %d: state at applied=%d does not match the reference model (err %v)", round, got, err)
		}
		// Recovery: replay (applied, total] from the log.
		applyAll(t, NewApplier(r, nil), genRange(got+1, total), 5)
		if _, h, _ := r.StateHash(); h != finalRef {
			t.Fatalf("round %d: after replay from %d the state differs from an uncrashed run", round, got)
		}
		_ = r.Close()
	}
}

// Truncation-safety with unflushed memtables (WBS 3.1.7): if the WAL had been
// trimmed to the *applied* index instead of the durable one, a crash would
// leave a gap. The D-30 bound keeps every entry the crash image needs.
func TestTruncationSafetyWithUnflushedMemtables(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s := openMem(t, fs)
	a := NewApplier(s, nil)
	applyAll(t, a, genRange(1, 50), 5)
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	applyAll(t, a, genRange(51, 120), 5) // unflushed
	bound := TruncationBound(1000, s.DurableApplied(), 0)
	clone := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0, RNG: rand.New(rand.NewPCG(1, 1))})
	_ = s.db.Close()

	r := openMem(t, clone)
	defer func() { _ = r.Close() }()
	if r.Applied() != 50 {
		t.Fatalf("crash image applied = %d, want exactly the flushed 50", r.Applied())
	}
	// The WAL still holds (bound, ...]: replay needs entries 51..120, and the
	// bound (50) leaves them. A bound of 120 (applied) would have dropped them.
	if bound >= r.Applied()+1 {
		t.Fatalf("bound %d would drop entry %d needed for replay", bound, r.Applied()+1)
	}
	applyAll(t, NewApplier(r, nil), genRange(r.Applied()+1, 120), 5)
	if _, h, _ := r.StateHash(); h != referenceHash(t, 120) {
		t.Fatal("replay after the crash did not rebuild the state")
	}
}

// --- kill -9 suite (WBS 3.1.7) -------------------------------------------

const (
	envChildDir = "RAFTKV_CRASH_CHILD_DIR"
	envRounds   = "RAFTKV_CRASH_ROUNDS"
	childReady  = "RAFTKV_CRASH_CHILD_READY"
)

// TestCrashChild is not a test: it is the body of the subprocess that
// TestKillMinus9DuringApply starts and SIGKILLs. It applies the deterministic
// workload from wherever the store left off, flushing at random, until killed.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv(envChildDir)
	if dir == "" {
		t.Skip("subprocess helper")
	}
	s, err := Open(Options{Dir: dir})
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(3)
	}
	a := NewApplier(s, nil)
	fmt.Println(childReady) // the parent starts its kill timer from here, not from process start
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	for i := s.Applied() + 1; ; {
		n := uint64(1 + rng.IntN(8))
		if err := a.Apply(genRange(i, i+n-1)); err != nil {
			fmt.Fprintln(os.Stderr, "child apply:", err)
			os.Exit(4)
		}
		i += n
		if rng.IntN(20) == 0 {
			if err := s.Flush(); err != nil {
				fmt.Fprintln(os.Stderr, "child flush:", err)
				os.Exit(5)
			}
		}
	}
}

// 500 rounds (30 with -short): start a child that applies and flushes at
// random, SIGKILL it at a random moment, reopen the directory and require the
// state hash to equal the reference model at the recovered appliedIndex; the
// next child then continues by replay from there.
func TestKillMinus9DuringApply(t *testing.T) {
	rounds := 500
	if testing.Short() {
		rounds = 30
	}
	if v := os.Getenv(envRounds); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatal(err)
		}
		rounds = n
	}
	const shards = 4 // independent directories killed in parallel
	for shard := range shards {
		t.Run(fmt.Sprint("shard", shard), func(t *testing.T) {
			t.Parallel()
			killRounds(t, rounds/shards, uint64(shard))
		})
	}
}

// waitReady blocks until the child has opened the store and printed its marker.
func waitReady(t *testing.T, out io.Reader) {
	t.Helper()
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if sc.Text() == childReady {
			return
		}
	}
	t.Fatalf("child exited before becoming ready: %v", sc.Err())
}

func killRounds(t *testing.T, rounds int, seed uint64) {
	dir := t.TempDir()
	ref := openMem(t, vfs.NewMem()) // the reference model, advanced incrementally
	defer func() { _ = ref.Close() }()
	refApplier := NewApplier(ref, nil)
	rng := rand.New(rand.NewPCG(99, seed))

	var highest uint64
	for round := range rounds {
		ctx, cancel := context.WithCancel(context.Background())
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashChild$", "-test.v=false")
		cmd.Env = append(os.Environ(), envChildDir+"="+dir)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		waitReady(t, out)
		time.Sleep(time.Duration(10+rng.IntN(50)) * time.Millisecond)
		_ = cmd.Process.Kill() // SIGKILL: no deferred flush, no clean Close
		_, _ = io.Copy(io.Discard, out)
		_ = cmd.Wait()
		cancel()

		s, err := Open(Options{Dir: dir})
		if err != nil {
			t.Fatalf("round %d: reopen after kill -9: %v", round, err)
		}
		got := s.Applied()
		if got < highest {
			t.Fatalf("round %d: applied went backwards %d -> %d (INV-6)", round, highest, got)
		}
		highest = got
		for ref.Applied() < got {
			next := ref.Applied() + 1
			applyAll(t, refApplier, genRange(next, min(got, next+255)), 16)
		}
		_, want, _ := ref.StateHash()
		if _, have, err := s.StateHash(); err != nil || have != want {
			t.Fatalf("round %d: state hash mismatch at appliedIndex %d (err %v)", round, got, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if highest == 0 {
		t.Fatal("no round ever made progress durable; the test proves nothing")
	}
	t.Logf("%d kill -9 rounds, durable progress reached index %d, 0 mismatches", rounds, highest)
}
