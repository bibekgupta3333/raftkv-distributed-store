package linearizability

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// fakeClock hands out 10, 20, 30... so histories are easy to write by hand.
func fakeClock() func() int64 {
	var t int64
	return func() int64 { t += 10; return t }
}

// WBS 3.4.1: invoke/result events are recorded with times, client, op, result,
// and "unknown" on timeout; the JSONL form round-trips.
func TestRecorderEventsAndJSONL(t *testing.T) {
	r := NewRecorderWithClock(fakeClock())
	p1 := r.Invoke(1, FPut, "k", "v1")
	p2 := r.Invoke(2, FGet, "k", "")
	p1.OK(false, "")
	p2.OK(true, "v1")
	p3 := r.Invoke(3, FPut, "k", "v2")
	p3.Info()
	p4 := r.Invoke(4, FDelete, "k", "")
	p4.Fail()

	ev := r.Events()
	if len(ev) != 8 || ev[0].Type != Invoke || ev[0].T != 10 || ev[2].Type != OK || ev[2].Op != ev[0].Op {
		t.Fatalf("events = %+v", ev)
	}
	if ev[3].Found == nil || !*ev[3].Found || ev[3].Value != "v1" {
		t.Fatalf("get result not recorded: %+v", ev[3])
	}
	for i := 1; i < len(ev); i++ {
		if ev[i].T <= ev[i-1].T {
			t.Fatal("clock must be monotonic across events")
		}
	}

	var buf bytes.Buffer
	if err := r.WriteJSONL(&buf); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(buf.String(), "\n"); lines != 8 || !strings.Contains(buf.String(), `"type":"invoke"`) {
		t.Fatalf("jsonl:\n%s", buf.String())
	}
	back, err := ReadJSONL(&buf)
	if err != nil || len(back) != 8 || back[3] != ev[3] && (back[3].Found == nil || *back[3].Found != *ev[3].Found) {
		t.Fatalf("round trip: %v %+v", err, back)
	}
}

func TestOperationClassification(t *testing.T) {
	r := NewRecorderWithClock(fakeClock())
	r.Invoke(1, FPut, "k", "a").OK(false, "")
	r.Invoke(2, FPut, "k", "b").Fail() // dropped
	r.Invoke(3, FPut, "k", "c").Info() // kept, open-ended
	r.Invoke(4, FGet, "k", "").Info()  // dropped
	r.Invoke(5, FDelete, "k", "")      // never completed: unknown write
	r.Invoke(6, FGet, "k", "")         // never completed read: dropped
	ops, st := r.Operations()
	if len(ops) != 3 || st.OK != 1 || st.Fail != 1 || st.Info != 4 || st.Dropped != 2 {
		t.Fatalf("ops=%d stats=%v", len(ops), st)
	}
	for _, o := range ops[1:] {
		if o.Return != math.MaxInt64 || !o.Output.(Output).Unknown {
			t.Fatalf("indeterminate write must be open-ended: %+v", o)
		}
	}
}

// A sequential history that respects the model passes.
func TestGoodHistoryIsLinearizable(t *testing.T) {
	r := NewRecorderWithClock(fakeClock())
	r.Invoke(1, FPut, "k", "a").OK(false, "")
	r.Invoke(1, FGet, "k", "").OK(true, "a")
	r.Invoke(1, FDelete, "k", "").OK(false, "")
	r.Invoke(1, FGet, "k", "").OK(false, "")
	rep := CheckRecorder(r, 5*time.Second)
	if !rep.OK() || rep.Verdict != Linearizable {
		t.Fatalf("verdict = %v", rep.Verdict)
	}
}

// Concurrent overlapping ops may take effect in either order.
func TestConcurrentOpsLinearizable(t *testing.T) {
	r := NewRecorderWithClock(fakeClock())
	a := r.Invoke(1, FPut, "k", "a")
	b := r.Invoke(2, FPut, "k", "b")
	g := r.Invoke(3, FGet, "k", "")
	a.OK(false, "")
	b.OK(false, "")
	g.OK(true, "a") // b may be ordered before a
	if rep := CheckRecorder(r, 5*time.Second); !rep.OK() {
		t.Fatalf("verdict = %v", rep.Verdict)
	}
}

// An indeterminate write that a later read observes is fine; so is one that
// never took effect (it can be ordered last).
func TestIndeterminateWrites(t *testing.T) {
	for name, readSees := range map[string]string{"took effect": "b", "never applied": "a"} {
		r := NewRecorderWithClock(fakeClock())
		r.Invoke(1, FPut, "k", "a").OK(false, "")
		r.Invoke(2, FPut, "k", "b").Info()
		r.Invoke(3, FGet, "k", "").OK(true, readSees)
		if rep := CheckRecorder(r, 5*time.Second); !rep.OK() {
			t.Fatalf("%s: verdict = %v", name, rep.Verdict)
		}
	}
}

// WBS 3.4.2: a known-bad history is detected and the visualization is produced.
func TestKnownBadHistoryIsDetected(t *testing.T) {
	// put(k,a) and put(k,b) both completed, in that order; a later read still
	// returns the OLD value a: a stale read, never linearizable.
	r := NewRecorderWithClock(fakeClock())
	r.Invoke(1, FPut, "k", "a").OK(false, "")
	r.Invoke(1, FPut, "k", "b").OK(false, "")
	r.Invoke(2, FGet, "k", "").OK(true, "a")
	rep := CheckRecorder(r, 5*time.Second)
	if rep.OK() || rep.Verdict != Violation {
		t.Fatalf("a stale read must be a violation, got %v", rep.Verdict)
	}
	var html bytes.Buffer
	if err := rep.WriteHTML(&html); err != nil || !strings.Contains(html.String(), "<html") {
		t.Fatalf("visualization: err=%v len=%d", err, html.Len())
	}
	path := t.ArtifactDir() + "/known-bad.html"
	if err := rep.SaveHTML(path); err != nil {
		t.Fatal(err)
	}

	// Other classic violations.
	for name, build := range map[string]func(*Recorder){
		"read of a value never written": func(r *Recorder) {
			r.Invoke(1, FPut, "k", "a").OK(false, "")
			r.Invoke(2, FGet, "k", "").OK(true, "ghost")
		},
		"lost acknowledged write": func(r *Recorder) {
			r.Invoke(1, FPut, "k", "a").OK(false, "")
			r.Invoke(2, FGet, "k", "").OK(false, "")
		},
		"deleted key reappears": func(r *Recorder) {
			r.Invoke(1, FPut, "k", "a").OK(false, "")
			r.Invoke(1, FDelete, "k", "").OK(false, "")
			r.Invoke(2, FGet, "k", "").OK(true, "a")
		},
	} {
		rr := NewRecorderWithClock(fakeClock())
		build(rr)
		if rep := CheckRecorder(rr, 5*time.Second); rep.Verdict != Violation {
			t.Errorf("%s: verdict = %v, want violation", name, rep.Verdict)
		}
	}
	// A violation on one key is found even when other keys are fine.
	r2 := NewRecorderWithClock(fakeClock())
	r2.Invoke(1, FPut, "good", "x").OK(false, "")
	r2.Invoke(1, FGet, "good", "").OK(true, "x")
	r2.Invoke(1, FPut, "bad", "a").OK(false, "")
	r2.Invoke(1, FGet, "bad", "").OK(true, "zzz")
	if rep := CheckRecorder(r2, 5*time.Second); rep.Verdict != Violation {
		t.Fatalf("per-key partition missed the bad key: %v", rep.Verdict)
	}
}

// Timeout = fail (testing.md 3.4): an inconclusive check is not a pass.
func TestCheckerTimeoutCountsAsFailure(t *testing.T) {
	if v := verdictOf(porcupine.Unknown); v != TimedOut || (Report{Verdict: v}).OK() {
		t.Fatalf("Unknown must map to TimedOut and not be OK, got %v", v)
	}
	if verdictOf(porcupine.Ok) != Linearizable || verdictOf(porcupine.Illegal) != Violation {
		t.Fatal("verdict mapping broken")
	}
	if TimedOut.String() == Linearizable.String() {
		t.Fatal("verdict strings must differ")
	}
}
