package linearizability

import (
	"testing"
	"time"
)

// WBS 4.6.1: the CompareAndSwap and Range models. Each case is a small
// hand-written history; "good" ones must check as linearizable and every
// known-bad one must be reported as a violation.

func put(r *Recorder, proc int, key, val string) { r.Invoke(proc, FPut, key, val).OK(false, "") }

func get(r *Recorder, proc int, key, val string) {
	r.Invoke(proc, FGet, key, "").OK(val != "", val)
}

func cas(r *Recorder, proc int, c CASCall, swapped bool, cur string) {
	r.InvokeCAS(proc, c).CASOK(swapped, cur != "", cur)
}

func scan(r *Recorder, proc int, start, end string, more bool, kvs ...string) {
	r.InvokeRange(proc, start, end).RangeOK(pairs(kvs...), more)
}

func pairs(kv ...string) []KVPair {
	var out []KVPair
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, KVPair{Key: kv[i], Value: kv[i+1]})
	}
	return out
}

func verdict(t *testing.T, build func(*Recorder)) Verdict {
	t.Helper()
	r := NewRecorderWithClock(fakeClock())
	build(r)
	return CheckRecorder(r, 5*time.Second).Verdict
}

func TestCASModelAcceptsGoodHistories(t *testing.T) {
	for name, build := range map[string]func(*Recorder){
		"create if absent, then value and revision compares": func(r *Recorder) {
			cas(r, 1, CASCall{Key: "k", Cmp: CmpAbsent, Value: "a"}, true, "a")
			get(r, 1, "k", "a")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "b"}, true, "b")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpRev, Rev: 7, Expect: "b", Value: "c"}, true, "c")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpValue, Expect: "c", Del: true}, true, "")
			get(r, 1, "k", "")
		},
		"failed compare is an ok result carrying the current value": func(r *Recorder) {
			put(r, 1, "k", "a")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpAbsent, Value: "x"}, false, "a")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpValue, Expect: "zz", Value: "x"}, false, "a")
			r.Invoke(1, FDelete, "k", "").DeleteOK(true)
			cas(r, 1, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "x"}, false, "")
			r.Invoke(1, FDelete, "k", "").DeleteOK(false)
		},
		"concurrent compares on one value: exactly one wins": func(r *Recorder) {
			put(r, 1, "k", "a")
			c1 := r.InvokeCAS(1, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "b"})
			c2 := r.InvokeCAS(2, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "c"})
			c2.CASOK(false, true, "b") // c1 went first
			c1.CASOK(true, true, "b")
			get(r, 3, "k", "b")
		},
		"indeterminate cas that took effect": func(r *Recorder) {
			put(r, 1, "k", "a")
			r.InvokeCAS(2, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "b"}).Info()
			get(r, 3, "k", "b")
		},
		"indeterminate cas that never applied": func(r *Recorder) {
			put(r, 1, "k", "a")
			r.InvokeCAS(2, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "b"}).Info()
			get(r, 3, "k", "a")
			put(r, 3, "k", "c")
			get(r, 3, "k", "c")
		},
		"indeterminate cas applied late, after a later write": func(r *Recorder) {
			put(r, 1, "k", "a")
			r.InvokeCAS(2, CASCall{Key: "k", Cmp: CmpAbsent, Value: "b"}).Info()
			r.Invoke(3, FDelete, "k", "").DeleteOK(true)
			get(r, 3, "k", "b") // the CAS was applied after the delete
		},
	} {
		if v := verdict(t, build); v != Linearizable {
			t.Errorf("%s: verdict %v, want linearizable", name, v)
		}
	}
}

func TestCASModelRejectsKnownBadHistories(t *testing.T) {
	for name, build := range map[string]func(*Recorder){
		"swapped although the compare could not match": func(r *Recorder) {
			put(r, 1, "k", "a")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpAbsent, Value: "b"}, true, "b")
		},
		"not swapped although the compare matched": func(r *Recorder) {
			put(r, 1, "k", "a")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "b"}, false, "a")
		},
		"failed compare returns a stale current value": func(r *Recorder) {
			put(r, 1, "k", "a")
			put(r, 1, "k", "b")
			cas(r, 2, CASCall{Key: "k", Cmp: CmpValue, Expect: "zz", Value: "c"}, false, "a")
		},
		"failed compare claims absent while the key exists": func(r *Recorder) {
			put(r, 1, "k", "a")
			cas(r, 2, CASCall{Key: "k", Cmp: CmpValue, Expect: "zz", Value: "c"}, false, "")
		},
		"lost update: two compares on the same value both swap": func(r *Recorder) {
			put(r, 1, "k", "a")
			c1 := r.InvokeCAS(1, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "b"})
			c2 := r.InvokeCAS(2, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Value: "c"})
			c1.CASOK(true, true, "b")
			c2.CASOK(true, true, "c")
		},
		"revision compare swaps on a superseded revision": func(r *Recorder) {
			put(r, 1, "k", "a")
			put(r, 1, "k", "b")
			cas(r, 2, CASCall{Key: "k", Cmp: CmpRev, Rev: 3, Expect: "a", Value: "c"}, true, "c")
		},
		"swapped value is not visible to a later read": func(r *Recorder) {
			cas(r, 1, CASCall{Key: "k", Cmp: CmpAbsent, Value: "a"}, true, "a")
			get(r, 2, "k", "")
		},
		"swap reports the wrong new value": func(r *Recorder) {
			cas(r, 1, CASCall{Key: "k", Cmp: CmpAbsent, Value: "a"}, true, "other")
		},
		"cas delete leaves the key behind": func(r *Recorder) {
			put(r, 1, "k", "a")
			cas(r, 1, CASCall{Key: "k", Cmp: CmpValue, Expect: "a", Del: true}, true, "")
			get(r, 2, "k", "a")
		},
		"indeterminate cas observed although its compare never matched": func(r *Recorder) {
			put(r, 1, "k", "a")
			r.InvokeCAS(2, CASCall{Key: "k", Cmp: CmpValue, Expect: "zz", Value: "b"}).Info()
			get(r, 3, "k", "b")
		},
		"delete claims a key existed when it did not": func(r *Recorder) {
			r.Invoke(1, FDelete, "k", "").DeleteOK(true)
		},
	} {
		if v := verdict(t, build); v != Violation {
			t.Errorf("%s: verdict %v, want violation", name, v)
		}
	}
}

func TestRangeModelAcceptsGoodHistories(t *testing.T) {
	for name, build := range map[string]func(*Recorder){
		"whole span, then two pages": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k2", "c")
			scan(r, 1, "g/", "g0", false, "g/k0", "a", "g/k2", "c")
			// limit 1: the first page stops at g/k0 and claims nothing after it
			scan(r, 1, "g/", "g0", true, "g/k0", "a")
			scan(r, 1, "g/k0\x00", "g0", false, "g/k2", "c")
		},
		"start/end span ignores keys outside it": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k1", "b")
			put(r, 1, "g/k2", "c")
			scan(r, 1, "g/k1", "g/k2", false, "g/k1", "b")
		},
		"empty page over an empty group": func(r *Recorder) {
			scan(r, 1, "g/", "g0", false)
			put(r, 1, "h/k0", "x")
			scan(r, 1, "g/", "g0", false)
		},
		"page concurrent with writes sees one consistent cut": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			p := r.InvokeRange(2, "g/", "g0")
			put(r, 1, "g/k1", "b")
			r.Invoke(1, FDelete, "g/k0", "").OK(false, "")
			p.RangeOK(pairs("g/k0", "a", "g/k1", "b"), false) // after the put, before the delete
		},
		"later pages may see newer state (not one snapshot)": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k1", "b")
			scan(r, 2, "g/", "g0", true, "g/k0", "a")
			put(r, 1, "g/k0", "a2")
			put(r, 1, "g/k1", "b2")
			scan(r, 2, "g/k0\x00", "g0", false, "g/k1", "b2")
		},
		"indeterminate write seen by a page": func(r *Recorder) {
			r.Invoke(1, FPut, "g/k0", "a").Info()
			scan(r, 2, "g/", "g0", false, "g/k0", "a")
			get(r, 2, "g/k0", "a")
		},
	} {
		if v := verdict(t, build); v != Linearizable {
			t.Errorf("%s: verdict %v, want linearizable", name, v)
		}
	}
}

func TestRangeModelRejectsKnownBadHistories(t *testing.T) {
	for name, build := range map[string]func(*Recorder){
		"page misses an acknowledged key": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k1", "b")
			scan(r, 2, "g/", "g0", false, "g/k1", "b")
		},
		"page returns a deleted key": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			r.Invoke(1, FDelete, "g/k0", "").OK(false, "")
			scan(r, 2, "g/", "g0", false, "g/k0", "a")
		},
		"page returns a value never written": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			scan(r, 2, "g/", "g0", false, "g/k0", "ghost")
		},
		"page returns a key nobody wrote": func(r *Recorder) {
			scan(r, 2, "g/", "g0", false, "g/zz", "ghost")
		},
		"stale page after a newer write": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k0", "b")
			scan(r, 2, "g/", "g0", false, "g/k0", "a")
		},
		"page returns a key outside its span": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k1", "b")
			scan(r, 2, "g/k1", "g0", false, "g/k0", "a", "g/k1", "b")
		},
		"limited page skips a key before its last key": func(r *Recorder) {
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k1", "b")
			scan(r, 2, "g/", "g0", true, "g/k1", "b")
		},
		// Each key on its own could be explained (k0 read before its put, k1
		// after its put), but no single point of time has k1=b and k0
		// absent: put k0 finished before put k1 started. Only the
		// multi-key page model catches this.
		"page is not one atomic cut": func(r *Recorder) {
			p := r.InvokeRange(2, "g/", "g0")
			put(r, 1, "g/k0", "a")
			put(r, 1, "g/k1", "b")
			p.RangeOK(pairs("g/k1", "b"), false)
		},
	} {
		if v := verdict(t, build); v != Violation {
			t.Errorf("%s: verdict %v, want violation", name, v)
		}
	}
}

// Ranges join the keys they cover into one partition; keys never covered by a
// range stay on their own, and groups that no range spans stay apart.
func TestPartitionByRangeSpans(t *testing.T) {
	r := NewRecorderWithClock(fakeClock())
	put(r, 1, "a/k0", "1")
	put(r, 1, "a/k1", "2")
	put(r, 1, "b/k0", "3")
	put(r, 1, "c/k0", "4")
	scan(r, 1, "a/", "a0", false, "a/k0", "1", "a/k1", "2")
	scan(r, 1, "z/", "z0", false) // covers no known key
	ops, _ := r.Operations()
	parts := partition(ops)
	sizes := map[string]int{}
	for _, p := range parts {
		in := p[0].Input.(Input)
		sizes[in.Key] = len(p)
	}
	want := map[string]int{"a/k0": 3, "b/k0": 1, "c/k0": 1, "z/": 1}
	if len(parts) != len(want) {
		t.Fatalf("got %d partitions %v, want %v", len(parts), sizes, want)
	}
	for k, n := range want {
		if sizes[k] != n {
			t.Errorf("partition of %s has %d ops, want %d (all: %v)", k, sizes[k], n, sizes)
		}
	}
	// The model must still describe every op for the visualization.
	m := KVModel()
	for _, o := range ops {
		if m.DescribeOperation(o.Input, o.Output) == "" {
			t.Fatalf("no description for %+v", o.Input)
		}
	}
}
