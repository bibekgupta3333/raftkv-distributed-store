package linearizability

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Function names recorded in a history.
const (
	FPut    = "put"
	FGet    = "get"
	FDelete = "delete"
	FCAS    = "cas"
	FRange  = "range"
)

// CAS compare kinds recorded in a history (api-design 3.1).
const (
	CmpValue  = "value"  // key exists with exactly Expect
	CmpAbsent = "absent" // key does not exist
	// CmpRev: key exists with mod_revision Rev. The recorder also stores, in
	// Expect, the value that was read together with that revision; the model
	// checks the compare as "current value == Expect" (see [ConvertEvents]).
	CmpRev = "rev"
)

// KVPair is one key/value a Range page returned.
type KVPair struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// EventType is the kind of history event.
type EventType string

// Event types (testing.md 3.1).
const (
	Invoke EventType = "invoke"
	OK     EventType = "ok"   // the operation happened and returned a result
	Fail   EventType = "fail" // definitely did NOT happen
	Info   EventType = "info" // unknown: may or may not have happened
)

// Event is one line of the JSON-lines history.
//
// Field use by function (testing.md 3.1):
//
//	put     invoke: key, value
//	get     ok: found, value
//	delete  ok: deleted (whether the key existed; optional)
//	cas     invoke: key, cmp, expect, rev, value (new value) or del
//	        ok: swapped, found, value (the key after a swap, or the current
//	        state that made the compare fail)
//	range   invoke: key = first key of the scanned span, end = exclusive end
//	        ("" = no end); ok: kvs, more
type Event struct {
	Proc  int       `json:"proc"`
	Op    uint64    `json:"op"`
	Type  EventType `json:"type"`
	F     string    `json:"f"`
	Key   string    `json:"key"`
	Value string    `json:"value,omitempty"` // put/cas: value written; get/cas result: value read
	Found *bool     `json:"found,omitempty"` // get/cas result: whether the key existed
	T     int64     `json:"t"`               // ns since the recorder started (monotonic)

	Cmp     string   `json:"cmp,omitempty"`     // cas: CmpValue, CmpAbsent or CmpRev
	Expect  string   `json:"expect,omitempty"`  // cas: expected value
	Rev     uint64   `json:"rev,omitempty"`     // cas with CmpRev: expected mod_revision
	Del     bool     `json:"del,omitempty"`     // cas: the action deletes the key
	Swapped *bool    `json:"swapped,omitempty"` // cas result
	Deleted *bool    `json:"deleted,omitempty"` // delete result
	End     string   `json:"end,omitempty"`     // range: exclusive end of the span
	KVs     []KVPair `json:"kvs,omitempty"`     // range result
	More    bool     `json:"more,omitempty"`    // range result: the page stopped at its limit
}

// Recorder collects a history from many concurrent clients sharing one clock.
// It is safe for concurrent use.
type Recorder struct {
	start time.Time
	clock func() int64

	mu     sync.Mutex
	events []Event
	nextOp uint64
}

// NewRecorder returns a Recorder timed by the monotonic wall clock.
func NewRecorder() *Recorder {
	start := time.Now()
	return &Recorder{start: start, clock: func() int64 { return int64(time.Since(start)) }}
}

// NewRecorderWithClock returns a Recorder with an injected clock (tests).
func NewRecorderWithClock(clock func() int64) *Recorder { return &Recorder{clock: clock} }

// Pending is an invoked, not yet completed operation.
type Pending struct {
	r    *Recorder
	proc int
	op   uint64
	f    string
	key  string
	done bool
}

// Invoke records the start of a put, get or delete by proc. A proc runs one
// operation at a time; after an Info outcome the caller must continue under a
// new proc (testing.md 3.2).
func (r *Recorder) Invoke(proc int, f, key, value string) *Pending {
	return r.invoke(Event{Proc: proc, F: f, Key: key, Value: value})
}

// CASCall describes a CompareAndSwap for [Recorder.InvokeCAS].
type CASCall struct {
	Key    string
	Cmp    string // CmpValue, CmpAbsent or CmpRev
	Expect string // CmpValue: expected value; CmpRev: the value read with Rev
	Rev    uint64 // CmpRev only
	Value  string // new value (unless Del)
	Del    bool   // the action deletes the key
}

// InvokeCAS records the start of a CompareAndSwap.
func (r *Recorder) InvokeCAS(proc int, c CASCall) *Pending {
	return r.invoke(Event{Proc: proc, F: FCAS, Key: c.Key, Cmp: c.Cmp, Expect: c.Expect, Rev: c.Rev, Value: c.Value, Del: c.Del})
}

// InvokeRange records the start of one Range page over [start, end) (end ""
// = no end). For a continuation page, start is the first key after the last
// key of the previous page.
func (r *Recorder) InvokeRange(proc int, start, end string) *Pending {
	return r.invoke(Event{Proc: proc, F: FRange, Key: start, End: end})
}

func (r *Recorder) invoke(e Event) *Pending {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextOp++
	e.Op, e.Type, e.T = r.nextOp, Invoke, r.clock()
	r.events = append(r.events, e)
	return &Pending{r: r, proc: e.Proc, op: e.Op, f: e.F, key: e.Key}
}

func (p *Pending) finish(e Event) {
	if p.done {
		panic("linearizability: operation completed twice")
	}
	p.done = true
	r := p.r
	r.mu.Lock()
	defer r.mu.Unlock()
	e.Proc, e.Op, e.F, e.Key, e.T = p.proc, p.op, p.f, p.key, r.clock()
	r.events = append(r.events, e)
}

// OK records a successful completion. For a get, found and value are what it
// read; for put and delete they are ignored.
func (p *Pending) OK(found bool, value string) {
	p.finish(Event{Type: OK, Value: value, Found: &found})
}

// DeleteOK records a successful delete that reports whether the key existed.
func (p *Pending) DeleteOK(deleted bool) { p.finish(Event{Type: OK, Deleted: &deleted}) }

// CASOK records a CompareAndSwap result. A failed compare is a normal result
// (swapped = false) carrying the current state; after a swap, found and value
// are the key's new state.
func (p *Pending) CASOK(swapped, found bool, value string) {
	p.finish(Event{Type: OK, Swapped: &swapped, Found: &found, Value: value})
}

// RangeOK records a Range page: the pairs it returned, in order, and whether
// the page stopped at its limit with more keys possibly left.
func (p *Pending) RangeOK(kvs []KVPair, more bool) {
	p.finish(Event{Type: OK, KVs: append([]KVPair(nil), kvs...), More: more})
}

// Fail records that the operation provably did not happen (for example it was
// rejected before being proposed).
func (p *Pending) Fail() { p.finish(Event{Type: Fail}) }

// Info records that the outcome is unknown (timeout, connection reset, leader
// lost after proposing).
func (p *Pending) Info() { p.finish(Event{Type: Info}) }

// Events returns a copy of the recorded events in recording order.
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

// WriteJSONL writes the history, one event per line (testing.md 3.1).
func (r *Recorder) WriteJSONL(w io.Writer) error {
	enc := json.NewEncoder(w)
	for _, e := range r.Events() {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return nil
}

// ReadJSONL reads a history written by [Recorder.WriteJSONL].
func ReadJSONL(rd io.Reader) ([]Event, error) {
	dec := json.NewDecoder(rd)
	var out []Event
	for {
		var e Event
		if err := dec.Decode(&e); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
}

// Input is a model operation's input.
type Input struct {
	F     string
	Key   string // range: first key of the span
	Value string // put, cas: value written
	// cas only. A CmpRev compare arrives here as Cmp = CmpValue with the value
	// read together with that revision (see [ConvertEvents]).
	Cmp    string
	Expect string
	Del    bool
	End    string // range: exclusive end of the span the page covered ("" = no end)
}

// Output is a model operation's output.
type Output struct {
	Found   bool   // get, cas
	Value   string // get, cas
	Unknown bool   // indeterminate write: the result is not known

	Swapped bool     // cas
	Deleted *bool    // delete; nil = not recorded (not checked)
	KVs     []KVPair // range
}

// Stats summarises a history.
type Stats struct {
	OK, Fail, Info, Dropped int
}

// isRead reports whether f changes nothing (an indeterminate read is dropped).
func isRead(f string) bool { return f == FGet || f == FRange }

// ConvertEvents turns events into Porcupine operations and counts outcomes.
//
//   - ok: a normal operation over [invoke, result].
//   - fail: dropped (it did not happen).
//   - info write (put, delete, cas): Return = +infinity with an unknown
//     output, so it may be linearized anywhere after its call (including
//     last, which covers "never applied").
//   - info read (get, range) and reads with no result at all: dropped; reads
//     change nothing.
//   - a write with no result event at all (the run ended) is treated as info.
func ConvertEvents(events []Event) ([]porcupine.Operation, Stats) {
	calls := map[uint64]Event{}
	var order []uint64
	var ops []porcupine.Operation
	var st Stats
	done := map[uint64]bool{}
	for _, e := range events {
		if e.Type == Invoke {
			calls[e.Op] = e
			order = append(order, e.Op)
			continue
		}
		inv, ok := calls[e.Op]
		if !ok {
			continue
		}
		done[e.Op] = true
		switch e.Type {
		case OK:
			st.OK++
			ops = append(ops, op(inv, e, e.T, outputOf(inv, e)))
		case Fail:
			st.Fail++
		case Info:
			st.Info++
			if isRead(inv.F) {
				st.Dropped++
				continue
			}
			ops = append(ops, op(inv, e, math.MaxInt64, Output{Unknown: true}))
		}
	}
	for _, id := range order {
		if done[id] {
			continue
		}
		inv := calls[id]
		st.Info++
		if isRead(inv.F) {
			st.Dropped++
			continue
		}
		ops = append(ops, op(inv, Event{}, math.MaxInt64, Output{Unknown: true}))
	}
	return ops, st
}

func op(inv, res Event, ret int64, out Output) porcupine.Operation {
	in := Input{F: inv.F, Key: inv.Key, Value: inv.Value}
	switch inv.F {
	case FCAS:
		in.Cmp, in.Expect, in.Del = inv.Cmp, inv.Expect, inv.Del
		if in.Cmp == CmpRev {
			// "mod_revision == Rev" holds exactly when the key's current
			// version is the write the revision was read with. Every write
			// stores a unique value and a revision names one write, so the
			// compare is "current value == the value read with Rev".
			in.Cmp = CmpValue
		}
		if in.Del {
			in.Value = ""
		}
	case FRange:
		in.End = rangeEnd(inv, res)
	}
	return porcupine.Operation{ClientId: inv.Proc, Input: in, Call: inv.T, Output: out, Return: ret}
}

// rangeEnd is the exclusive end of the span a page actually covered (D-34): a
// page that stopped at its limit (more) covers keys up to and including its
// last key; otherwise it covers the whole requested span.
func rangeEnd(inv, res Event) string {
	if !res.More {
		return inv.End
	}
	if len(res.KVs) == 0 {
		return inv.Key // an empty span: the page claims nothing
	}
	return res.KVs[len(res.KVs)-1].Key + "\x00"
}

func outputOf(inv, res Event) Output {
	switch inv.F {
	case FGet:
		return Output{Found: res.Found != nil && *res.Found, Value: res.Value}
	case FDelete:
		return Output{Deleted: res.Deleted}
	case FCAS:
		return Output{Swapped: res.Swapped != nil && *res.Swapped, Found: res.Found != nil && *res.Found, Value: res.Value}
	case FRange:
		return Output{KVs: res.KVs}
	}
	return Output{}
}

// Breakdown counts the successful operations of a history by kind: put, get,
// delete, cas-swapped, cas-failed (a failed compare) and range-page. A run
// uses it to show that every kind of operation was actually checked.
func Breakdown(events []Event) map[string]int {
	f := map[uint64]string{}
	out := map[string]int{}
	for _, e := range events {
		switch {
		case e.Type == Invoke:
			f[e.Op] = e.F
		case e.Type != OK:
		case f[e.Op] == FCAS && e.Swapped != nil && *e.Swapped:
			out["cas-swapped"]++
		case f[e.Op] == FCAS:
			out["cas-failed"]++
		case f[e.Op] == FRange:
			out["range-page"]++
		default:
			out[f[e.Op]]++
		}
	}
	return out
}

// Operations converts the recorded history. See [ConvertEvents].
func (r *Recorder) Operations() ([]porcupine.Operation, Stats) { return ConvertEvents(r.Events()) }

func (s Stats) String() string {
	return fmt.Sprintf("ok=%d fail=%d info=%d (reads dropped=%d)", s.OK, s.Fail, s.Info, s.Dropped)
}
