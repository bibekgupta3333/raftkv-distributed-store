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
)

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
type Event struct {
	Proc  int       `json:"proc"`
	Op    uint64    `json:"op"`
	Type  EventType `json:"type"`
	F     string    `json:"f"`
	Key   string    `json:"key"`
	Value string    `json:"value,omitempty"` // put: value written; get: value read
	Found *bool     `json:"found,omitempty"` // get: whether the key existed
	T     int64     `json:"t"`               // ns since the recorder started (monotonic)
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

// Invoke records the start of an operation by proc. A proc runs one operation
// at a time; after an Info outcome the caller must continue under a new proc
// (testing.md 3.2).
func (r *Recorder) Invoke(proc int, f, key, value string) *Pending {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextOp++
	p := &Pending{r: r, proc: proc, op: r.nextOp, f: f, key: key}
	r.events = append(r.events, Event{Proc: proc, Op: p.op, Type: Invoke, F: f, Key: key, Value: value, T: r.clock()})
	return p
}

func (p *Pending) finish(t EventType, value string, found *bool) {
	if p.done {
		panic("linearizability: operation completed twice")
	}
	p.done = true
	r := p.r
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, Event{Proc: p.proc, Op: p.op, Type: t, F: p.f, Key: p.key, Value: value, Found: found, T: r.clock()})
}

// OK records a successful completion. For a get, found and value are what it read.
func (p *Pending) OK(found bool, value string) { p.finish(OK, value, &found) }

// Fail records that the operation provably did not happen (for example it was
// rejected before being proposed).
func (p *Pending) Fail() { p.finish(Fail, "", nil) }

// Info records that the outcome is unknown (timeout, connection reset, leader
// lost after proposing).
func (p *Pending) Info() { p.finish(Info, "", nil) }

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
	Key   string
	Value string // put only
}

// Output is a model operation's output.
type Output struct {
	Found   bool   // get only
	Value   string // get only
	Unknown bool   // indeterminate write: the result is not known
}

// Stats summarises a history.
type Stats struct {
	OK, Fail, Info, Dropped int
}

// ConvertEvents turns events into Porcupine operations and counts outcomes.
//
//   - ok: a normal operation over [invoke, result].
//   - fail: dropped (it did not happen).
//   - info write: Return = +infinity with an unknown output, so it may be
//     linearized anywhere after its call (including last, which covers "never
//     applied").
//   - info read and reads with no result at all: dropped; reads change nothing.
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
			ops = append(ops, op(inv, e.T, outputOf(inv, e)))
		case Fail:
			st.Fail++
		case Info:
			st.Info++
			if inv.F == FGet {
				st.Dropped++
				continue
			}
			ops = append(ops, op(inv, math.MaxInt64, Output{Unknown: true}))
		}
	}
	for _, id := range order {
		if done[id] {
			continue
		}
		inv := calls[id]
		st.Info++
		if inv.F == FGet {
			st.Dropped++
			continue
		}
		ops = append(ops, op(inv, math.MaxInt64, Output{Unknown: true}))
	}
	return ops, st
}

func op(inv Event, ret int64, out Output) porcupine.Operation {
	return porcupine.Operation{
		ClientId: inv.Proc,
		Input:    Input{F: inv.F, Key: inv.Key, Value: inv.Value},
		Call:     inv.T,
		Output:   out,
		Return:   ret,
	}
}

func outputOf(inv, res Event) Output {
	if inv.F == FGet {
		return Output{Found: res.Found != nil && *res.Found, Value: res.Value}
	}
	return Output{}
}

// Operations converts the recorded history. See [ConvertEvents].
func (r *Recorder) Operations() ([]porcupine.Operation, Stats) { return ConvertEvents(r.Events()) }

func (s Stats) String() string {
	return fmt.Sprintf("ok=%d fail=%d info=%d (reads dropped=%d)", s.OK, s.Fail, s.Info, s.Dropped)
}
