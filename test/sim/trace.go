package sim

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Event is one line of the simulator trace. Fields not used by a Kind are
// zero. The trace hash covers every field, so any divergence between two runs
// of one seed shows up as a different hash (WBS 1.5.1).
type Event struct {
	At    int64  `json:"t"`
	N     uint64 `json:"n"`
	Kind  string `json:"k"`
	Node  uint64 `json:"node,omitempty"`
	Peer  uint64 `json:"peer,omitempty"`
	Msg   string `json:"msg,omitempty"`
	Term  uint64 `json:"term,omitempty"`
	Index uint64 `json:"index,omitempty"`
	Count uint64 `json:"count,omitempty"`
	Note  string `json:"note,omitempty"`
}

// String renders e as one human-readable line.
func (e Event) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%8dms #%-6d %-9s", e.At, e.N, e.Kind)
	if e.Node != 0 {
		fmt.Fprintf(&b, " n%d", e.Node)
	}
	if e.Peer != 0 {
		fmt.Fprintf(&b, " -> n%d", e.Peer)
	}
	if e.Msg != "" {
		fmt.Fprintf(&b, " %s", e.Msg)
	}
	if e.Term != 0 {
		fmt.Fprintf(&b, " term=%d", e.Term)
	}
	if e.Index != 0 {
		fmt.Fprintf(&b, " index=%d", e.Index)
	}
	if e.Count != 0 {
		fmt.Fprintf(&b, " count=%d", e.Count)
	}
	if e.Note != "" {
		fmt.Fprintf(&b, " %s", e.Note)
	}
	return b.String()
}

// Trace accumulates a running FNV-1a hash of all events and, when recording,
// the events themselves.
type Trace struct {
	hash   uint64
	n      uint64
	record bool
	events []Event
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

// NewTrace returns an empty trace. Recording keeps every event in memory for
// a JSONL dump; hashing is always on.
func NewTrace(record bool) *Trace { return &Trace{hash: fnvOffset, record: record} }

func (t *Trace) mix(v uint64) {
	for range 8 {
		t.hash ^= v & 0xff
		t.hash *= fnvPrime
		v >>= 8
	}
}

func (t *Trace) mixStr(s string) {
	for i := 0; i < len(s); i++ {
		t.hash ^= uint64(s[i])
		t.hash *= fnvPrime
	}
	t.mix(uint64(len(s)))
}

// Add appends e, assigning its sequence number.
func (t *Trace) Add(e Event) {
	e.N = t.n
	t.n++
	t.mix(uint64(e.At))
	t.mixStr(e.Kind)
	t.mix(e.Node)
	t.mix(e.Peer)
	t.mixStr(e.Msg)
	t.mix(e.Term)
	t.mix(e.Index)
	t.mix(e.Count)
	t.mixStr(e.Note)
	if t.record {
		t.events = append(t.events, e)
	}
}

// Hash is the running hash of all events.
func (t *Trace) Hash() uint64 { return t.hash }

// Len is the number of events added.
func (t *Trace) Len() uint64 { return t.n }

// Events returns the recorded events (nil unless recording).
func (t *Trace) Events() []Event { return t.events }

// WriteJSONL writes the recorded events, one JSON object per line.
func (t *Trace) WriteJSONL(w io.Writer) error {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	for _, e := range t.events {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadJSONL parses a trace written by [Trace.WriteJSONL].
func ReadJSONL(r io.Reader) ([]Event, error) {
	var out []Event
	dec := json.NewDecoder(r)
	for {
		var e Event
		if err := dec.Decode(&e); errors.Is(err, io.EOF) {
			return out, nil
		} else if err != nil {
			return out, err
		}
		out = append(out, e)
	}
}

// PrettyPrint reads a JSONL trace from r and writes readable lines to w. If
// tail > 0 only the last tail events are printed.
func PrettyPrint(w io.Writer, r io.Reader, tail int) error {
	evs, err := ReadJSONL(r)
	if err != nil {
		return err
	}
	if tail > 0 && len(evs) > tail {
		_, _ = fmt.Fprintf(w, "... %d earlier events omitted\n", len(evs)-tail)
		evs = evs[len(evs)-tail:]
	}
	for _, e := range evs {
		if _, err := fmt.Fprintln(w, e); err != nil {
			return err
		}
	}
	return nil
}
