package linearizability

import (
	"io"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

// Verdict is the outcome of a check.
type Verdict int

// Verdicts. Only [Linearizable] is a pass: a checker timeout is a failure
// (testing.md 3.4, exit code 3), because "didn't finish" proves nothing.
const (
	Linearizable Verdict = iota
	Violation
	TimedOut
)

func (v Verdict) String() string {
	switch v {
	case Linearizable:
		return "linearizable"
	case Violation:
		return "violation"
	default:
		return "checker timeout"
	}
}

// Report is a checked history.
type Report struct {
	Verdict Verdict
	Stats   Stats
	// Ops is how many operations the checker saw.
	Ops int

	info porcupine.LinearizationInfo
}

// OK reports whether the history passed. A timeout is not OK.
func (r Report) OK() bool { return r.Verdict == Linearizable }

// DefaultTimeout is the per-check budget (testing.md 3.4: 60 s per key).
const DefaultTimeout = 60 * time.Second

// Check runs the KV model over events. timeout <= 0 means [DefaultTimeout].
func Check(events []Event, timeout time.Duration) Report {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ops, stats := ConvertEvents(events)
	res, info := porcupine.CheckOperationsVerbose(KVModel(), ops, timeout)
	return Report{Verdict: verdictOf(res), Stats: stats, Ops: len(ops), info: info}
}

// CheckRecorder checks everything a recorder captured.
func CheckRecorder(r *Recorder, timeout time.Duration) Report { return Check(r.Events(), timeout) }

func verdictOf(res porcupine.CheckResult) Verdict {
	switch res {
	case porcupine.Ok:
		return Linearizable
	case porcupine.Illegal:
		return Violation
	default: // Unknown: timed out
		return TimedOut
	}
}

// WriteHTML renders Porcupine's visualization of the checked history.
func (r Report) WriteHTML(w io.Writer) error { return porcupine.Visualize(KVModel(), r.info, w) }

// SaveHTML writes the visualization to path.
func (r Report) SaveHTML(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := r.WriteHTML(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
