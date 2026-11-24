package chaos

import (
	"strings"
	"testing"
)

// The rolling-restart check (WBS 7.4.5) reads raftkvd's JSON log records;
// the record shapes here are the ones internal/node writes at shutdown.
func TestHandoverVerdict(t *testing.T) {
	const (
		other    = `{"time":"2026-11-23T10:00:00.000Z","level":"INFO","msg":"node stopping","node_id":3,"term":7,"component":"node"}`
		began    = `{"time":"2026-11-23T10:00:01.000Z","level":"INFO","msg":"handing leadership over before shutdown","node_id":3,"term":7,"component":"node","to":2}`
		fast     = `{"time":"2026-11-23T10:00:01.040Z","level":"INFO","msg":"shutdown handover finished","node_id":3,"term":8,"component":"node","leader":2}`
		unknown  = `{"time":"2026-11-23T10:00:01.100Z","level":"INFO","msg":"shutdown handover finished","node_id":3,"term":8,"component":"node","leader":0}`
		slow     = `{"time":"2026-11-23T10:00:02.200Z","level":"INFO","msg":"shutdown handover finished","node_id":3,"term":8,"component":"node","leader":4}`
		timedOut = `{"time":"2026-11-23T10:00:03.200Z","level":"INFO","msg":"shutdown handover finished","node_id":3,"term":7,"component":"node","leader":3}`
		skipped  = `{"time":"2026-11-23T10:00:01.000Z","level":"WARN","msg":"shutdown handover not started","node_id":3,"term":7,"component":"node","err":"raft: transfer in progress"}`
	)
	for _, tc := range []struct {
		name          string
		lines         []string
		note, problem string // substrings; "" = must be empty
	}{
		{"follower", []string{other}, "", ""},
		{"empty log", nil, "", ""},
		{"handover", []string{other, began, "not json", fast}, "node 3) -> node 2 in 40ms", ""},
		{"new leader not known yet", []string{began, unknown}, "-> node 0 in 100ms", ""},
		{"election timeout", []string{began, slow}, "", "election timeout"},
		{"timed out", []string{began, timedOut}, "", "still led"},
		{"never finished", []string{began}, "", "never finished"},
		{"not started", []string{skipped}, "", "did not hand over"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			note, problem := parseHandover([]byte(strings.Join(tc.lines, "\n"))).verdict("raftkv-2")
			for _, c := range []struct{ got, want, what string }{{note, tc.note, "note"}, {problem, tc.problem, "problem"}} {
				if (c.want == "") != (c.got == "") || !strings.Contains(c.got, c.want) {
					t.Errorf("%s = %q, want %q", c.what, c.got, c.want)
				}
			}
		})
	}
}
