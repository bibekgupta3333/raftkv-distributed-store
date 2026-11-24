package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// maxHandover bounds a TimeoutNow handover, from the old leader's "handing
// leadership over" record to its "shutdown handover finished" record (WBS
// 7.4.5). It is below the shortest election timeout (10 ticks of 100 ms,
// D-08), so a leader that only stepped down because the followers timed out
// and elected someone else fails the check.
const maxHandover = 800 * time.Millisecond

// handover is what a member's log says about its shutdown handover
// (internal/node/node.go: startHandover and the Ready loop, WBS 6.2.3).
// leader is the leader it knew when it stopped: 0 if it had not heard from the
// new one yet, its own ID if the handover timed out.
type handover struct {
	nodeID, leader uint64
	began, ended   time.Time
	skipped        string
}

// parseHandover reads a member's JSON log (raftkvd writes one record per line).
func parseHandover(log []byte) handover {
	var h handover
	for _, line := range bytes.Split(log, []byte("\n")) {
		var rec struct {
			Time   time.Time `json:"time"`
			Msg    string    `json:"msg"`
			NodeID uint64    `json:"node_id"`
			Leader uint64    `json:"leader"`
			Err    string    `json:"err"`
		}
		if json.Unmarshal(line, &rec) != nil {
			continue
		}
		switch rec.Msg {
		case "handing leadership over before shutdown":
			h.nodeID, h.began = rec.NodeID, rec.Time
		case "shutdown handover finished":
			h.nodeID, h.ended, h.leader = rec.NodeID, rec.Time, rec.Leader
		case "shutdown handover not started", "shutdown handover":
			h.skipped = rec.Msg + ": " + rec.Err
		}
	}
	return h
}

// verdict judges one member's handover. It returns a note for a good
// handover, a problem for a bad one, and neither when the member did not lead
// when it got SIGTERM.
func (h handover) verdict(pod string) (note, problem string) {
	d := h.ended.Sub(h.began)
	switch {
	case h.skipped != "":
		return "", fmt.Sprintf("%s led at SIGTERM but did not hand over (%s)", pod, h.skipped)
	case h.began.IsZero():
		return "", ""
	case h.ended.IsZero():
		return "", fmt.Sprintf("%s began a handover and never finished it", pod)
	case h.leader == h.nodeID:
		return "", fmt.Sprintf("%s: the handover timed out after %v; it still led", pod, d)
	case d > maxHandover:
		return "", fmt.Sprintf("%s stepped down only after %v (> %v): an election timeout, not a TimeoutNow handover",
			pod, d, maxHandover)
	}
	return fmt.Sprintf("%s (node %d) -> node %d in %v", pod, h.nodeID, h.leader, d), ""
}
