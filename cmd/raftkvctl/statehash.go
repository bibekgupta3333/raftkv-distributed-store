package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// runStateHash implements `raftkvctl debug state-hash` (WBS 3.1.8): it opens a
// stopped node's state machine read-only and prints the applied index and a
// hash of the replicated state. Two replicas at the same applied index must
// print the same hash (D-18, INV-5).
func runStateHash(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("raftkvctl debug state-hash", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("data-dir", "", "data directory of a stopped node")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *dir == "" {
		_, _ = fmt.Fprintln(stderr, "raftkvctl: -data-dir is required")
		return 2
	}
	smDir := filepath.Join(*dir, node.StateDir)
	live, err := storage.ReadCurrent(smDir) // the live database (storage-design 7)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return 1
	}
	s, err := storage.Open(storage.Options{Dir: filepath.Join(smDir, live), ReadOnly: true})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return 1
	}
	defer func() { _ = s.Close() }()
	applied, sum, err := s.StateHash()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "applied_index=%d state_hash=%s\n", applied, hex.EncodeToString(sum[:]))
	return 0
}
