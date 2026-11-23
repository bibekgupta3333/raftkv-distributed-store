// Command raftkvd is the RaftKV server.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/version"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/wal"
)

// Exit codes. 78 is EX_CONFIG: the node refuses to start because its data
// directory is corrupt or belongs to someone else (D-32, D-33).
const (
	exitOK     = 0
	exitFailed = 1
	exitConfig = 78
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run is main without the process globals, so it can be tested.
func run(args []string, stdout, stderr io.Writer) int {
	if slices.Contains(args, "-version") || slices.Contains(args, "--version") {
		_, _ = fmt.Fprintln(stdout, version.String("raftkvd"))
		return exitOK
	}
	cfg, err := node.LoadConfig(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		_, _ = fmt.Fprintln(stderr, "raftkvd:", err)
		return exitConfig
	}
	n, err := node.New(cfg, node.Options{LogWriter: stderr})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvd:", err)
		if errors.Is(err, node.ErrIdentityMismatch) || errors.Is(err, wal.ErrCorrupt) {
			return exitConfig
		}
		return exitFailed
	}
	// Blocks until SIGTERM or SIGINT, then shuts down gracefully (D-33: any
	// fsync error inside exits immediately instead).
	if err := node.RunUntilSignal(n); err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvd:", err)
		return exitFailed
	}
	return exitOK
}
