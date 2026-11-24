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
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/server"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/snapshot"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
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
	// M3: the Pebble state machine and the KV service replace the in-memory applier.
	n, err := server.OpenKVNode(cfg, node.Options{LogWriter: stderr})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvd:", err)
		if refusesToStart(err) {
			return exitConfig
		}
		return exitFailed
	}
	// Blocks until SIGTERM or SIGINT, then shuts down gracefully (D-33: any
	// fsync error inside exits immediately instead).
	if err := node.RunUntilSignal(n.Node); errors.Is(err, node.ErrRemoved) {
		_, _ = fmt.Fprintln(stderr, "raftkvd:", err)
		return exitOK // D-31: a removed member stops for good
	} else if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvd:", err)
		return exitFailed
	}
	return exitOK
}

// refusesToStart reports whether err means the data directory must not be
// used as it is: corrupt, someone else's (D-32, D-33), or written in an
// on-disk format this build does not read (WBS 7.5.2). Restarting will not
// help, so these exit with EX_CONFIG instead of 1.
func refusesToStart(err error) bool {
	for _, target := range []error{
		node.ErrIdentityMismatch, wal.ErrCorrupt, node.ErrAppliedOutsideLog,
		wal.ErrFormatVersion, storage.ErrSchemaVersion, snapshot.ErrFormat,
		node.ErrStateLost, node.ErrRemoved, // D-32, D-31
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
