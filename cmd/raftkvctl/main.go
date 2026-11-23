// Command raftkvctl is the RaftKV command-line client.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/version"
)

const usage = `usage: raftkvctl [-version] <command>

commands:
  put [--endpoints a,b,c] [--timeout 10s] [--prev] [--value-file F] KEY [VALUE]
  get [--endpoints a,b,c] [--timeout 10s] [--stale [--min-revision N]] [--hex] KEY
      exit 3 if the key does not exist
  del [--endpoints a,b,c] [--timeout 10s] [--prev] KEY
      exit 7 if a write's outcome is unknown (it may have been applied)
  cas [--endpoints a,b,c] (--expect-value V | --expect-revision N | --expect-absent)
      (--new-value V | --delete) KEY
      exit 4 if the compare failed (the current value is printed)
  range [--endpoints a,b,c] [--limit N] [--keys-only] [--reverse] [--all]
      [--stale [--min-revision N]] [--page-token HEX] (START [END] | --prefix PREFIX)
      without --all, prints one page and a "more=true next=HEX" line if more remain
  debug state-hash -data-dir DIR
      print a stopped node's applied index and state hash; equal hashes at the
      same applied index mean equal state machines
  debug wal-dump -data-dir DIR [-from N] [-to N] [-max-data N]
      print a stopped node's HardState and log entries, ending with a digest
      line; equal digests on two nodes mean equal logs`

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run is main without the process globals, so it can be tested.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	switch args[0] {
	case "-version", "--version":
		_, _ = fmt.Fprintln(stdout, version.String("raftkvctl"))
		return 0
	case "put", "get", "del":
		return runKV(args[0], args[1:], stdout, stderr)
	case "cas":
		return runCAS(args[1:], stdout, stderr)
	case "range":
		return runRange(args[1:], stdout, stderr)
	case "debug":
		return runDebug(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "raftkvctl: unknown command %q\n%s\n", args[0], usage)
		return 2
	}
}

func runDebug(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "state-hash" {
		return runStateHash(args[1:], stdout, stderr)
	}
	if len(args) == 0 || args[0] != "wal-dump" {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	fs := flag.NewFlagSet("raftkvctl debug wal-dump", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("data-dir", "", "data directory of a stopped node")
	from := fs.Uint64("from", 0, "first entry index to print")
	to := fs.Uint64("to", 0, "last entry index to print (0 = no limit)")
	maxData := fs.Int("max-data", 32, "payload bytes to show per entry")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *dir == "" {
		_, _ = fmt.Fprintln(stderr, "raftkvctl: -data-dir is required")
		return 2
	}
	if err := node.DumpWAL(stdout, *dir, node.DumpOptions{From: *from, To: *to, MaxData: *maxData}); err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return 1
	}
	return 0
}
