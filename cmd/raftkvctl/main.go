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
	case "debug":
		return runDebug(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "raftkvctl: unknown command %q\n%s\n", args[0], usage)
		return 2
	}
}

func runDebug(args []string, stdout, stderr io.Writer) int {
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
