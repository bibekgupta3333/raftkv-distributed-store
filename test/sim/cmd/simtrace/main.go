// Command simtrace pretty-prints a JSONL trace written by a failing simulator
// seed (WBS 1.7.2):
//
//	go run ./test/sim/cmd/simtrace [-tail N] trace.jsonl
//
// With no file argument it reads standard input.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/bibekgupta3333/raftkv-distributed-store/test/sim"
)

func main() {
	tail := flag.Int("tail", 0, "print only the last N events (0 = all)")
	flag.Parse()
	var in io.Reader = os.Stdin
	if flag.NArg() > 1 {
		fmt.Fprintln(os.Stderr, "usage: simtrace [-tail N] [trace.jsonl]")
		os.Exit(2)
	}
	if flag.NArg() == 1 {
		f, err := os.Open(flag.Arg(0))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer func() { _ = f.Close() }()
		in = f
	}
	if err := sim.PrettyPrint(os.Stdout, in, *tail); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
