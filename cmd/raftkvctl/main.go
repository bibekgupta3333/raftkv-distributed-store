// Command raftkvctl is the RaftKV command-line client.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/version"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version.String("raftkvctl"))
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, "raftkvctl: not implemented yet")
	os.Exit(1)
}
