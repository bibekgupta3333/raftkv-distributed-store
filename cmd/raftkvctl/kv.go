package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// Exit codes (api-design.md section 9).
const (
	exitOK           = 0
	exitUsage        = 1 // usage or INVALID_ARGUMENT
	exitUnavailable  = 2 // unavailable or deadline (also bad flags: see run)
	exitNotFound     = 3
	exitPrecondition = 6
	exitCASFailed    = 4 // compare failed (not an error)
	exitUnknown      = 7 // the write may or may not have applied
)

// kvCommand holds the flags shared by put, get and del.
type kvCommand struct {
	fs        *flag.FlagSet
	endpoints *string
	timeout   *time.Duration
	clusterID *uint64
}

func newKVCommand(name string, stderr io.Writer) *kvCommand {
	fs := flag.NewFlagSet("raftkvctl "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	def := os.Getenv("RAFTKV_ENDPOINTS")
	if def == "" {
		def = "localhost:2379"
	}
	return &kvCommand{fs: fs,
		endpoints: fs.String("endpoints", def, "comma-separated member addresses (env RAFTKV_ENDPOINTS)"),
		timeout:   fs.Duration("timeout", 10*time.Second, "overall deadline for the request"),
		clusterID: fs.Uint64("cluster-id", 0, "refuse servers of another cluster (0 = don't check)"),
	}
}

func (k *kvCommand) connect() (*client.Client, error) {
	var eps []string
	for e := range strings.SplitSeq(*k.endpoints, ",") {
		if e = strings.TrimSpace(e); e != "" {
			eps = append(eps, e)
		}
	}
	return client.New(client.Config{Endpoints: eps, ClusterID: *k.clusterID})
}

// runKV dispatches put, get and del.
func runKV(cmd string, args []string, stdout, stderr io.Writer) int {
	k := newKVCommand(cmd, stderr)
	var (
		prev      = k.fs.Bool("prev", false, "return the previous value (put, del)")
		valueFile = k.fs.String("value-file", "", "read the value from this file (put)")
		stale     = k.fs.Bool("stale", false, "serve from one member's local state, not linearizable (get)")
		minRev    = k.fs.Uint64("min-revision", 0, "with --stale, wait until the member applied this index (get)")
		asHex     = k.fs.Bool("hex", false, "print the value as hex (get)")
	)
	if err := k.fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return 2
	}
	pos := k.fs.Args()
	want := map[string]int{"put": 2, "get": 1, "del": 1}[cmd]
	if cmd == "put" && *valueFile != "" {
		want = 1
	}
	if len(pos) != want {
		_, _ = fmt.Fprintf(stderr, "raftkvctl %s: want %d argument(s), got %d\n%s\n", cmd, want, len(pos), usage)
		return 2
	}
	cl, err := k.connect()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return exitUsage
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *k.timeout)
	defer cancel()
	key := []byte(pos[0])

	var opts []client.Option
	if *prev {
		opts = append(opts, client.WithPrevious())
	}
	switch cmd {
	case "put":
		value := []byte(nil)
		if *valueFile != "" {
			if value, err = os.ReadFile(*valueFile); err != nil {
				_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
				return exitUsage
			}
		} else {
			value = []byte(pos[1])
		}
		res, err := cl.Put(ctx, key, value, opts...)
		if err != nil {
			return report(stderr, err, true)
		}
		_, _ = fmt.Fprintf(stdout, "OK revision=%d\n", res.Revision)
		printPrev(stdout, res.Previous)
	case "del":
		res, err := cl.Delete(ctx, key, opts...)
		if err != nil {
			return report(stderr, err, true)
		}
		_, _ = fmt.Fprintf(stdout, "OK revision=%d deleted=%t\n", res.Revision, res.Deleted)
		printPrev(stdout, res.Previous)
	case "get":
		var gopts []client.Option
		if *stale {
			gopts = append(gopts, client.WithStale(*minRev))
		}
		res, err := cl.Get(ctx, key, gopts...)
		if err != nil {
			return report(stderr, err, false)
		}
		if !res.Found {
			_, _ = fmt.Fprintln(stderr, "raftkvctl: key not found")
			return exitNotFound
		}
		if *asHex {
			_, _ = fmt.Fprintln(stdout, hex.EncodeToString(res.KV.Value))
		} else {
			_, _ = fmt.Fprintf(stdout, "%s\n", res.KV.Value)
		}
	}
	return exitOK
}

func printPrev(out io.Writer, kv *client.KeyValue) {
	if kv != nil {
		_, _ = fmt.Fprintf(out, "previous=%s\n", kv.Value)
	}
}

// report prints err and maps it to an exit code. A write that failed with an
// unlabelled or INDETERMINATE error exits 7: it may have been applied.
func report(stderr io.Writer, err error, write bool) int {
	var ce *client.Error
	if !errors.As(err, &ce) {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return exitUnavailable
	}
	_, _ = fmt.Fprintln(stderr, "raftkvctl:", ce)
	switch {
	case ce.Code == codes.InvalidArgument:
		return exitUsage
	case ce.Code == codes.FailedPrecondition:
		return exitPrecondition
	case write && ce.Indeterminate():
		_, _ = fmt.Fprintln(stderr, "raftkvctl: the write may or may not have been applied")
		return exitUnknown
	default:
		return exitUnavailable
	}
}
