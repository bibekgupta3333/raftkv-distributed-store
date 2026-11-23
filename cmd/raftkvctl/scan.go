package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// runCAS is `raftkvctl cas` (WBS 4.4.4, api-design 9): exactly one of
// --expect-value / --expect-revision / --expect-absent, plus --new-value or
// --delete. Exit 0 if the swap happened, 4 if the compare failed.
func runCAS(args []string, stdout, stderr io.Writer) int {
	k := newKVCommand("cas", stderr)
	var (
		expectValue = k.fs.String("expect-value", "", "swap only if the key has exactly this value")
		expectRev   = k.fs.Uint64("expect-revision", 0, "swap only if the key's mod revision is this")
		expectAbs   = k.fs.Bool("expect-absent", false, "swap only if the key does not exist")
		newValue    = k.fs.String("new-value", "", "value to set on success")
		del         = k.fs.Bool("delete", false, "delete the key on success")
	)
	if err := k.fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return 2
	}
	set := map[string]bool{}
	k.fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	compares := 0
	for _, n := range []string{"expect-value", "expect-revision", "expect-absent"} {
		if set[n] {
			compares++
		}
	}
	if len(k.fs.Args()) != 1 || compares != 1 || set["new-value"] == *del {
		_, _ = fmt.Fprintf(stderr, "raftkvctl cas: want KEY, one of --expect-value/--expect-revision/--expect-absent, and --new-value or --delete\n%s\n", usage)
		return 2
	}
	var cmp client.Compare
	switch {
	case set["expect-value"]:
		cmp = client.ValueEquals([]byte(*expectValue))
	case set["expect-revision"]:
		cmp = client.ModRevisionEquals(*expectRev)
	case *expectAbs:
		cmp = client.Absent()
	default:
		_, _ = fmt.Fprintln(stderr, "raftkvctl cas: --expect-absent=false is not a compare")
		return 2
	}
	act := client.SetValue([]byte(*newValue))
	if *del {
		act = client.DeleteKey()
	}
	cl, err := k.connect()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return exitUsage
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *k.timeout)
	defer cancel()
	res, err := cl.CompareAndSwap(ctx, []byte(k.fs.Arg(0)), cmp, act)
	if err != nil {
		return report(stderr, err, true)
	}
	if !res.Succeeded {
		_, _ = fmt.Fprintf(stdout, "FAILED revision=%d\n", res.Revision)
		if res.Found {
			_, _ = fmt.Fprintf(stdout, "current=%s mod_revision=%d\n", res.Current.Value, res.Current.ModRevision)
		} else {
			_, _ = fmt.Fprintln(stdout, "current=<absent>")
		}
		return exitCASFailed
	}
	_, _ = fmt.Fprintf(stdout, "OK revision=%d\n", res.Revision)
	return exitOK
}

// runRange is `raftkvctl range` (WBS 4.4.4): one page, or every page with
// --all. Each line is "KEY VALUE" (or just KEY with --keys-only); a final
// "more=true next=<hex token>" line tells how to continue without --all.
func runRange(args []string, stdout, stderr io.Writer) int {
	k := newKVCommand("range", stderr)
	var (
		prefix   = k.fs.Bool("prefix", false, "treat START as a key prefix")
		limit    = k.fs.Int("limit", 0, "keys per page, 1..1000 (0 = 1000)")
		keysOnly = k.fs.Bool("keys-only", false, "print keys only")
		reverse  = k.fs.Bool("reverse", false, "scan in descending key order")
		all      = k.fs.Bool("all", false, "follow continuation tokens to the end")
		stale    = k.fs.Bool("stale", false, "serve from one member's local state, not linearizable")
		minRev   = k.fs.Uint64("min-revision", 0, "with --stale, wait until the member applied this index")
		token    = k.fs.String("page-token", "", "continue from a previous page (hex)")
		asHex    = k.fs.Bool("hex", false, "print values as hex")
	)
	if err := k.fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return 2
	}
	pos := k.fs.Args()
	if len(pos) < 1 || len(pos) > 2 || (*prefix && len(pos) != 1) {
		_, _ = fmt.Fprintf(stderr, "raftkvctl range: want START [END], or --prefix PREFIX\n%s\n", usage)
		return 2
	}
	rr := client.RangeRequest{Limit: *limit, KeysOnly: *keysOnly, Reverse: *reverse, Stale: *stale, MinRevision: *minRev}
	if *prefix {
		rr.Prefix = []byte(pos[0])
	} else {
		rr.Start = []byte(pos[0])
		if len(pos) == 2 {
			rr.End = []byte(pos[1])
		}
	}
	if *token != "" {
		tok, err := hex.DecodeString(*token)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "raftkvctl range: --page-token is not hex")
			return 2
		}
		rr.PageToken = tok
	}
	cl, err := k.connect()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
		return exitUsage
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *k.timeout)
	defer cancel()
	var last *client.RangePage
	err = cl.RangeAll(ctx, rr, func(p *client.RangePage) bool {
		for _, kv := range p.KVs {
			switch {
			case *keysOnly:
				_, _ = fmt.Fprintf(stdout, "%s\n", kv.Key)
			case *asHex:
				_, _ = fmt.Fprintf(stdout, "%s %s\n", kv.Key, hex.EncodeToString(kv.Value))
			default:
				_, _ = fmt.Fprintf(stdout, "%s %s\n", kv.Key, kv.Value)
			}
		}
		last = p
		return *all
	})
	if err != nil {
		return report(stderr, err, false)
	}
	if last != nil && last.More {
		_, _ = fmt.Fprintf(stdout, "more=true next=%s\n", hex.EncodeToString(last.NextPageToken))
	}
	return exitOK
}
