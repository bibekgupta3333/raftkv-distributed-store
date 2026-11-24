package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// exitNotCaughtUp is `member promote` without --wait on a lagging learner
// (api-design.md section 10).
const exitNotCaughtUp = 5

// runMember implements `raftkvctl member add-learner|promote|remove|transfer|list`
// (D-23, WBS 6.4.7). Any member may be asked: followers forward mutating calls
// to the leader (api-design.md section 4).
func runMember(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return exitUnavailable
	}
	sub, args := args[0], args[1:]
	k := newKVCommand("member "+sub, stderr)
	fs := k.fs
	var (
		peerURL, clientURL *string
		wait, force, stale *bool
	)
	switch sub {
	case "add-learner":
		peerURL = fs.String("peer-url", "", "the new member's peer address host:port (required)")
		clientURL = fs.String("client-url", "", "the new member's client address (default: the peer URL)")
	case "promote":
		wait = fs.Bool("wait", false, "retry until the learner has caught up, within --timeout")
	case "remove":
		force = fs.Bool("force", false, "remove even if the remaining voters could not form a quorum now")
	case "list":
		stale = fs.Bool("stale", false, "answer from one member's local view, not linearizable")
	case "transfer":
	default:
		_, _ = fmt.Fprintf(stderr, "raftkvctl: unknown member command %q\n%s\n", sub, usage)
		return exitUnavailable
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUnavailable
	}
	rest := fs.Args()
	id := func(i int, what string) (uint64, bool) {
		if len(rest) <= i {
			_, _ = fmt.Fprintf(stderr, "raftkvctl: member %s needs %s\n", sub, what)
			return 0, false
		}
		v, err := strconv.ParseUint(rest[i], 10, 64)
		if err != nil || v == 0 {
			_, _ = fmt.Fprintf(stderr, "raftkvctl: bad member ID %q\n", rest[i])
			return 0, false
		}
		return v, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), *k.timeout)
	defer cancel()
	hdr := &raftkvv1.RequestHeader{ClusterId: *k.clusterID}
	eps := k.endpointList()

	switch sub {
	case "add-learner":
		if *peerURL == "" || len(rest) > 1 {
			_, _ = fmt.Fprintln(stderr, "raftkvctl: usage: member add-learner --peer-url HOST:PORT [--client-url HOST:PORT] [NAME]")
			return exitUsage
		}
		name := ""
		if len(rest) == 1 {
			name = rest[0]
		}
		var resp *raftkvv1.AddLearnerResponse
		err := callCluster(ctx, eps, func(ctx context.Context, c raftkvv1.ClusterClient) (err error) {
			resp, err = c.AddLearner(ctx, &raftkvv1.AddLearnerRequest{Header: hdr, Name: name, PeerUrl: *peerURL, ClientUrl: *clientURL})
			return err
		})
		if err != nil {
			return reportAdmin(stderr, err)
		}
		m := resp.GetMember()
		_, _ = fmt.Fprintf(stdout, "added learner id=%d name=%s peer_url=%s\n", m.GetId(), m.GetName(), m.GetPeerUrl())
		return exitOK
	case "promote":
		target, ok := id(0, "a member ID")
		if !ok {
			return exitUsage
		}
		for {
			err := callCluster(ctx, eps, func(ctx context.Context, c raftkvv1.ClusterClient) error {
				_, err := c.PromoteLearner(ctx, &raftkvv1.PromoteLearnerRequest{Header: hdr, MemberId: target})
				return err
			})
			if err == nil {
				_, _ = fmt.Fprintf(stdout, "promoted %d\n", target)
				return exitOK
			}
			if reasonOf(err) != "LEARNER_NOT_CAUGHT_UP" {
				return reportAdmin(stderr, err)
			}
			if !*wait {
				_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
				return exitNotCaughtUp
			}
			select {
			case <-ctx.Done():
				_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
				return exitNotCaughtUp
			case <-time.After(500 * time.Millisecond):
			}
		}
	case "remove":
		target, ok := id(0, "a member ID")
		if !ok {
			return exitUsage
		}
		err := callCluster(ctx, eps, func(ctx context.Context, c raftkvv1.ClusterClient) error {
			_, err := c.RemoveMember(ctx, &raftkvv1.RemoveMemberRequest{Header: hdr, MemberId: target, Force: *force})
			return err
		})
		if err != nil {
			return reportAdmin(stderr, err)
		}
		_, _ = fmt.Fprintf(stdout, "removed %d\n", target)
		return exitOK
	case "transfer":
		var target uint64
		if len(rest) > 0 {
			t, ok := id(0, "a member ID")
			if !ok {
				return exitUsage
			}
			target = t
		}
		var resp *raftkvv1.TransferLeadershipResponse
		err := callCluster(ctx, eps, func(ctx context.Context, c raftkvv1.ClusterClient) (err error) {
			resp, err = c.TransferLeadership(ctx, &raftkvv1.TransferLeadershipRequest{Header: hdr, TargetMemberId: target})
			return err
		})
		if err != nil {
			return reportAdmin(stderr, err)
		}
		_, _ = fmt.Fprintf(stdout, "leader=%d term=%d\n", resp.GetNewLeaderId(), resp.GetNewTerm())
		return exitOK
	default: // list
		cons := raftkvv1.Consistency_LINEARIZABLE
		if *stale {
			cons = raftkvv1.Consistency_STALE
		}
		var resp *raftkvv1.ListMembersResponse
		err := callCluster(ctx, eps, func(ctx context.Context, c raftkvv1.ClusterClient) (err error) {
			resp, err = c.ListMembers(ctx, &raftkvv1.ListMembersRequest{Header: hdr, Consistency: cons})
			return err
		})
		if err != nil {
			return reportAdmin(stderr, err)
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "ID\tROLE\tNAME\tPEER_URL\tCLIENT_URL\tLEADER")
		for _, m := range resp.GetMembers() {
			lead := ""
			if m.GetId() == resp.GetHeader().GetLeaderId() {
				lead = "*"
			}
			_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n", m.GetId(), m.GetRole(), m.GetName(), m.GetPeerUrl(), m.GetClientUrl(), lead)
		}
		_ = tw.Flush()
		_, _ = fmt.Fprintf(stdout, "config_index=%d\n", resp.GetConfigIndex())
		return exitOK
	}
}

func (k *kvCommand) endpointList() []string {
	var eps []string
	for e := range strings.SplitSeq(*k.endpoints, ",") {
		if e = strings.TrimSpace(e); e != "" {
			eps = append(eps, e)
		}
	}
	return eps
}

// callCluster tries the endpoints in order until one answers anything other
// than "unavailable before sending" (NO_LEADER, a dead endpoint). Any member
// forwards to the leader, so the first live one usually suffices.
func callCluster(ctx context.Context, eps []string, f func(context.Context, raftkvv1.ClusterClient) error) error {
	if len(eps) == 0 {
		return status.Error(codes.InvalidArgument, "no endpoints")
	}
	var last error
	for attempt := 0; ctx.Err() == nil; attempt++ {
		ep := eps[attempt%len(eps)]
		conn, err := grpc.NewClient(ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = f(actx, raftkvv1.NewClusterClient(conn))
		cancel()
		_ = conn.Close()
		if err == nil {
			return nil
		}
		last = err
		r := reasonOf(err)
		retry := r == "NO_LEADER" || r == "NOT_LEADER" || (r == "" && status.Code(err) == codes.Unavailable)
		if !retry {
			return err
		}
		if attempt%len(eps) == len(eps)-1 {
			select {
			case <-ctx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	if last == nil {
		last = ctx.Err()
	}
	return last
}

// reasonOf is the ErrorInfo reason of a RaftKV error, or "".
func reasonOf(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

// reportAdmin prints err and maps it to an exit code (api-design.md section 10).
func reportAdmin(stderr io.Writer, err error) int {
	_, _ = fmt.Fprintln(stderr, "raftkvctl:", err)
	switch status.Code(err) {
	case codes.InvalidArgument:
		return exitUsage
	case codes.FailedPrecondition, codes.AlreadyExists:
		return exitPrecondition
	case codes.NotFound:
		return exitNotFound
	}
	if reasonOf(err) == "OUTCOME_UNKNOWN" {
		_, _ = fmt.Fprintln(stderr, "raftkvctl: the change may or may not have been applied")
		return exitUnknown
	}
	return exitUnavailable
}
