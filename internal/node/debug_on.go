//go:build debugpropose

package node

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// DebugProposeMethod is the full gRPC method of the temporary debug RPC. It
// carries a BytesValue (the raw log entry payload) and answers a UInt64Value
// (the entry's log index once applied). It is hand-registered rather than
// generated from a .proto because it is throw-away scaffolding for M2, removed
// when the real KV API lands in M4 (WBS 2.3.2).
const DebugProposeMethod = "/raftkv.debug.v1.Debug/Propose"

type debugServer struct{ n *Node }

func (d *debugServer) propose(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	req := new(wrapperspb.BytesValue)
	if err := dec(req); err != nil {
		return nil, err
	}
	idx, err := d.n.ProposeAndWait(ctx, req.GetValue())
	var nl *raft.NotLeaderError
	switch {
	case err == nil:
		return wrapperspb.UInt64(idx), nil
	case errors.As(err, &nl):
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("not leader; leader is %d", nl.Leader))
	case errors.Is(err, raft.ErrProposalDropped):
		return nil, status.Error(codes.Unavailable, err.Error())
	default:
		return nil, status.FromContextError(err).Err()
	}
}

// registerDebug exposes the debug Propose RPC.
func registerDebug(srv *grpc.Server, n *Node) {
	d := &debugServer{n: n}
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "raftkv.debug.v1.Debug",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Propose",
			Handler:    d.propose,
		}},
	}, d)
}

// DebugPropose calls the debug RPC on conn and returns the applied log index.
func DebugPropose(ctx context.Context, conn grpc.ClientConnInterface, data []byte) (uint64, error) {
	resp := new(wrapperspb.UInt64Value)
	if err := conn.Invoke(ctx, DebugProposeMethod, wrapperspb.Bytes(data), resp); err != nil {
		return 0, err
	}
	return resp.GetValue(), nil
}
