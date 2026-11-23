package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// Outcome says whether a failed write may have been applied (api-design 6.1).
type Outcome int

const (
	// OutcomeIndeterminate: the request may have been, or may still be,
	// applied. It is the default for errors the server did not label.
	OutcomeIndeterminate Outcome = iota
	// OutcomeDefinite: the server proved the request was never proposed.
	OutcomeDefinite
)

func (o Outcome) String() string {
	if o == OutcomeDefinite {
		return "DEFINITE"
	}
	return "INDETERMINATE"
}

// Reasons the client branches on (api-design 6.1); never on message text.
const (
	reasonNotLeader      = "NOT_LEADER"
	reasonNoLeader       = "NO_LEADER"
	reasonNoQuorum       = "NO_QUORUM"
	reasonLeaderNotReady = "LEADER_NOT_READY"
	reasonNotProposed    = "NOT_PROPOSED"
	reasonOverloaded     = "OVERLOADED"
	reasonOutcomeUnknown = "OUTCOME_UNKNOWN"
	reasonSessionExpired = "SESSION_EXPIRED"
)

// codeOf is the status code for a finished context.
func codeOf(ctx context.Context) codes.Code {
	if errors.Is(ctx.Err(), context.Canceled) {
		return codes.Canceled
	}
	return codes.DeadlineExceeded
}

// Error is a failed request: the gRPC status, the server's reason, whether a
// write may have applied, and any leader hint.
type Error struct {
	Code    codes.Code
	Reason  string // ErrorInfo.reason; empty for transport errors
	Outcome Outcome
	Hint    *raftkvv1.LeaderHint
	Message string
	// Attempts is how many RPCs were sent before giving up (0 if no member
	// was reachable).
	Attempts int
}

func (e *Error) Error() string {
	reason := e.Reason
	if reason == "" {
		reason = "-"
	}
	return fmt.Sprintf("raftkv: %s (%s) outcome=%s after %d attempt(s): %s", e.Code, reason, e.Outcome, e.Attempts, e.Message)
}

// Indeterminate reports whether a write that failed this way may have been applied.
func (e *Error) Indeterminate() bool { return e.Outcome == OutcomeIndeterminate }

// fromRPC converts a gRPC error. An error without a server-provided
// ErrorInfo is INDETERMINATE (transport failure, client-side deadline).
func fromRPC(err error) *Error {
	st, ok := status.FromError(err)
	if !ok {
		return &Error{Code: codes.Unknown, Message: err.Error()}
	}
	e := &Error{Code: st.Code(), Message: st.Message()}
	for _, d := range st.Details() {
		switch d := d.(type) {
		case *errdetails.ErrorInfo:
			if d.GetDomain() == "raftkv" {
				e.Reason = d.GetReason()
				if d.GetMetadata()["outcome"] == "DEFINITE" {
					e.Outcome = OutcomeDefinite
				}
			}
		case *raftkvv1.LeaderHint:
			e.Hint = d
		}
	}
	return e
}

// IsNotLeader reports whether err is a NOT_LEADER answer.
func IsNotLeader(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Reason == reasonNotLeader
}
