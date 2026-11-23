package server

import (
	"context"
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Error domain, reasons and outcome metadata (api-design.md section 6).
const (
	ErrorDomain = "raftkv"

	ReasonNotLeader       = "NOT_LEADER"
	ReasonNoLeader        = "NO_LEADER"
	ReasonOutcomeUnknown  = "OUTCOME_UNKNOWN"
	ReasonNotProposed     = "NOT_PROPOSED"
	ReasonOverloaded      = "OVERLOADED"
	ReasonRevisionMissing = "REVISION_NOT_REACHED"
	ReasonClusterMismatch = "CLUSTER_ID_MISMATCH"
	ReasonEmptyKey        = "EMPTY_KEY"
	ReasonKeyTooLarge     = "KEY_TOO_LARGE"
	ReasonValueTooLarge   = "VALUE_TOO_LARGE"
	ReasonMalformed       = "MALFORMED_REQUEST"
	ReasonLimitTooLarge   = "LIMIT_TOO_LARGE"
	ReasonBadPageToken    = "INVALID_PAGE_TOKEN"
	ReasonSessionExpired  = "SESSION_EXPIRED"
	ReasonSequenceStale   = "SEQUENCE_STALE"
	ReasonSessionWindow   = "SESSION_WINDOW_EXCEEDED"
	ReasonInternal        = "INTERNAL"
	ReasonReadTimeout     = "READ_INDEX_TIMEOUT"
	ReasonCorruption      = "STORAGE_CORRUPTION"

	// OutcomeKey is the ErrorInfo.metadata key for writes: DEFINITE means the
	// request was provably never proposed; INDETERMINATE means it may have
	// been (or may still be) applied.
	OutcomeKey    = "outcome"
	Definite      = "DEFINITE"
	Indeterminate = "INDETERMINATE"
)

// newError builds a gRPC status carrying ErrorInfo (and a LeaderHint when hint
// is non-nil). Clients branch on the reason, never on the message.
func newError(code codes.Code, reason, outcome, msg string, hint *raftkvv1.LeaderHint) error {
	info := &errdetails.ErrorInfo{Reason: reason, Domain: ErrorDomain}
	if outcome != "" {
		info.Metadata = map[string]string{OutcomeKey: outcome}
	}
	details := []protoadapt.MessageV1{info}
	if hint != nil {
		details = append(details, hint)
	}
	st, err := status.New(code, msg).WithDetails(details...)
	if err != nil {
		return status.Error(code, msg) // details are best effort
	}
	return st.Err()
}

// mapProposeError turns a failure to get a command proposed into a status. The
// core only returns these errors when nothing was appended, so they are
// DEFINITE; a context error is ambiguous (the proposal may have been queued)
// and is reported as OUTCOME_UNKNOWN.
func (s *KV) mapProposeError(err error) error {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		return newError(codes.Unavailable, ReasonNotLeader, Definite, "not the leader", s.hint(nl.Leader))
	case errors.Is(err, raft.ErrProposalDropped):
		return newError(codes.Unavailable, ReasonNoLeader, Definite, "no leader or proposal limit reached", nil)
	case errors.Is(err, node.ErrStopped), errors.Is(err, ErrTrackerClosed):
		return newError(codes.Unavailable, ReasonOutcomeUnknown, Indeterminate, "node is shutting down", nil)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return newError(codes.DeadlineExceeded, ReasonOutcomeUnknown, Indeterminate, "deadline passed while proposing", nil)
	default:
		return newError(codes.Internal, ReasonInternal, Indeterminate, err.Error(), nil)
	}
}

// mapWaitError turns a failure after a successful proposal into a status.
func mapWaitError(err error) error {
	switch {
	case errors.Is(err, ErrLeaderChanged):
		return newError(codes.Unavailable, ReasonOutcomeUnknown, Indeterminate, "leadership changed before apply", nil)
	case errors.Is(err, ErrTrackerClosed):
		return newError(codes.Unavailable, ReasonOutcomeUnknown, Indeterminate, "node is shutting down", nil)
	case errors.Is(err, context.DeadlineExceeded):
		return newError(codes.DeadlineExceeded, ReasonOutcomeUnknown, Indeterminate, "deadline passed before apply", nil)
	case errors.Is(err, context.Canceled):
		return newError(codes.Canceled, ReasonOutcomeUnknown, Indeterminate, "cancelled before apply", nil)
	default:
		return newError(codes.Internal, ReasonInternal, Indeterminate, err.Error(), nil)
	}
}

// mapReadError turns a failed read into a status (api-design 6.1). Reads have
// no side effects, so there is no outcome metadata and all but corruption are
// safe to retry, usually at another member.
func mapReadError(err error) error {
	var se *storageErr
	switch {
	case errors.As(err, &se):
		return newError(codes.DataLoss, ReasonCorruption, "", se.Error(), nil)
	case errors.Is(err, raft.ErrReadIndexDropped), errors.Is(err, node.ErrReadRejected):
		return newError(codes.Unavailable, ReasonNoLeader, "", "no leader to confirm the read", nil)
	case errors.Is(err, node.ErrStopped):
		return newError(codes.Unavailable, ReasonNoLeader, "", "node is shutting down", nil)
	case errors.Is(err, context.DeadlineExceeded):
		return newError(codes.DeadlineExceeded, ReasonReadTimeout, "", "read index not confirmed before the deadline", nil)
	case errors.Is(err, context.Canceled):
		return newError(codes.Canceled, ReasonReadTimeout, "", "cancelled", nil)
	default:
		return newError(codes.Internal, ReasonInternal, "", err.Error(), nil)
	}
}

// mapStaleWaitError is for a STALE read whose min_revision was not reached.
func mapStaleWaitError(err error) error {
	switch {
	case errors.Is(err, node.ErrStopped):
		return newError(codes.Unavailable, ReasonNoLeader, "", "node is shutting down", nil)
	case errors.Is(err, context.Canceled):
		return newError(codes.Canceled, ReasonRevisionMissing, "", "cancelled", nil)
	default:
		return newError(codes.DeadlineExceeded, ReasonRevisionMissing, "", "revision not reached", nil)
	}
}

// storageErr marks an error from the local store inside a read.
type storageErr struct{ err error }

func (e *storageErr) Error() string { return e.err.Error() }

// mapValidation turns a command validation error into INVALID_ARGUMENT, or a
// session rule into FAILED_PRECONDITION. Both are checked before proposing (or
// decided at apply time without side effects), so the outcome is DEFINITE.
func mapValidation(err error) error {
	reason := ReasonMalformed
	switch {
	case errors.Is(err, command.ErrSessionExpired):
		return newError(codes.FailedPrecondition, ReasonSessionExpired, Definite, err.Error(), nil)
	case errors.Is(err, command.ErrSequenceStale):
		return newError(codes.FailedPrecondition, ReasonSequenceStale, Definite, err.Error(), nil)
	case errors.Is(err, command.ErrSessionWindow):
		return newError(codes.FailedPrecondition, ReasonSessionWindow, Definite, err.Error(), nil)
	case errors.Is(err, command.ErrEmptyKey):
		reason = ReasonEmptyKey
	case errors.Is(err, command.ErrKeyTooLarge):
		reason = ReasonKeyTooLarge
	case errors.Is(err, command.ErrValueTooLarge):
		reason = ReasonValueTooLarge
	}
	return newError(codes.InvalidArgument, reason, Definite, err.Error(), nil)
}
