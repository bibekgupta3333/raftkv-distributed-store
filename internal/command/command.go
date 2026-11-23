// Package command is the envelope of a state machine command carried in a Raft
// log entry (raft.proto Command, WBS 3.2.1): request ID for the proposal
// tracker, the leader's timestamp, and one operation.
package command

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// Client key and value limits (D-11).
const (
	MaxKeySize   = 4096
	MaxValueSize = 1 << 20
)

// Validation errors; the server maps them to INVALID_ARGUMENT.
var (
	ErrEmptyKey      = errors.New("command: key is empty")
	ErrKeyTooLarge   = errors.New("command: key is larger than 4096 bytes")
	ErrValueTooLarge = errors.New("command: value is larger than 1 MiB")
	ErrNoOp          = errors.New("command: no operation set")
	// ErrMalformed covers a CompareAndSwap without a usable compare or action
	// (api-design 3.1: expected_mod_revision 0 is invalid, use expect_absent)
	// and a session with sequence 0.
	ErrMalformed = errors.New("command: malformed request")
)

// Kind names the operation inside a Command.
type Kind int

// Kinds of operation.
const (
	KindUnknown Kind = iota
	KindPut
	KindDelete
	KindGet
	KindRegisterSession
	KindCas
)

func (k Kind) String() string {
	switch k {
	case KindPut:
		return "put"
	case KindDelete:
		return "delete"
	case KindGet:
		return "get"
	case KindRegisterSession:
		return "register-session"
	case KindCas:
		return "cas"
	default:
		return "unknown"
	}
}

// NewID returns a random non-zero request ID. Uniqueness only matters on the
// proposing node's tracker, so randomness is enough.
func NewID() uint64 {
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("command: no randomness: %v", err))
		}
		if id := binary.LittleEndian.Uint64(b[:]); id != 0 {
			return id
		}
	}
}

// Put builds a validated Put command.
func Put(id uint64, leaderTimeMs int64, key, value []byte) (*raftkvv1.Command, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if len(value) > MaxValueSize {
		return nil, ErrValueTooLarge
	}
	return &raftkvv1.Command{RequestId: id, LeaderTimeUnixMs: leaderTimeMs,
		Op: &raftkvv1.Command_Put{Put: &raftkvv1.PutOp{Key: key, Value: value}}}, nil
}

// Delete builds a validated Delete command.
func Delete(id uint64, leaderTimeMs int64, key []byte) (*raftkvv1.Command, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	return &raftkvv1.Command{RequestId: id, LeaderTimeUnixMs: leaderTimeMs,
		Op: &raftkvv1.Command_Delete{Delete: &raftkvv1.DeleteOp{Key: key}}}, nil
}

// Get builds a validated linearizable-read-through-the-log command.
func Get(id uint64, leaderTimeMs int64, key []byte) (*raftkvv1.Command, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	return &raftkvv1.Command{RequestId: id, LeaderTimeUnixMs: leaderTimeMs,
		Op: &raftkvv1.Command_Get{Get: &raftkvv1.GetOp{Key: key}}}, nil
}

// RegisterSession builds a session registration (D-26). The applied entry's
// log index becomes the client ID.
func RegisterSession(id uint64, leaderTimeMs int64) *raftkvv1.Command {
	return &raftkvv1.Command{RequestId: id, LeaderTimeUnixMs: leaderTimeMs,
		Op: &raftkvv1.Command_RegisterSession{RegisterSession: &raftkvv1.RegisterSessionOp{}}}
}

// Cas builds a validated CompareAndSwap command from op (api-design 3.1).
func Cas(id uint64, leaderTimeMs int64, op *raftkvv1.CasOp) (*raftkvv1.Command, error) {
	if err := ValidateCas(op); err != nil {
		return nil, err
	}
	return &raftkvv1.Command{RequestId: id, LeaderTimeUnixMs: leaderTimeMs,
		Op: &raftkvv1.Command_Cas{Cas: op}}, nil
}

// ValidateCas checks a CasOp: key and value limits (D-11), exactly one
// compare and one action, no expected_mod_revision of 0, and no false
// expect_absent or delete flag.
func ValidateCas(op *raftkvv1.CasOp) error {
	if err := validateKey(op.GetKey()); err != nil {
		return err
	}
	switch e := op.GetExpected().(type) {
	case *raftkvv1.CasOp_ExpectedValue:
		if len(e.ExpectedValue) > MaxValueSize {
			return ErrValueTooLarge
		}
	case *raftkvv1.CasOp_ExpectedModRevision:
		if e.ExpectedModRevision == 0 {
			return fmt.Errorf("%w: expected_mod_revision 0 (use expect_absent)", ErrMalformed)
		}
	case *raftkvv1.CasOp_ExpectAbsent:
		if !e.ExpectAbsent {
			return fmt.Errorf("%w: expect_absent must be true when set", ErrMalformed)
		}
	default:
		return fmt.Errorf("%w: no compare set", ErrMalformed)
	}
	switch a := op.GetAction().(type) {
	case *raftkvv1.CasOp_NewValue:
		if len(a.NewValue) > MaxValueSize {
			return ErrValueTooLarge
		}
	case *raftkvv1.CasOp_Delete:
		if !a.Delete {
			return fmt.Errorf("%w: delete must be true when set", ErrMalformed)
		}
	default:
		return fmt.Errorf("%w: no action set", ErrMalformed)
	}
	return nil
}

// ValidateSession checks a write session before it is proposed (api-design
// 5): a nil session or client ID 0 means "no session"; otherwise the sequence
// starts at 1, the acked sequence is below it, and at most [SessionWindow]
// sequences are outstanding. The applier enforces the same rules.
func ValidateSession(s *raftkvv1.WriteSession) error {
	if s.GetClientId() == 0 {
		return nil
	}
	switch {
	case s.GetSequence() == 0:
		return fmt.Errorf("%w: session sequence starts at 1", ErrMalformed)
	case s.GetAckedSequence() >= s.GetSequence():
		return ErrSequenceStale
	case s.GetSequence()-s.GetAckedSequence() > SessionWindow:
		return ErrSessionWindow
	}
	return nil
}

// ValidateKey checks a key against D-11 (non-empty, at most 4 KiB).
func ValidateKey(key []byte) error { return validateKey(key) }

func validateKey(key []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > MaxKeySize:
		return ErrKeyTooLarge
	}
	return nil
}

// Marshal encodes c for a log entry's Data.
func Marshal(c *raftkvv1.Command) ([]byte, error) {
	if KindOf(c) == KindUnknown {
		return nil, ErrNoOp
	}
	return proto.Marshal(c)
}

// Unmarshal decodes a log entry's Data.
func Unmarshal(data []byte) (*raftkvv1.Command, error) {
	c := new(raftkvv1.Command)
	if err := proto.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("command: decode: %w", err)
	}
	return c, nil
}

// KindOf reports which operation c carries.
func KindOf(c *raftkvv1.Command) Kind {
	switch c.GetOp().(type) {
	case *raftkvv1.Command_Put:
		return KindPut
	case *raftkvv1.Command_Delete:
		return KindDelete
	case *raftkvv1.Command_Get:
		return KindGet
	case *raftkvv1.Command_RegisterSession:
		return KindRegisterSession
	case *raftkvv1.Command_Cas:
		return KindCas
	default:
		return KindUnknown
	}
}
