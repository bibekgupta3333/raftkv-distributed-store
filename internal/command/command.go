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
)

// Kind names the operation inside a Command.
type Kind int

// Kinds of operation.
const (
	KindUnknown Kind = iota
	KindPut
	KindDelete
	KindGet
)

func (k Kind) String() string {
	switch k {
	case KindPut:
		return "put"
	case KindDelete:
		return "delete"
	case KindGet:
		return "get"
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
	default:
		return KindUnknown
	}
}
