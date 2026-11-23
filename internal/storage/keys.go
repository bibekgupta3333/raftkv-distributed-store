package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Key layout (storage-design.md 3.1). A one-byte prefix separates namespaces so
// a client key can never collide with an internal key, and Range-style scans
// only ever iterate inside the user prefix.
const (
	prefixMeta    byte = 'm' // internal metadata: 'm' || name
	prefixSession byte = 's' // sessions (D-09, M4): 's' || client_id (8 B BE)
	prefixExpiry  byte = 't' // session expiry index (M4)
	prefixUser    byte = 'u' // user keys: 'u' || key
)

// SchemaVersion is the on-disk schema of the state machine. Open refuses a
// database with a different version instead of misreading it.
const SchemaVersion uint64 = 1

// Client key and value limits (D-11).
const (
	MaxKeySize   = 4096
	MaxValueSize = 1 << 20
)

// Metadata keys. The applied pair is written in every apply batch (D-04).
var (
	keyAppliedIndex  = metaKey("/appliedIndex")
	keyAppliedTerm   = metaKey("/appliedTerm")
	keySchemaVersion = metaKey("/schemaVersion")
)

func metaKey(name string) []byte { return append([]byte{prefixMeta}, name...) }

// UserKey returns the Pebble key for a client key.
func UserKey(k []byte) []byte { return append([]byte{prefixUser}, k...) }

// ClientKey inverts [UserKey]. ok is false if pk is not a user key.
func ClientKey(pk []byte) (k []byte, ok bool) {
	if len(pk) == 0 || pk[0] != prefixUser {
		return nil, false
	}
	return pk[1:], true
}

// SessionKey returns the Pebble key of a client session (used from M4).
func SessionKey(clientID uint64) []byte {
	k := make([]byte, 9)
	k[0] = prefixSession
	binary.BigEndian.PutUint64(k[1:], clientID)
	return k
}

// ExpiryKey returns the session expiry index key (used from M4).
func ExpiryKey(lastActiveUnixMs, clientID uint64) []byte {
	k := make([]byte, 17)
	k[0] = prefixExpiry
	binary.BigEndian.PutUint64(k[1:], lastActiveUnixMs)
	binary.BigEndian.PutUint64(k[9:], clientID)
	return k
}

// Value is a stored user value with its revision header.
type Value struct {
	// CreateRevision and ModRevision are the log indexes of the entries that
	// created and last modified the key; Version counts modifications since
	// creation (1 on create).
	CreateRevision, ModRevision, Version uint64
	Data                                 []byte
}

const valueHeaderSize = 24

// encode lays the value out as a fixed little-endian header followed by the
// data, so a Get needs no protobuf decode (storage-design 3.1).
func (v Value) encode() []byte {
	b := make([]byte, valueHeaderSize+len(v.Data))
	binary.LittleEndian.PutUint64(b[0:], v.CreateRevision)
	binary.LittleEndian.PutUint64(b[8:], v.ModRevision)
	binary.LittleEndian.PutUint64(b[16:], v.Version)
	copy(b[valueHeaderSize:], v.Data)
	return b
}

var errShortValue = errors.New("storage: user value shorter than its header")

func decodeValue(b []byte) (Value, error) {
	if len(b) < valueHeaderSize {
		return Value{}, fmt.Errorf("%w (%d bytes)", errShortValue, len(b))
	}
	return Value{
		CreateRevision: binary.LittleEndian.Uint64(b[0:]),
		ModRevision:    binary.LittleEndian.Uint64(b[8:]),
		Version:        binary.LittleEndian.Uint64(b[16:]),
		Data:           append([]byte(nil), b[valueHeaderSize:]...),
	}, nil
}

func putU64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func getU64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("storage: metadata value is %d bytes, want 8", len(b))
	}
	return binary.LittleEndian.Uint64(b), nil
}
