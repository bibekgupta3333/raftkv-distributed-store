package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Record layout (docs/storage-design.md §1.3), little-endian, 8-byte aligned:
//
//	0  4 length  payload length L (<= MaxPayload)
//	4  4 crc     CRC32C over length || type || reserved || term || index || payload
//	8  1 type
//	9  3 reserved (zero)
//	12 8 term
//	20 8 index
//	28 L payload
//	.. 0-7 zero padding to the next 8-byte boundary
const (
	recordHeaderSize = 28
	recordAlign      = 8

	// MaxPayload is the largest allowed record payload; larger is corruption.
	MaxPayload = 8 << 20
)

// RecordType identifies what a record holds. Unknown types are corruption,
// never skipped (docs/storage-design.md §1.4).
type RecordType uint8

// Record types.
const (
	RecordEntry     RecordType = 1 // term/index = entry term/index; payload = entry type + data
	RecordHardState RecordType = 2 // term = term, index = commit; payload = vote
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Decode errors. Both mean "this is not a valid record at this offset".
var (
	errTruncated = errors.New("wal: truncated record")
	errBadRecord = errors.New("wal: bad record")
)

// Record is the low-level unit written to a segment.
type Record struct {
	Type    RecordType
	Term    uint64
	Index   uint64
	Payload []byte
}

// EncodedLen is the number of bytes Encode appends, including padding.
func (r Record) EncodedLen() int {
	return alignUp(recordHeaderSize + len(r.Payload))
}

// Encode appends the encoded record to dst and returns the extended slice.
func (r Record) Encode(dst []byte) []byte {
	start := len(dst)
	total := r.EncodedLen()
	dst = append(dst, make([]byte, total)...)
	b := dst[start:]
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(r.Payload)))
	b[8] = byte(r.Type)
	binary.LittleEndian.PutUint64(b[12:20], r.Term)
	binary.LittleEndian.PutUint64(b[20:28], r.Index)
	copy(b[recordHeaderSize:], r.Payload)
	binary.LittleEndian.PutUint32(b[4:8], recordCRC(b[:recordHeaderSize+len(r.Payload)]))
	return dst
}

// DecodeRecord decodes one record from the front of b and returns it with the
// number of bytes consumed (including padding). The payload aliases b. Any
// problem (short buffer, bad length, bad type, bad CRC, non-zero reserved or
// padding bytes) is reported as an error; it never panics.
func DecodeRecord(b []byte) (Record, int, error) {
	if len(b) < recordHeaderSize {
		return Record{}, 0, errTruncated
	}
	l := binary.LittleEndian.Uint32(b[0:4])
	if l > MaxPayload {
		return Record{}, 0, fmt.Errorf("%w: length %d too large", errBadRecord, l)
	}
	end := recordHeaderSize + int(l)
	total := alignUp(end)
	if total > len(b) {
		return Record{}, 0, errTruncated
	}
	if got, want := recordCRC(b[:end]), binary.LittleEndian.Uint32(b[4:8]); got != want {
		return Record{}, 0, fmt.Errorf("%w: crc mismatch", errBadRecord)
	}
	t := RecordType(b[8])
	if t != RecordEntry && t != RecordHardState {
		return Record{}, 0, fmt.Errorf("%w: unknown type %d", errBadRecord, t)
	}
	if b[9] != 0 || b[10] != 0 || b[11] != 0 {
		return Record{}, 0, fmt.Errorf("%w: reserved bytes set", errBadRecord)
	}
	for _, p := range b[end:total] {
		if p != 0 {
			return Record{}, 0, fmt.Errorf("%w: non-zero padding", errBadRecord)
		}
	}
	return Record{
		Type:    t,
		Term:    binary.LittleEndian.Uint64(b[12:20]),
		Index:   binary.LittleEndian.Uint64(b[20:28]),
		Payload: b[recordHeaderSize:end],
	}, total, nil
}

// recordCRC covers length, type, reserved, term, index and payload (not the
// crc field itself). b is the unpadded record.
func recordCRC(b []byte) uint32 {
	c := crc32.Update(0, crcTable, b[0:4])
	return crc32.Update(c, crcTable, b[8:])
}

func alignUp(n int) int {
	return (n + recordAlign - 1) &^ (recordAlign - 1)
}

// Entry is a Raft log entry as stored by the WAL.
type Entry struct {
	Index uint64
	Term  uint64
	Type  uint32
	Data  []byte
}

// HardState is the Raft state that must survive restarts (D-03).
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// IsEmpty reports whether hs is the zero HardState.
func (hs HardState) IsEmpty() bool { return hs == HardState{} }

func (e Entry) record() Record {
	p := make([]byte, 4+len(e.Data))
	binary.LittleEndian.PutUint32(p, e.Type)
	copy(p[4:], e.Data)
	return Record{Type: RecordEntry, Term: e.Term, Index: e.Index, Payload: p}
}

func (hs HardState) record() Record {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint64(p, hs.Vote)
	return Record{Type: RecordHardState, Term: hs.Term, Index: hs.Commit, Payload: p}
}

// entryFromRecord copies the payload, so the result does not alias file data.
func entryFromRecord(r Record) (Entry, error) {
	if r.Type != RecordEntry || len(r.Payload) < 4 {
		return Entry{}, fmt.Errorf("%w: malformed entry record", errBadRecord)
	}
	return Entry{
		Index: r.Index,
		Term:  r.Term,
		Type:  binary.LittleEndian.Uint32(r.Payload),
		Data:  append([]byte(nil), r.Payload[4:]...),
	}, nil
}

func hardStateFromRecord(r Record) (HardState, error) {
	if r.Type != RecordHardState || len(r.Payload) != 8 {
		return HardState{}, fmt.Errorf("%w: malformed hard state record", errBadRecord)
	}
	return HardState{Term: r.Term, Vote: binary.LittleEndian.Uint64(r.Payload), Commit: r.Index}, nil
}
