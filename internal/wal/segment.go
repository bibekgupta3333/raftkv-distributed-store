package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
)

// Segment header (docs/storage-design.md §1.2), 64 bytes, little-endian. The
// magic is "RKVWAL" followed by the format version as two ASCII digits.
const (
	segmentHeaderSize = 64
	segmentMagicStem  = "RKVWAL"
	// SegmentFormatVersion is the segment and record format this code writes
	// and reads (storage-design §1.2, WBS 7.5.2).
	SegmentFormatVersion = 1
	segmentMagic         = "RKVWAL01"
	segmentPrefix        = "wal-"
	segmentSuffix        = ".seg"
)

type segmentHeader struct {
	Seq        uint64
	FirstIndex uint64
	ClusterID  uint64
	MemberID   uint64
}

func (h segmentHeader) encode() []byte {
	b := make([]byte, segmentHeaderSize)
	copy(b[0:8], segmentMagic)
	binary.LittleEndian.PutUint64(b[8:16], h.Seq)
	binary.LittleEndian.PutUint64(b[16:24], h.FirstIndex)
	binary.LittleEndian.PutUint64(b[24:32], h.ClusterID)
	binary.LittleEndian.PutUint64(b[32:40], h.MemberID)
	binary.LittleEndian.PutUint32(b[60:64], crc32.Checksum(b[:60], crcTable))
	return b
}

func decodeSegmentHeader(b []byte) (segmentHeader, error) {
	if len(b) < segmentHeaderSize {
		return segmentHeader{}, errors.New("short segment header")
	}
	// The version is checked before the CRC: a newer format may lay out the
	// rest of the header differently, and "unsupported version" is the useful
	// error then, not "crc mismatch".
	if string(b[0:6]) != segmentMagicStem {
		return segmentHeader{}, errors.New("bad segment magic")
	}
	v, ok := parseMagicVersion(b[6:8])
	if !ok {
		return segmentHeader{}, errors.New("bad segment magic")
	}
	if v != SegmentFormatVersion {
		return segmentHeader{}, fmt.Errorf("%w: segment format version %d, this build reads version %d",
			ErrFormatVersion, v, SegmentFormatVersion)
	}
	if crc32.Checksum(b[:60], crcTable) != binary.LittleEndian.Uint32(b[60:64]) {
		return segmentHeader{}, errors.New("segment header crc mismatch")
	}
	for _, x := range b[40:60] {
		if x != 0 {
			return segmentHeader{}, errors.New("segment header reserved bytes set")
		}
	}
	return segmentHeader{
		Seq:        binary.LittleEndian.Uint64(b[8:16]),
		FirstIndex: binary.LittleEndian.Uint64(b[16:24]),
		ClusterID:  binary.LittleEndian.Uint64(b[24:32]),
		MemberID:   binary.LittleEndian.Uint64(b[32:40]),
	}, nil
}

// parseMagicVersion reads the two ASCII digits after the magic stem.
func parseMagicVersion(b []byte) (int, bool) {
	if len(b) != 2 || b[0] < '0' || b[0] > '9' || b[1] < '0' || b[1] > '9' {
		return 0, false
	}
	return int(b[0]-'0')*10 + int(b[1]-'0'), true
}

func segmentName(seq, firstIndex uint64) string {
	return fmt.Sprintf("%s%016x-%016x%s", segmentPrefix, seq, firstIndex, segmentSuffix)
}

func parseSegmentName(name string) (seq, firstIndex uint64, ok bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, 0, false
	}
	core := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix)
	if len(core) != 33 || core[16] != '-' {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(core, "%016x-%016x", &seq, &firstIndex); err != nil {
		return 0, 0, false
	}
	return seq, firstIndex, segmentName(seq, firstIndex) == name
}

// segInfo is the in-memory description of one segment file.
type segInfo struct {
	seq        uint64
	firstIndex uint64
	name       string
	maxIndex   uint64 // highest entry index stored in the file (0 if none)
}
