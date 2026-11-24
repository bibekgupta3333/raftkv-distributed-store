package snapshot

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

const (
	// FormatVersion is the snapshot layout this code writes and reads: a
	// Pebble checkpoint of the storage.SchemaVersion state machine.
	FormatVersion = 1
	// MetaFile holds the snapshot's metadata inside its directory.
	MetaFile = "snapshot.meta"
	// ChunkSize is the largest chunk of file data in one InstallSnapshot
	// message (D-14).
	ChunkSize = 1 << 20
)

// Errors about snapshot contents.
var (
	// ErrChecksum means metadata or file data does not match its CRC32C.
	ErrChecksum = errors.New("snapshot: checksum mismatch")
	// ErrFormat means the snapshot was written in an unknown format.
	ErrFormat = errors.New("snapshot: unsupported format version")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Meta is a snapshot's metadata (D-14): last_included_index/term, the cluster
// configuration, the format version, the files with their CRC32C, and a
// checksum over all of it. On the wire, leader_id and term are filled in per
// transfer; they are not covered by the checksum.
type Meta = raftkvv1.SnapshotMetadata

// Checksum is the CRC32C of m's deterministic encoding with checksum,
// leader_id and term zeroed.
func Checksum(m *Meta) uint32 {
	c, _ := proto.Clone(m).(*Meta)
	c.Checksum, c.LeaderId, c.Term = 0, 0, 0
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		panic(fmt.Sprintf("snapshot: marshal metadata: %v", err)) // only on invalid UTF-8, which we never write
	}
	return crc32.Checksum(b, castagnoli)
}

// Seal stamps the format version and the checksum on m.
func Seal(m *Meta) {
	m.FormatVersion = FormatVersion
	m.Checksum = Checksum(m)
}

// Verify checks m's format version, checksum and file names.
func Verify(m *Meta) error {
	if m.GetFormatVersion() != FormatVersion {
		return fmt.Errorf("%w: %d", ErrFormat, m.GetFormatVersion())
	}
	if got := Checksum(m); got != m.GetChecksum() {
		return fmt.Errorf("%w: metadata crc %08x, want %08x", ErrChecksum, got, m.GetChecksum())
	}
	if m.GetLastIncludedIndex() == 0 {
		return errors.New("snapshot: metadata has index 0")
	}
	for _, f := range m.GetFiles() {
		if n := f.GetName(); n == "" || n == MetaFile || filepath.Base(n) != n || n == "." || n == ".." {
			return fmt.Errorf("snapshot: bad file name %q", n)
		}
	}
	return nil
}

// WriteMeta writes m (sealed) to dir/snapshot.meta and fsyncs it.
func WriteMeta(dir string, m *Meta) error {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return err
	}
	return storage.WriteFileSync(filepath.Join(dir, MetaFile), b)
}

// ReadMeta reads and verifies dir/snapshot.meta.
func ReadMeta(dir string) (*Meta, error) {
	b, err := os.ReadFile(filepath.Join(dir, MetaFile))
	if err != nil {
		return nil, err
	}
	m, err := DecodeMeta(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", dir, err)
	}
	return m, nil
}

// DecodeMeta decodes and verifies the bytes of a snapshot.meta file. Bytes
// that are not a SnapshotMetadata are ErrChecksum; a well-formed record of
// another format version is ErrFormat (WBS 7.5.2).
func DecodeMeta(b []byte) (*Meta, error) {
	m := &Meta{}
	if err := proto.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrChecksum, err)
	}
	if err := Verify(m); err != nil {
		return nil, err
	}
	return m, nil
}

// fileCRC returns the size and CRC32C of a file.
func fileCRC(path string) (int64, uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()
	h := crc32.New(castagnoli)
	n, err := io.Copy(h, f)
	return n, h.Sum32(), err
}

// syncFile fsyncs one file.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
