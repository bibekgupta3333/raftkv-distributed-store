package snapshot

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// ErrPosition means a chunk arrived out of order: wrong file, wrong offset,
// past the end of its file, or a stream that ended early (gRPC
// INVALID_ARGUMENT). Checksum failures wrap [ErrChecksum] (DATA_LOSS).
var ErrPosition = errors.New("snapshot: chunk out of position")

// SendFunc sends one InstallSnapshot message on the stream.
type SendFunc func(*raftkvv1.InstallSnapshotRequest) error

// RecvFunc receives the next InstallSnapshot message; io.EOF ends the stream.
type RecvFunc func() (*raftkvv1.InstallSnapshotRequest, error)

// Send streams the newest local snapshot at or above minIndex (D-14,
// storage-design 4.2): first the metadata (with leaderID and term filled in),
// then every file in list order in chunks of at most ChunkSize bytes, each with
// its CRC32C; the final chunk has last set. The snapshot is pinned against
// retention while it is read. It returns the metadata that was sent.
func (m *Manager) Send(minIndex, leaderID, term uint64, send SendFunc) (*Meta, error) {
	meta, release, err := m.acquire(minIndex)
	if err != nil {
		return nil, err
	}
	defer release()
	wire, _ := proto.Clone(meta).(*Meta)
	wire.LeaderId, wire.Term = leaderID, term
	if err := send(&raftkvv1.InstallSnapshotRequest{Msg: &raftkvv1.InstallSnapshotRequest_Metadata{Metadata: wire}}); err != nil {
		return nil, err
	}
	dir := m.Dir(meta)
	files := meta.GetFiles()
	buf := make([]byte, ChunkSize)
	for i, f := range files {
		if err := sendFile(filepath.Join(dir, f.GetName()), uint32(i), f.GetSize(), i == len(files)-1, buf, send); err != nil {
			return nil, err
		}
	}
	if len(files) == 0 {
		if err := send(chunkReq(&raftkvv1.SnapshotChunk{Last: true})); err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func chunkReq(c *raftkvv1.SnapshotChunk) *raftkvv1.InstallSnapshotRequest {
	return &raftkvv1.InstallSnapshotRequest{Msg: &raftkvv1.InstallSnapshotRequest_Chunk{Chunk: c}}
}

func sendFile(path string, idx uint32, size uint64, lastFile bool, buf []byte, send SendFunc) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var off uint64
	for {
		n, rerr := io.ReadFull(f, buf[:min(uint64(len(buf)), size-off)])
		if rerr != nil && !errors.Is(rerr, io.ErrUnexpectedEOF) && !errors.Is(rerr, io.EOF) {
			return rerr
		}
		data := buf[:n]
		done := off+uint64(n) >= size
		c := &raftkvv1.SnapshotChunk{
			FileIndex: idx, Offset: off, Data: data,
			Crc32C: crc32.Checksum(data, castagnoli), Last: done && lastFile,
		}
		if err := send(chunkReq(c)); err != nil {
			return err
		}
		off += uint64(n)
		if done {
			return nil
		}
		if n == 0 {
			return fmt.Errorf("snapshot: %s shorter than its recorded size %d", path, size)
		}
	}
}

// Staged is a fully received and verified snapshot waiting to be installed.
type Staged struct {
	Dir  string
	Meta *Meta
}

// Receive stages an inbound snapshot whose metadata already arrived (D-14,
// storage-design 4.2): it writes the chunks to snap/incoming/<index>-<term>/,
// checking each chunk's position (ErrPosition) and CRC32C (ErrChecksum) and
// each completed file's size and CRC32C, then fsyncs the files and the
// directory. On any error, including a broken stream, the staging directory
// is deleted: there is no resume, the leader starts over (D-14).
func (m *Manager) Receive(meta *Meta, recv RecvFunc) (*Staged, error) {
	if err := Verify(meta); err != nil {
		return nil, err
	}
	if meta.GetClusterId() != m.cfg.ClusterID {
		return nil, fmt.Errorf("snapshot: cluster id %d, want %d", meta.GetClusterId(), m.cfg.ClusterID)
	}
	root := filepath.Join(m.cfg.Dir, incomingDir)
	dir := filepath.Join(root, dirName(meta))
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := receiveFiles(dir, meta, recv); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	stored, _ := proto.Clone(meta).(*Meta)
	stored.LeaderId, stored.Term = 0, 0
	if err := WriteMeta(dir, stored); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if err := storage.SyncDir(dir); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if err := storage.SyncDir(root); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return &Staged{Dir: dir, Meta: stored}, nil
}

type fileWriter struct {
	f    *os.File
	h    hash.Hash32
	spec *raftkvv1.SnapshotFile
	off  uint64
}

func (w *fileWriter) finish() error {
	if w.off != w.spec.GetSize() {
		return fmt.Errorf("%w: file %s ended at %d, want %d bytes", ErrPosition, w.spec.GetName(), w.off, w.spec.GetSize())
	}
	if got := w.h.Sum32(); got != w.spec.GetCrc32C() {
		_ = w.f.Close()
		return fmt.Errorf("%w: file %s crc %08x, want %08x", ErrChecksum, w.spec.GetName(), got, w.spec.GetCrc32C())
	}
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

func receiveFiles(dir string, meta *Meta, recv RecvFunc) (err error) {
	files := meta.GetFiles()
	var cur *fileWriter
	defer func() {
		if cur != nil && err != nil {
			_ = cur.f.Close()
		}
	}()
	fi := 0
	open := func() error {
		f, err := os.OpenFile(filepath.Join(dir, files[fi].GetName()), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		cur = &fileWriter{f: f, h: crc32.New(castagnoli), spec: files[fi]}
		return nil
	}
	if len(files) > 0 {
		if err := open(); err != nil {
			return err
		}
	}
	for {
		req, err := recv()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: stream ended before the last chunk", ErrPosition)
		}
		if err != nil {
			return err
		}
		c := req.GetChunk()
		if c == nil {
			return fmt.Errorf("%w: expected a chunk", ErrPosition)
		}
		if fi < len(files) {
			if int(c.GetFileIndex()) != fi || c.GetOffset() != cur.off {
				return fmt.Errorf("%w: got file %d offset %d, want file %d offset %d",
					ErrPosition, c.GetFileIndex(), c.GetOffset(), fi, cur.off)
			}
			if crc32.Checksum(c.GetData(), castagnoli) != c.GetCrc32C() {
				return fmt.Errorf("%w: chunk of file %d at offset %d", ErrChecksum, fi, c.GetOffset())
			}
			if cur.off+uint64(len(c.GetData())) > cur.spec.GetSize() {
				return fmt.Errorf("%w: chunk runs past the end of file %s", ErrPosition, cur.spec.GetName())
			}
			if _, err := cur.f.Write(c.GetData()); err != nil {
				return err
			}
			_, _ = cur.h.Write(c.GetData())
			cur.off += uint64(len(c.GetData()))
			if cur.off == cur.spec.GetSize() {
				if err := cur.finish(); err != nil {
					cur = nil
					return err
				}
				cur = nil
				fi++
				if fi < len(files) && !c.GetLast() {
					if err := open(); err != nil {
						return err
					}
				}
			}
		} else if len(c.GetData()) > 0 {
			return fmt.Errorf("%w: data after the last file", ErrPosition)
		}
		if c.GetLast() {
			if fi != len(files) {
				return fmt.Errorf("%w: last chunk before file %d of %d", ErrPosition, fi, len(files))
			}
			return nil
		}
	}
}

// Discard deletes a staged snapshot that will not be installed (stale, or
// the install was refused).
func (m *Manager) Discard(s *Staged) {
	if s != nil {
		_ = os.RemoveAll(s.Dir)
	}
}
