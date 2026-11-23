package node

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/wal"
)

// DumpOptions select what [DumpWAL] prints.
type DumpOptions struct {
	// From and To bound the printed entries (inclusive). To == 0 means no
	// upper bound. The digest always covers the whole log.
	From, To uint64
	// MaxData is how many payload bytes to show per entry (default 32).
	MaxData int
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// DumpWAL prints the HardState and log entries stored in dataDir in a stable,
// diff-friendly format, ending with a digest of the whole log. Running it on
// two nodes and comparing the digests shows whether their logs agree (WBS
// 2.3.6, D-18).
//
// It opens the WAL, which repairs a torn tail exactly like a real start (D-33),
// so run it only against a stopped node's data directory.
func DumpWAL(out io.Writer, dataDir string, opts DumpOptions) error {
	if opts.MaxData <= 0 {
		opts.MaxData = 32
	}
	id, ok, err := ReadIdentity(dataDir)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("node: %s has no %s file; is it a raftkvd data directory?", dataDir, IdentityFile)
	}
	walDir := filepath.Join(dataDir, WALDir)
	if _, err := os.Stat(walDir); err != nil {
		return fmt.Errorf("node: no WAL in %s: %w", dataDir, err)
	}
	w, rec, err := wal.Open(walDir, wal.Options{ClusterID: id.ClusterID, MemberID: id.NodeID,
		OnFatal: func(error) {}})
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()

	_, _ = fmt.Fprintf(out, "node_id=%d cluster_id=%d\n", id.NodeID, id.ClusterID)
	_, _ = fmt.Fprintf(out, "hard_state term=%d vote=%d commit=%d\n", rec.HardState.Term, rec.HardState.Vote, rec.HardState.Commit)
	if rec.TruncatedBytes > 0 {
		_, _ = fmt.Fprintf(out, "torn_tail_cut_bytes=%d\n", rec.TruncatedBytes)
	}
	digest := crc32.New(castagnoli)
	var last uint64
	for _, e := range rec.Entries {
		writeDigest(digest, e)
		last = e.Index
		if e.Index < opts.From || (opts.To != 0 && e.Index > opts.To) {
			continue
		}
		data := e.Data
		suffix := ""
		if len(data) > opts.MaxData {
			data, suffix = data[:opts.MaxData], "..."
		}
		_, _ = fmt.Fprintf(out, "entry index=%d term=%d type=%s len=%d data=%q%s\n",
			e.Index, e.Term, raft.EntryType(e.Type), len(e.Data), data, suffix)
	}
	first := uint64(0)
	if len(rec.Entries) > 0 {
		first = rec.Entries[0].Index
	}
	_, _ = fmt.Fprintf(out, "summary entries=%d first_index=%d last_index=%d digest=%08x\n",
		len(rec.Entries), first, last, digest.Sum32())
	return nil
}

func writeDigest(h io.Writer, e wal.Entry) {
	var b [20]byte
	for i, v := range []uint64{e.Index, e.Term} {
		for j := range 8 {
			b[i*8+j] = byte(v >> (8 * j))
		}
	}
	b[16], b[17], b[18], b[19] = byte(e.Type), byte(e.Type>>8), byte(e.Type>>16), byte(e.Type>>24)
	_, _ = h.Write(b[:])
	_, _ = h.Write(e.Data)
}
