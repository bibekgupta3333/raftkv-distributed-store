package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
)

// Session table (D-09, D-26, D-27; storage-design 3.1). Everything here is
// staged in the apply batch, so a session change commits atomically with the
// data change it guards and with appliedIndex (D-04). Nothing reads a local
// clock: "now" is the leader-stamped time carried in each command.
//
//	s ‖ client_id                → session record (24 bytes, below)
//	s ‖ client_id ‖ sequence     → cached reply for that sequence
//	t ‖ last_active ‖ client_id  → empty; expiry index in time order
//	m/sessionClock               → highest leader time applied so far
//
// The clock is the maximum of every stamped time, so it never goes backwards
// even when a new leader's wall clock is behind the old one's.

var keySessionClock = metaKey("/sessionClock")

var sessionTTLMs = uint64(command.SessionTTL.Milliseconds())

// session is the fixed part of a session record.
type session struct {
	// lastSeq is the highest sequence executed; acked is the highest
	// acked_sequence seen (replies at or below it are dropped); lastActive is
	// the session clock at the last registration or write.
	lastSeq, acked, lastActive uint64
}

func (s session) encode() []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:], s.lastSeq)
	binary.LittleEndian.PutUint64(b[8:], s.acked)
	binary.LittleEndian.PutUint64(b[16:], s.lastActive)
	return b
}

func decodeSession(b []byte) (session, error) {
	if len(b) != 24 {
		return session{}, fmt.Errorf("storage: session record is %d bytes, want 24", len(b))
	}
	return session{
		lastSeq:    binary.LittleEndian.Uint64(b[0:]),
		acked:      binary.LittleEndian.Uint64(b[8:]),
		lastActive: binary.LittleEndian.Uint64(b[16:]),
	}, nil
}

// replyKey is the key of a cached reply: s ‖ client_id ‖ sequence. It sorts
// directly after the session record of the same client.
func replyKey(clientID, seq uint64) []byte {
	k := make([]byte, 17)
	k[0] = prefixSession
	binary.BigEndian.PutUint64(k[1:], clientID)
	binary.BigEndian.PutUint64(k[9:], seq)
	return k
}

// ---------------------------------------------------------------- batch ops

func (a *ApplyBatch) getRaw(key []byte) ([]byte, bool, error) {
	v, c, err := a.b.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = c.Close() }()
	return append([]byte(nil), v...), true, nil
}

// advanceClock folds a leader-stamped time into the session clock and
// returns the new clock (D-27). Negative stamps count as 0.
func (a *ApplyBatch) advanceClock(stampMs int64) (uint64, error) {
	raw, found, err := a.getRaw(keySessionClock)
	if err != nil {
		return 0, err
	}
	var cur uint64
	if found {
		if cur, err = getU64(raw); err != nil {
			return 0, err
		}
	}
	if stampMs <= 0 || uint64(stampMs) <= cur {
		return cur, nil
	}
	return uint64(stampMs), a.b.Set(keySessionClock, putU64(uint64(stampMs)), nil)
}

// expireSessions removes every session idle for longer than the TTL at clock
// now, oldest first, by walking the expiry index in key order (deterministic).
func (a *ApplyBatch) expireSessions(now uint64) error {
	if now <= sessionTTLMs {
		return nil
	}
	// Expired: lastActive < now - TTL, i.e. idle for more than the TTL.
	upper := ExpiryKey(now-sessionTTLMs, 0)
	it, err := a.b.NewIter(&pebble.IterOptions{LowerBound: []byte{prefixExpiry}, UpperBound: upper})
	if err != nil {
		return err
	}
	var ids []uint64
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) != 17 {
			_ = it.Close()
			return fmt.Errorf("storage: bad expiry key %x", k)
		}
		ids = append(ids, binary.BigEndian.Uint64(k[9:]))
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	for _, id := range ids {
		if err := a.deleteSession(id); err != nil {
			return err
		}
	}
	return nil
}

func (a *ApplyBatch) getSession(clientID uint64) (session, bool, error) {
	raw, found, err := a.getRaw(SessionKey(clientID))
	if err != nil || !found {
		return session{}, false, err
	}
	s, err := decodeSession(raw)
	return s, err == nil, err
}

// putSession writes rec and moves its expiry index entry from oldActive.
func (a *ApplyBatch) putSession(clientID uint64, rec session, oldActive uint64, existed bool) error {
	if existed && oldActive != rec.lastActive {
		if err := a.b.Delete(ExpiryKey(oldActive, clientID), nil); err != nil {
			return err
		}
	}
	if !existed || oldActive != rec.lastActive {
		if err := a.b.Set(ExpiryKey(rec.lastActive, clientID), nil, nil); err != nil {
			return err
		}
	}
	return a.b.Set(SessionKey(clientID), rec.encode(), nil)
}

// deleteSession removes a session, all its cached replies and its expiry
// index entry.
func (a *ApplyBatch) deleteSession(clientID uint64) error {
	rec, found, err := a.getSession(clientID)
	if err != nil || !found {
		return err
	}
	if err := a.dropReplies(clientID, 0, ^uint64(0)); err != nil {
		return err
	}
	if err := a.b.Delete(ExpiryKey(rec.lastActive, clientID), nil); err != nil {
		return err
	}
	return a.b.Delete(SessionKey(clientID), nil)
}

// dropReplies deletes cached replies with from <= seq <= to.
func (a *ApplyBatch) dropReplies(clientID, from, to uint64) error {
	lower := replyKey(clientID, from)
	var upper []byte
	if to == ^uint64(0) {
		upper = SessionKey(clientID + 1)
	} else {
		upper = replyKey(clientID, to+1)
	}
	it, err := a.b.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	var keys [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	for _, k := range keys {
		if err := a.b.Delete(k, nil); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------- replies

// A cached reply is the part of a Result the server needs to answer a retry:
//
//	kind u8 | flags u8 (1 found, 2 succeeded) | index u64 LE |
//	value: u32 LE length + encoded Value (0 = none) | previous: same
func encodeReply(r Result) []byte {
	var flags byte
	if r.Found {
		flags |= 1
	}
	if r.Succeeded {
		flags |= 2
	}
	var buf bytes.Buffer
	buf.WriteByte(byte(r.Kind))
	buf.WriteByte(flags)
	_ = binary.Write(&buf, binary.LittleEndian, r.Index)
	for _, v := range []Value{r.Value, r.Previous} {
		if v.ModRevision == 0 { // every stored value has a revision >= 1

			_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
			continue
		}
		enc := v.encode()
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(enc)))
		buf.Write(enc)
	}
	return buf.Bytes()
}

var errBadReply = errors.New("storage: malformed cached reply")

func decodeReply(b []byte) (Result, error) {
	if len(b) < 10 {
		return Result{}, errBadReply
	}
	r := Result{Kind: command.Kind(b[0]), Found: b[1]&1 != 0, Succeeded: b[1]&2 != 0}
	r.Index = binary.LittleEndian.Uint64(b[2:])
	rest := b[10:]
	for _, dst := range []*Value{&r.Value, &r.Previous} {
		if len(rest) < 4 {
			return Result{}, errBadReply
		}
		n := binary.LittleEndian.Uint32(rest)
		rest = rest[4:]
		if n == 0 {
			continue
		}
		if uint32(len(rest)) < n {
			return Result{}, errBadReply
		}
		v, err := decodeValue(rest[:n])
		if err != nil {
			return Result{}, err
		}
		*dst = v
		rest = rest[n:]
	}
	if len(rest) != 0 {
		return Result{}, errBadReply
	}
	return r, nil
}

// ---------------------------------------------------------------- reads

// SessionClock is the highest leader-stamped time applied so far (unix ms,
// D-27). A new leader stamps at least this, so session time never goes
// backwards across leader changes.
func (s *Store) SessionClock() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, _, err := s.getMeta(keySessionClock)
	if err != nil {
		return 0
	}
	return v
}

// HasSession reports whether a session exists in committed state.
func (s *Store) HasSession(clientID uint64) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, c, err := s.db.Get(SessionKey(clientID))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, c.Close()
}

// SessionHash hashes the whole session table: records, cached replies, the
// expiry index and the session clock. Replicas at the same applied index must
// agree on it (WBS 4.3.3, 4.3.6). It also returns the number of sessions.
func (s *Store) SessionHash() (sessions int, sum [sha256.Size]byte, err error) {
	h := sha256.New()
	var lenBuf [8]byte
	write := func(b []byte) {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(b)))
		h.Write(lenBuf[:])
		h.Write(b)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	clock, _, err := s.getMeta(keySessionClock)
	if err != nil {
		return 0, sum, err
	}
	write(putU64(clock))
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{prefixSession}, UpperBound: []byte{prefixExpiry + 1}})
	if err != nil {
		return 0, sum, err
	}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) == 9 && k[0] == prefixSession {
			sessions++
		}
		write(k)
		write(it.Value())
	}
	err = errors.Join(it.Error(), it.Close())
	copy(sum[:], h.Sum(nil))
	return sessions, sum, err
}
