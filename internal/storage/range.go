package storage

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// Range page limits (D-11).
const (
	MaxRangeLimit = 1000
	MaxRangeBytes = 4 << 20
)

// RangeOptions select one page of a scan over user keys.
type RangeOptions struct {
	// Start is inclusive; empty means the first key. End is exclusive; empty
	// means past the last key.
	Start, End []byte
	// Limit is the most keys in the page (1..MaxRangeLimit).
	Limit int
	// MaxBytes cuts the page early once the keys and values returned reach
	// it (default MaxRangeBytes). At least one pair is always returned.
	MaxBytes int
	KeysOnly bool
	Reverse  bool
}

// KV is one key with its stored value.
type KV struct {
	Key   []byte
	Value Value
}

// Range reads one page of user keys in [Start, End) from committed state.
// One Pebble iterator serves the whole page, so the page is a consistent
// snapshot (D-34: a page is linearizable when read after a linearizable read
// point; separate pages are not one snapshot). more is true if keys remain in
// the range after the page.
func (s *Store) Range(o RangeOptions) (kvs []KV, more bool, err error) {
	if o.Limit <= 0 || o.Limit > MaxRangeLimit {
		o.Limit = MaxRangeLimit
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = MaxRangeBytes
	}
	lower := UserKey(o.Start)
	upper := []byte{prefixUser + 1}
	if len(o.End) > 0 {
		upper = UserKey(o.End)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, false, err
	}
	first, next := it.First, it.Next
	if o.Reverse {
		first, next = it.Last, it.Prev
	}
	size := 0
	for ok := first(); ok; ok = next() {
		key, _ := ClientKey(it.Key())
		if len(kvs) == o.Limit {
			more = true
			break
		}
		kv := KV{Key: append([]byte(nil), key...)}
		v, verr := decodeValue(it.Value())
		if verr != nil {
			_ = it.Close()
			return nil, false, verr
		}
		if o.KeysOnly {
			v.Data = nil
		}
		kv.Value = v
		n := len(kv.Key) + len(v.Data)
		if len(kvs) > 0 && size+n > o.MaxBytes {
			more = true
			break
		}
		size += n
		kvs = append(kvs, kv)
	}
	return kvs, more, errors.Join(it.Error(), it.Close())
}
