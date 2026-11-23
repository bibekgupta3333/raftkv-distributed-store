package linearizability

import (
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

// kvState is the state of the keys of one partition: the present keys and
// their values, sorted by key. Absent keys are not listed. A state is never
// modified in place; every write returns a new one.
type kvState struct {
	kvs []KVPair
}

func (s kvState) find(key string) (int, bool) {
	i := sort.Search(len(s.kvs), func(i int) bool { return s.kvs[i].Key >= key })
	return i, i < len(s.kvs) && s.kvs[i].Key == key
}

func (s kvState) get(key string) (string, bool) {
	if i, ok := s.find(key); ok {
		return s.kvs[i].Value, true
	}
	return "", false
}

func (s kvState) put(key, value string) kvState {
	i, ok := s.find(key)
	out := make([]KVPair, 0, len(s.kvs)+1)
	out = append(out, s.kvs[:i]...)
	out = append(out, KVPair{Key: key, Value: value})
	if ok {
		i++
	}
	return kvState{kvs: append(out, s.kvs[i:]...)}
}

func (s kvState) del(key string) kvState {
	i, ok := s.find(key)
	if !ok {
		return s
	}
	out := make([]KVPair, 0, len(s.kvs)-1)
	out = append(out, s.kvs[:i]...)
	return kvState{kvs: append(out, s.kvs[i+1:]...)}
}

// span returns the present keys in [start, end) (end "" = no end).
func (s kvState) span(start, end string) []KVPair {
	i, _ := s.find(start)
	j := len(s.kvs)
	if end != "" {
		j, _ = s.find(end)
	}
	if j < i {
		j = i
	}
	return s.kvs[i:j]
}

// KVModel is the sequential specification of the KV store (testing.md 3.3).
//
// Per key the state is absent or present(value); the initial state is absent.
//
//	put(v):        always succeeds; new = present(v)
//	delete:        always succeeds; new = absent. If the result says whether
//	               the key existed, that must match the state.
//	get:           matches the state (absent = not found)
//	cas(cmp, act): matches = (cmp absent: state absent; cmp value e: state
//	               present(e)). If it matches: swapped must be true and the
//	               returned key must be the new state; new = act applied.
//	               Otherwise swapped must be false and the returned current
//	               value must equal the state (a failed compare is an ordinary
//	               linearizable read); new = state. A revision compare is
//	               turned into a value compare when the history is converted
//	               (see [ConvertEvents]).
//	range page:    the keys present in the page's span [key, end), with their
//	               values, in order, are exactly the pairs the page returned.
//
// Writes with an unknown output (indeterminate) take the same transition with
// any output accepted; for a cas that is "applied iff the compare matches".
// Their open-ended return time lets the checker place them anywhere after
// their call, or last, which is the same as never applied.
//
// Partitioning. Linearizability is compositional: a history is linearizable
// iff each independent sub-history is (Herlihy and Wing). Single-key
// operations touch one key, so without ranges every key is its own partition.
// A range page is one atomic read of every key in its span (D-34: each page is
// linearizable on its own), so the model unions all keys a page's span covers
// (among the keys that appear anywhere in the history: a key never written or
// returned is absent throughout and needs no state) into one partition and
// checks the page against all of them at a single point. Keeping range spans
// inside small key groups keeps those partitions small.
func KVModel() porcupine.Model {
	return porcupine.Model{
		Partition: partition,
		Init:      func() any { return kvState{} },
		Step: func(state, input, output any) (bool, any) {
			return step(state.(kvState), input.(Input), output.(Output))
		},
		Equal: func(a, b any) bool { return slices.Equal(a.(kvState).kvs, b.(kvState).kvs) },
		Hash: func(state any) uint64 {
			h := fnv.New64a()
			for _, kv := range state.(kvState).kvs {
				_, _ = h.Write([]byte(kv.Key))
				_, _ = h.Write([]byte{0})
				_, _ = h.Write([]byte(kv.Value))
				_, _ = h.Write([]byte{1})
			}
			return h.Sum64()
		},
		DescribeOperation: func(input, output any) string { return describe(input.(Input), output.(Output)) },
		DescribeState: func(state any) string {
			s := state.(kvState)
			if len(s.kvs) == 0 {
				return "<all absent>"
			}
			parts := make([]string, len(s.kvs))
			for i, kv := range s.kvs {
				parts[i] = kv.Key + "=" + kv.Value
			}
			return strings.Join(parts, " ")
		},
	}
}

func step(st kvState, in Input, out Output) (bool, any) {
	cur, present := st.get(in.Key)
	switch in.F {
	case FPut:
		return true, st.put(in.Key, in.Value)
	case FDelete:
		if !out.Unknown && out.Deleted != nil && *out.Deleted != present {
			return false, st
		}
		return true, st.del(in.Key)
	case FGet:
		if out.Found != present || (present && out.Value != cur) {
			return false, st
		}
		return true, st
	case FCAS:
		matches := !present
		if in.Cmp != CmpAbsent {
			matches = present && cur == in.Expect
		}
		next := st
		if matches {
			if in.Del {
				next = st.del(in.Key)
			} else {
				next = st.put(in.Key, in.Value)
			}
		}
		if out.Unknown {
			return true, next
		}
		if out.Swapped != matches {
			return false, st
		}
		// After a swap the result is the key's new state; after a failed
		// compare it is the state that made the compare fail.
		want, wantFound := next.get(in.Key)
		if out.Found != wantFound || (wantFound && out.Value != want) {
			return false, st
		}
		return true, next
	case FRange:
		return slices.Equal(st.span(in.Key, in.End), out.KVs), st
	}
	return false, st
}

// partition splits a history into independent sub-histories (see [KVModel]).
func partition(history []porcupine.Operation) [][]porcupine.Operation {
	// The universe: every key an operation names or a page returned.
	seen := map[string]bool{}
	for _, o := range history {
		in := o.Input.(Input)
		if in.F != FRange {
			seen[in.Key] = true
		}
		for _, kv := range o.Output.(Output).KVs {
			seen[kv.Key] = true
		}
	}
	universe := make([]string, 0, len(seen))
	for k := range seen {
		universe = append(universe, k)
	}
	sort.Strings(universe)
	index := make(map[string]int, len(universe))
	for i, k := range universe {
		index[k] = i
	}

	// Union-find over keys, plus one extra node per range page that covers
	// no known key (it claims nothing and is checked on its own).
	parent := make([]int, len(universe), len(universe)+8)
	for i := range parent {
		parent[i] = i
	}
	root := func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]]
			i = parent[i]
		}
		return i
	}
	union := func(a, b int) { parent[root(a)] = root(b) }

	anchor := make([]int, len(history))
	for n, o := range history {
		in := o.Input.(Input)
		if in.F != FRange {
			anchor[n] = index[in.Key]
			continue
		}
		var members []int
		lo := sort.SearchStrings(universe, in.Key)
		hi := len(universe)
		if in.End != "" {
			hi = max(lo, sort.SearchStrings(universe, in.End))
		}
		for i := lo; i < hi; i++ {
			members = append(members, i)
		}
		for _, kv := range o.Output.(Output).KVs {
			members = append(members, index[kv.Key])
		}
		if len(members) == 0 {
			parent = append(parent, len(parent))
			anchor[n] = len(parent) - 1
			continue
		}
		for _, m := range members[1:] {
			union(m, members[0])
		}
		anchor[n] = members[0]
	}

	byRoot := map[int]int{} // root -> position in out
	var out [][]porcupine.Operation
	for n, o := range history {
		r := root(anchor[n])
		pos, ok := byRoot[r]
		if !ok {
			pos = len(out)
			byRoot[r] = pos
			out = append(out, nil)
		}
		out[pos] = append(out[pos], o)
	}
	return out
}

func describe(in Input, out Output) string {
	switch in.F {
	case FPut:
		return fmt.Sprintf("put(%s, %s)%s", in.Key, in.Value, unknown(out))
	case FDelete:
		res := ""
		if !out.Unknown && out.Deleted != nil {
			res = fmt.Sprintf(" -> deleted=%v", *out.Deleted)
		}
		return fmt.Sprintf("delete(%s)%s%s", in.Key, res, unknown(out))
	case FCAS:
		cmp := "absent"
		if in.Cmp != CmpAbsent {
			cmp = "== " + in.Expect
		}
		act := "set " + in.Value
		if in.Del {
			act = "delete"
		}
		res := unknown(out)
		if !out.Unknown {
			cur := "absent"
			if out.Found {
				cur = out.Value
			}
			res = fmt.Sprintf(" -> swapped=%v current=%s", out.Swapped, cur)
		}
		return fmt.Sprintf("cas(%s %s ? %s)%s", in.Key, cmp, act, res)
	case FRange:
		end := in.End
		if end == "" {
			end = "<end>"
		}
		parts := make([]string, len(out.KVs))
		for i, kv := range out.KVs {
			parts[i] = kv.Key + "=" + kv.Value
		}
		return fmt.Sprintf("range[%q, %q) -> {%s}", in.Key, end, strings.Join(parts, " "))
	default:
		if out.Found {
			return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
		}
		return fmt.Sprintf("get(%s) -> not found", in.Key)
	}
}

func unknown(o Output) string {
	if o.Unknown {
		return " (outcome unknown)"
	}
	return ""
}
