package linearizability

import (
	"fmt"
	"hash/fnv"

	"github.com/anishathalye/porcupine"
)

// kvState is one key's state: absent or present(value).
type kvState struct {
	Present bool
	Value   string
}

// KVModel is the sequential specification of the KV store, split per key
// (testing.md 3.3). Linearizability is compositional, so checking every key on
// its own is sound for single-key operations.
//
//	put(v):  always succeeds; state = present(v)
//	delete:  always succeeds; state = absent
//	get:     matches the state (absent = not found)
//
// Writes with an unknown output (indeterminate) step the same way; their
// open-ended return time lets the checker place them anywhere, or last.
func KVModel() porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := map[string][]porcupine.Operation{}
			var keys []string
			for _, o := range history {
				k := o.Input.(Input).Key
				if _, seen := byKey[k]; !seen {
					keys = append(keys, k)
				}
				byKey[k] = append(byKey[k], o)
			}
			out := make([][]porcupine.Operation, 0, len(keys))
			for _, k := range keys {
				out = append(out, byKey[k])
			}
			return out
		},
		Init: func() any { return kvState{} },
		Step: func(state, input, output any) (bool, any) {
			st, in, out := state.(kvState), input.(Input), output.(Output)
			switch in.F {
			case FPut:
				return true, kvState{Present: true, Value: in.Value}
			case FDelete:
				return true, kvState{}
			case FGet:
				if out.Found != st.Present || (st.Present && out.Value != st.Value) {
					return false, st
				}
				return true, st
			}
			return false, st
		},
		Equal: func(a, b any) bool { return a.(kvState) == b.(kvState) },
		Hash: func(state any) uint64 {
			s := state.(kvState)
			h := fnv.New64a()
			if s.Present {
				_, _ = h.Write([]byte{1})
			}
			_, _ = h.Write([]byte(s.Value))
			return h.Sum64()
		},
		DescribeOperation: func(input, output any) string {
			in, out := input.(Input), output.(Output)
			switch in.F {
			case FPut:
				return fmt.Sprintf("put(%s, %s)%s", in.Key, in.Value, unknown(out))
			case FDelete:
				return fmt.Sprintf("delete(%s)%s", in.Key, unknown(out))
			default:
				if out.Found {
					return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
				}
				return fmt.Sprintf("get(%s) -> not found", in.Key)
			}
		},
		DescribeState: func(state any) string {
			if s := state.(kvState); s.Present {
				return s.Value
			}
			return "<absent>"
		},
	}
}

func unknown(o Output) string {
	if o.Unknown {
		return " (outcome unknown)"
	}
	return ""
}
