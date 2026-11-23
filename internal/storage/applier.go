package storage

import (
	"fmt"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Result is what applying one command produced. It is handed to the
// [ResultSink] so the node that proposed the command can answer its client.
type Result struct {
	// Index is the log index of the applied entry (the "revision").
	Index uint64
	Kind  command.Kind
	// Found: Get found the key; Delete removed an existing key; Put replaced
	// an existing key.
	Found bool
	// Value is the current value for Get and the new value for Put.
	Value Value
	// Previous is the value before a Put or Delete (valid when Found).
	Previous Value
}

// ResultSink receives the result of every applied command that carries a
// request ID. It is called from the apply goroutine after the batch is
// committed, and must not block.
type ResultSink interface {
	Applied(requestID uint64, r Result)
}

// Applier is the state machine: it decodes committed entries and applies them
// to a [Store] in one atomic batch per call (D-04). It satisfies node.Applier.
type Applier struct {
	store *Store
	sink  ResultSink
}

// NewApplier returns an Applier over store. sink may be nil.
func NewApplier(store *Store, sink ResultSink) *Applier {
	return &Applier{store: store, sink: sink}
}

// Store returns the underlying store.
func (a *Applier) Store() *Store { return a.store }

// Applied is the highest applied index; it seeds the Raft core on restart.
func (a *Applier) Applied() uint64 { return a.store.Applied() }

// Close closes the store (flushing first).
func (a *Applier) Close() error { return a.store.Close() }

type pendingResult struct {
	id uint64
	r  Result
}

// Apply applies entries (a contiguous range directly after Applied) in one
// batch: data changes and appliedIndex commit together (D-04). Entries that
// carry no command (the leader's no-op, config changes before M6) only advance
// appliedIndex. Apply is deterministic: no clock, no randomness, no map
// iteration. Any error is fatal to the node (D-33).
func (a *Applier) Apply(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	b, err := a.store.NewApplyBatch()
	if err != nil {
		return err
	}
	var results []pendingResult
	for _, e := range entries {
		if err := b.Next(e.Index, e.Term); err != nil {
			b.Abort()
			return err
		}
		if e.Type != raft.EntryNormal || len(e.Data) == 0 {
			continue
		}
		pr, ok, err := a.applyOne(b, e)
		if err != nil {
			b.Abort()
			return fmt.Errorf("storage: apply entry %d: %w", e.Index, err)
		}
		if ok {
			results = append(results, pr)
		}
	}
	if err := b.Commit(); err != nil {
		return err
	}
	if a.sink != nil {
		for _, pr := range results {
			a.sink.Applied(pr.id, pr.r)
		}
	}
	return nil
}

// CheckProposal reports whether data is a command Apply accepts, so a raw
// payload can be refused before it is proposed instead of becoming a poison
// pill in the log (node.ProposalChecker; used by the debug Propose RPC).
func (a *Applier) CheckProposal(data []byte) error {
	cmd, err := command.Unmarshal(data)
	if err != nil {
		return err
	}
	switch op := cmd.GetOp().(type) {
	case *raftkvv1.Command_Put:
		_, err = command.Put(0, 0, op.Put.GetKey(), op.Put.GetValue())
	case *raftkvv1.Command_Delete:
		_, err = command.Delete(0, 0, op.Delete.GetKey())
	case *raftkvv1.Command_Get:
		_, err = command.Get(0, 0, op.Get.GetKey())
	default:
		err = command.ErrNoOp
	}
	return err
}

func (a *Applier) applyOne(b *ApplyBatch, e raft.Entry) (pendingResult, bool, error) {
	cmd, err := command.Unmarshal(e.Data)
	if err != nil {
		return pendingResult{}, false, err
	}
	res := Result{Index: e.Index, Kind: command.KindOf(cmd)}
	switch op := cmd.GetOp().(type) {
	case *raftkvv1.Command_Put:
		// Validated at propose time; a bad entry would be a poison pill, so
		// re-check and fail loudly rather than diverge.
		if _, verr := command.Put(0, 0, op.Put.GetKey(), op.Put.GetValue()); verr != nil {
			return pendingResult{}, false, verr
		}
		cur, prev, had, err := b.Put(op.Put.GetKey(), op.Put.GetValue())
		if err != nil {
			return pendingResult{}, false, err
		}
		res.Value, res.Previous, res.Found = cur, prev, had
	case *raftkvv1.Command_Delete:
		prev, existed, err := b.Delete(op.Delete.GetKey())
		if err != nil {
			return pendingResult{}, false, err
		}
		res.Previous, res.Found = prev, existed
	case *raftkvv1.Command_Get:
		v, found, err := b.Get(op.Get.GetKey())
		if err != nil {
			return pendingResult{}, false, err
		}
		res.Value, res.Found = v, found
	default:
		return pendingResult{}, false, command.ErrNoOp
	}
	return pendingResult{id: cmd.GetRequestId(), r: res}, cmd.GetRequestId() != 0, nil
}
