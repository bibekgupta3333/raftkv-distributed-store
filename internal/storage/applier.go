package storage

import (
	"bytes"
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
	// an existing key; CAS: the key exists after evaluation.
	Found bool
	// Value is the current value for Get, the new value for Put, and for CAS
	// the value after a successful swap or the conflicting one on failure.
	Value Value
	// Previous is the value before a Put or Delete (valid when Found).
	Previous Value
	// Succeeded reports whether a CAS compare matched (and its action ran).
	Succeeded bool
	// ClientID is the session a RegisterSession created (its log index, D-26).
	ClientID uint64
	// Duplicate means the (client_id, sequence) had already been executed and
	// this is its cached reply, not a new execution (D-09). Index is then the
	// index of the original entry.
	Duplicate bool
	// Err is a session outcome decided at apply time (command.ErrSessionExpired,
	// ErrSequenceStale, ErrSessionWindow). The command changed no user data.
	Err error
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
	case *raftkvv1.Command_Cas:
		err = command.ValidateCas(op.Cas)
	case *raftkvv1.Command_RegisterSession:
	default:
		err = command.ErrNoOp
	}
	if err != nil {
		return err
	}
	return command.ValidateSession(cmd.GetSession())
}

// applyOne applies one command. Order (system-design 4.2, D-27): fold the
// leader-stamped time into the session clock, expire idle sessions at that
// clock, then run the operation, through the session dedup table when the
// command carries a session. Everything lands in the same batch as
// appliedIndex (D-04).
func (a *Applier) applyOne(b *ApplyBatch, e raft.Entry) (pendingResult, bool, error) {
	cmd, err := command.Unmarshal(e.Data)
	if err != nil {
		return pendingResult{}, false, err
	}
	now, err := b.advanceClock(cmd.GetLeaderTimeUnixMs())
	if err != nil {
		return pendingResult{}, false, err
	}
	if err := b.expireSessions(now); err != nil {
		return pendingResult{}, false, err
	}
	var res Result
	switch kind := command.KindOf(cmd); kind {
	case command.KindRegisterSession:
		// D-26: the entry's index is the client ID, so it is unique and
		// identical on every replica.
		res = Result{Index: e.Index, Kind: kind, ClientID: e.Index}
		err = b.putSession(e.Index, session{lastActive: now}, 0, false)
	case command.KindGet:
		res, err = execute(b, e.Index, cmd)
	case command.KindPut, command.KindDelete, command.KindCas:
		if cmd.GetSession().GetClientId() != 0 {
			res, err = applyWithSession(b, e.Index, now, cmd)
		} else {
			res, err = execute(b, e.Index, cmd) // no session: at-least-once on retry
		}
	default:
		err = command.ErrNoOp
	}
	if err != nil {
		return pendingResult{}, false, err
	}
	return pendingResult{id: cmd.GetRequestId(), r: res}, cmd.GetRequestId() != 0, nil
}

// applyWithSession is the dedup rule of api-design 5 (D-09). Session
// outcomes are returned in Result.Err and change no user data; the session
// record, its cached replies and the expiry index are staged in the batch.
func applyWithSession(b *ApplyBatch, index, now uint64, cmd *raftkvv1.Command) (Result, error) {
	ws := cmd.GetSession()
	cid, seq, acked := ws.GetClientId(), ws.GetSequence(), ws.GetAckedSequence()
	kind := command.KindOf(cmd)
	rec, found, err := b.getSession(cid)
	if err != nil {
		return Result{}, err
	}
	if !found {
		return Result{Index: index, Kind: kind, Err: command.ErrSessionExpired}, nil
	}
	old := rec
	if acked > rec.acked { // the client has every reply up to acked: drop them
		if err := b.dropReplies(cid, rec.acked+1, acked); err != nil {
			return Result{}, err
		}
		rec.acked = acked
	}
	raw, cached, err := b.getRaw(replyKey(cid, seq))
	if err != nil {
		return Result{}, err
	}
	var res Result
	switch {
	case cached: // duplicate: replay, do not re-execute
		if res, err = decodeReply(raw); err != nil {
			return Result{}, err
		}
		res.Duplicate = true
	case seq <= rec.acked: // includes seq 0; the reply is gone
		res = Result{Index: index, Kind: kind, Err: command.ErrSequenceStale}
	case seq-rec.acked > command.SessionWindow:
		res = Result{Index: index, Kind: kind, Err: command.ErrSessionWindow}
	default:
		if res, err = execute(b, index, cmd); err != nil {
			return Result{}, err
		}
		if err := b.b.Set(replyKey(cid, seq), encodeReply(trimForCache(res, cmd)), nil); err != nil {
			return Result{}, err
		}
		rec.lastSeq = max(rec.lastSeq, seq)
	}
	if res.Err == nil {
		rec.lastActive = now
	}
	if rec != old {
		return res, b.putSession(cid, rec, old.lastActive, true)
	}
	return res, nil
}

// trimForCache keeps only what the server needs to rebuild the reply, so
// cached replies stay small (a Put's new value is never sent back).
func trimForCache(r Result, cmd *raftkvv1.Command) Result {
	switch op := cmd.GetOp().(type) {
	case *raftkvv1.Command_Put:
		r.Value = Value{}
		if !op.Put.GetReturnPrevious() {
			r.Previous = Value{}
		}
	case *raftkvv1.Command_Delete:
		if !op.Delete.GetReturnPrevious() {
			r.Previous = Value{}
		}
	}
	return r
}

// execute runs an operation against the batch. Put and CAS are re-validated:
// they were checked at propose time, so a bad entry is a poison pill and
// failing loudly beats diverging.
func execute(b *ApplyBatch, index uint64, cmd *raftkvv1.Command) (Result, error) {
	res := Result{Index: index, Kind: command.KindOf(cmd)}
	switch op := cmd.GetOp().(type) {
	case *raftkvv1.Command_Put:
		if _, verr := command.Put(0, 0, op.Put.GetKey(), op.Put.GetValue()); verr != nil {
			return Result{}, verr
		}
		cur, prev, had, err := b.Put(op.Put.GetKey(), op.Put.GetValue())
		if err != nil {
			return Result{}, err
		}
		res.Value, res.Previous, res.Found = cur, prev, had
	case *raftkvv1.Command_Delete:
		prev, existed, err := b.Delete(op.Delete.GetKey())
		if err != nil {
			return Result{}, err
		}
		res.Previous, res.Found = prev, existed
	case *raftkvv1.Command_Get:
		if len(op.Get.GetKey()) == 0 {
			return res, nil // a read barrier (command.Barrier): reads nothing
		}
		v, found, err := b.Get(op.Get.GetKey())
		if err != nil {
			return Result{}, err
		}
		res.Value, res.Found = v, found
	case *raftkvv1.Command_Cas:
		if verr := command.ValidateCas(op.Cas); verr != nil {
			return Result{}, verr
		}
		return b.cas(index, op.Cas)
	default:
		return Result{}, command.ErrNoOp
	}
	return res, nil
}

// cas evaluates a CompareAndSwap at apply time, serialized with every other
// write (api-design 3.1). A failed compare is a normal result: the entry
// still advances the revision but changes no key.
func (a *ApplyBatch) cas(index uint64, op *raftkvv1.CasOp) (Result, error) {
	res := Result{Index: index, Kind: command.KindCas}
	cur, exists, err := a.Get(op.GetKey())
	if err != nil {
		return Result{}, err
	}
	switch e := op.GetExpected().(type) {
	case *raftkvv1.CasOp_ExpectedValue:
		res.Succeeded = exists && bytes.Equal(cur.Data, e.ExpectedValue)
	case *raftkvv1.CasOp_ExpectedModRevision:
		res.Succeeded = exists && cur.ModRevision == e.ExpectedModRevision
	case *raftkvv1.CasOp_ExpectAbsent:
		res.Succeeded = !exists
	}
	if !res.Succeeded {
		res.Found, res.Value = exists, cur
		return res, nil
	}
	switch act := op.GetAction().(type) {
	case *raftkvv1.CasOp_NewValue:
		v, _, _, err := a.Put(op.GetKey(), act.NewValue)
		if err != nil {
			return Result{}, err
		}
		res.Found, res.Value = true, v
	case *raftkvv1.CasOp_Delete:
		if _, _, err := a.Delete(op.GetKey()); err != nil {
			return Result{}, err
		}
	}
	return res, nil
}
