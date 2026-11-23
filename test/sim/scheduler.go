package sim

import "container/heap"

// Scheduler is the virtual clock and event queue (D-24). Time is an int64 in
// virtual milliseconds. Events with equal time run in insertion order, so a
// run is a pure function of the seed.
type Scheduler struct {
	now int64
	seq uint64
	q   eventQueue
}

type scheduled struct {
	at  int64
	seq uint64
	fn  func()
}

type eventQueue []scheduled

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(scheduled)) }
func (q *eventQueue) Pop() any {
	old := *q
	it := old[len(old)-1]
	old[len(old)-1] = scheduled{}
	*q = old[:len(old)-1]
	return it
}

// Now is the current virtual time in milliseconds.
func (s *Scheduler) Now() int64 { return s.now }

// After schedules fn to run delay ms from now (delay below 0 means 0).
func (s *Scheduler) After(delay int64, fn func()) {
	if delay < 0 {
		delay = 0
	}
	s.seq++
	heap.Push(&s.q, scheduled{at: s.now + delay, seq: s.seq, fn: fn})
}

// Pending is the number of queued events.
func (s *Scheduler) Pending() int { return len(s.q) }

// Step runs the next event and reports whether there was one.
func (s *Scheduler) Step() bool {
	if len(s.q) == 0 {
		return false
	}
	it := heap.Pop(&s.q).(scheduled)
	s.now = it.at
	it.fn()
	return true
}
