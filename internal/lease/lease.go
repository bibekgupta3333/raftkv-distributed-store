// Package lease is the optional leader lease for reads (D-25, system-design
// 3.2). It is off by default; ReadIndex (D-05) is the default read path.
//
// The lease rests on one promise of the Raft core: with CheckQuorum on (D-08)
// a follower that heard from its leader will not help elect anyone else until
// its election timer runs out (T5). If a quorum acked a heartbeat sent at time
// t, no other leader can exist before t + (that timer), so until then the
// leader may answer reads from its commit index without a round trip.
//
// The timer is counted in the follower's ticks, so the lease must be shortened
// for clock-rate drift between machines (ρ) and by a safety margin. A follower
// resets its timer when it receives the heartbeat but may tick right after, so
// only ElectionTick-1 full ticks are certain. Hence
//
//	duration = (ElectionTick - 1) × tick × (1 - ρ) - margin
//
// which with the defaults (10 ticks of 100 ms, ρ = 0.1, 100 ms) is 710 ms.
//
// The lease is not safe if clocks drift by more than ρ (the simulator shows
// that, WBS 4.2.5), or if a voter restarts inside the lease window: a
// restarted node forgot it heard from the leader and votes at once.
//
// The package is pure: callers pass times as durations on a monotonic clock
// of their choice (the node: time since start; the simulator: virtual time).
package lease

import (
	"errors"
	"time"
)

// Defaults (D-25).
const (
	DefaultDrift  = 0.1
	DefaultMargin = 100 * time.Millisecond
)

// Config describes the cluster timing the lease relies on.
type Config struct {
	// ElectionTick is the base election timeout in ticks (raft.Config).
	ElectionTick int
	// Tick is the wall-clock length of one tick.
	Tick time.Duration
	// Drift is the assumed bound on clock-rate difference, in [0, 1).
	Drift float64
	// Margin is subtracted as a safety margin.
	Margin time.Duration
}

// Duration returns how long a lease lasts after the heartbeat that earned it
// was sent, or an error if the parameters leave no positive lease.
func (c Config) Duration() (time.Duration, error) {
	if c.ElectionTick < 2 || c.Tick <= 0 || c.Drift < 0 || c.Drift >= 1 || c.Margin < 0 {
		return 0, errors.New("lease: need ElectionTick >= 2, Tick > 0, 0 <= Drift < 1, Margin >= 0")
	}
	safe := time.Duration(c.ElectionTick-1) * c.Tick
	d := time.Duration(float64(safe)*(1-c.Drift)) - c.Margin
	if d <= 0 {
		return 0, errors.New("lease: drift and margin leave no lease")
	}
	return d, nil
}

// Lease is the leader's lease. The zero value is not held. It is not safe for
// concurrent use; the node guards it with a mutex.
type Lease struct {
	dur    time.Duration
	term   uint64
	expiry time.Duration
	held   bool
}

// New returns an unheld lease that lasts dur after each renewal.
func New(dur time.Duration) *Lease { return &Lease{dur: dur} }

// Extend records that a heartbeat round sent at sentAt (or later) in term was
// acked by a quorum. An extension from an older term is ignored; one from a
// newer term replaces the lease.
func (l *Lease) Extend(term uint64, sentAt time.Duration) {
	switch {
	case l.held && term < l.term:
		return
	case !l.held || term > l.term:
		l.term, l.expiry, l.held = term, sentAt+l.dur, true
	default:
		l.expiry = max(l.expiry, sentAt+l.dur)
	}
}

// Valid reports whether the lease covers time now for a leader of term.
func (l *Lease) Valid(term uint64, now time.Duration) bool {
	return l.held && l.term == term && now < l.expiry
}

// Revoke drops the lease (step-down, higher term, transfer, D-25).
func (l *Lease) Revoke() { *l = Lease{dur: l.dur} }
