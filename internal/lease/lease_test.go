package lease

import (
	"testing"
	"time"
)

func TestDurationDefaults(t *testing.T) {
	d, err := Config{ElectionTick: 10, Tick: 100 * time.Millisecond, Drift: DefaultDrift, Margin: DefaultMargin}.Duration()
	if err != nil || d != 710*time.Millisecond {
		t.Fatalf("Duration = %v, %v; want 710ms", d, err)
	}
}

func TestDurationRejectsBadConfig(t *testing.T) {
	for _, c := range []Config{
		{ElectionTick: 1, Tick: time.Second},
		{ElectionTick: 10, Tick: 0},
		{ElectionTick: 10, Tick: time.Millisecond, Drift: 1},
		{ElectionTick: 10, Tick: time.Millisecond, Drift: -0.1},
		{ElectionTick: 10, Tick: 10 * time.Millisecond, Margin: time.Second},
	} {
		if _, err := c.Duration(); err == nil {
			t.Errorf("%+v: want an error", c)
		}
	}
}

func TestLeaseLifecycle(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	l := New(ms(700))
	if l.Valid(1, 0) {
		t.Fatal("a new lease must not be held")
	}
	l.Extend(3, ms(100))
	if !l.Valid(3, ms(799)) || l.Valid(3, ms(800)) {
		t.Fatal("lease must cover [sentAt, sentAt+dur)")
	}
	if l.Valid(4, ms(200)) || l.Valid(2, ms(200)) {
		t.Fatal("lease must only be valid in its own term")
	}
	l.Extend(3, ms(50)) // an older round finishing late never shortens it
	if !l.Valid(3, ms(799)) {
		t.Fatal("older extension shortened the lease")
	}
	l.Extend(2, ms(900)) // older term: ignored
	if l.Valid(2, ms(1000)) || !l.Valid(3, ms(700)) {
		t.Fatal("an extension from an older term must be ignored")
	}
	l.Revoke()
	if l.Valid(3, ms(200)) {
		t.Fatal("revoked lease still valid")
	}
	l.Extend(5, ms(1000))
	if !l.Valid(5, ms(1500)) {
		t.Fatal("lease not usable after revoke and extend")
	}
}
