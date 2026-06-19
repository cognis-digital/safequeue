package queue

import "time"

// Clock abstracts time so the queue and its tests do not depend on the real
// wall clock. Production code uses RealClock; tests inject a controllable
// fake to advance time without sleeping.
type Clock interface {
	Now() time.Time
}

// RealClock reports the actual current time.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// fixedClock is a test-friendly clock whose value can be advanced manually.
type fixedClock struct {
	t time.Time
}

// Now returns the clock's current fixed time.
func (c *fixedClock) Now() time.Time { return c.t }

// Advance moves the fixed clock forward by d.
func (c *fixedClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// newFixedClock builds a fixedClock starting at t.
func newFixedClock(t time.Time) *fixedClock { return &fixedClock{t: t} }
