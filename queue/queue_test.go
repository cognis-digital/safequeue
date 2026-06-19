package queue

import (
	"errors"
	"testing"
	"time"
)

// newTestQueue opens a queue in a temp dir with a controllable fake clock and
// tiny, deterministic backoff so tests never touch the wall clock.
func newTestQueue(t *testing.T, opts Options) (*Queue, *fixedClock) {
	t.Helper()
	clk := newFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	opts.Clock = clk
	q, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q, clk
}

func TestEnqueueDequeueAck(t *testing.T) {
	q, _ := newTestQueue(t, Options{})

	m, created, err := q.Enqueue("hello", "")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !created {
		t.Fatalf("expected created=true")
	}
	if m.Payload != "hello" || m.State != StateReady {
		t.Fatalf("unexpected message: %+v", m)
	}

	got, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.ID != m.ID {
		t.Fatalf("dequeued %s, want %s", got.ID, m.ID)
	}
	if got.State != StateLeased {
		t.Fatalf("expected leased, got %s", got.State)
	}
	if got.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", got.Attempts)
	}

	// Second dequeue should be empty (message is leased/invisible).
	if _, err := q.Dequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}

	if err := q.Ack(got.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// After ack, message is gone.
	if _, err := q.Get(got.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after ack, got %v", err)
	}
	s := q.Stats()
	if s.Total != 0 || s.Ready != 0 || s.Leased != 0 {
		t.Fatalf("unexpected stats after ack: %+v", s)
	}
}

func TestAckRequiresLease(t *testing.T) {
	q, _ := newTestQueue(t, Options{})
	m, _, _ := q.Enqueue("x", "")
	// Not leased yet.
	if err := q.Ack(m.ID); !errors.Is(err, ErrNotLeased) {
		t.Fatalf("expected ErrNotLeased, got %v", err)
	}
	if err := q.Ack("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFIFOOrder(t *testing.T) {
	q, _ := newTestQueue(t, Options{})
	a, _, _ := q.Enqueue("a", "")
	b, _, _ := q.Enqueue("b", "")

	d1, _ := q.Dequeue()
	d2, _ := q.Dequeue()
	if d1.ID != a.ID || d2.ID != b.ID {
		t.Fatalf("FIFO violated: got %s,%s want %s,%s", d1.ID, d2.ID, a.ID, b.ID)
	}
}

func TestIdempotencyDedupe(t *testing.T) {
	q, _ := newTestQueue(t, Options{})

	m1, created1, err := q.Enqueue("payload-1", "key-A")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !created1 {
		t.Fatalf("first enqueue should be created")
	}

	m2, created2, err := q.Enqueue("payload-2", "key-A")
	if err != nil {
		t.Fatalf("Enqueue dup: %v", err)
	}
	if created2 {
		t.Fatalf("duplicate key should not create a new message")
	}
	if m2.ID != m1.ID {
		t.Fatalf("dedup returned different ID: %s vs %s", m2.ID, m1.ID)
	}
	// Original payload preserved, not overwritten by the duplicate.
	if m2.Payload != "payload-1" {
		t.Fatalf("expected original payload, got %q", m2.Payload)
	}

	// A different key creates a distinct message.
	m3, created3, _ := q.Enqueue("payload-3", "key-B")
	if !created3 || m3.ID == m1.ID {
		t.Fatalf("distinct key should create a new message")
	}

	s := q.Stats()
	if s.Ready != 2 {
		t.Fatalf("expected 2 ready messages, got %d", s.Ready)
	}
}

func TestDedupeReleasedAfterAck(t *testing.T) {
	q, _ := newTestQueue(t, Options{})
	m1, _, _ := q.Enqueue("p", "key-X")
	got, _ := q.Dequeue()
	if err := q.Ack(got.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// After the keyed message is acked, the same key is free to enqueue anew.
	m2, created, _ := q.Enqueue("p2", "key-X")
	if !created {
		t.Fatalf("expected new message after acked key freed")
	}
	if m2.ID == m1.ID {
		t.Fatalf("new message should have a fresh ID")
	}
}

func TestNackBackoffThenDeadLetter(t *testing.T) {
	q, clk := newTestQueue(t, Options{
		VisibilityTimeout: 10 * time.Second,
		MaxAttempts:       3,
		BaseBackoff:       1 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
	})

	m, _, _ := q.Enqueue("work", "")

	// Attempt 1: dequeue, nack -> backoff 1ms, attempts=1.
	d, _ := q.Dequeue()
	if d.Attempts != 1 {
		t.Fatalf("attempt 1: expected attempts=1, got %d", d.Attempts)
	}
	dead, err := q.Nack(d.ID)
	if err != nil {
		t.Fatalf("Nack 1: %v", err)
	}
	if dead {
		t.Fatalf("should not be dead after attempt 1")
	}
	// Not yet visible (backoff in the future).
	if _, err := q.Dequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty during backoff, got %v", err)
	}
	clk.Advance(2 * time.Millisecond) // past 1ms backoff

	// Attempt 2.
	d, _ = q.Dequeue()
	if d.Attempts != 2 {
		t.Fatalf("attempt 2: expected attempts=2, got %d", d.Attempts)
	}
	dead, _ = q.Nack(d.ID)
	if dead {
		t.Fatalf("should not be dead after attempt 2")
	}
	clk.Advance(10 * time.Millisecond) // past 2ms backoff

	// Attempt 3 (== MaxAttempts).
	d, _ = q.Dequeue()
	if d.Attempts != 3 {
		t.Fatalf("attempt 3: expected attempts=3, got %d", d.Attempts)
	}
	dead, err = q.Nack(d.ID)
	if err != nil {
		t.Fatalf("Nack 3: %v", err)
	}
	if !dead {
		t.Fatalf("expected dead-letter after MaxAttempts")
	}

	gm, err := q.Get(m.ID)
	if err != nil {
		t.Fatalf("Get dead message: %v", err)
	}
	if gm.State != StateDead {
		t.Fatalf("expected dead state, got %s", gm.State)
	}
	// Dead messages are never redelivered.
	if _, err := q.Dequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("dead message should not be dequeued, got %v", err)
	}
	s := q.Stats()
	if s.Dead != 1 {
		t.Fatalf("expected 1 dead, got %+v", s)
	}
}

func TestBackoffExponentialAndCapped(t *testing.T) {
	q, _ := newTestQueue(t, Options{
		BaseBackoff: 1 * time.Second,
		MaxBackoff:  10 * time.Second,
	})
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // capped
		{6, 10 * time.Second}, // capped
	}
	for _, c := range cases {
		if got := q.backoff(c.attempts); got != c.want {
			t.Errorf("backoff(%d)=%v want %v", c.attempts, got, c.want)
		}
	}
}

func TestVisibilityTimeoutRedelivery(t *testing.T) {
	q, clk := newTestQueue(t, Options{VisibilityTimeout: 5 * time.Second})
	m, _, _ := q.Enqueue("v", "")

	d1, _ := q.Dequeue()
	if d1.ID != m.ID {
		t.Fatalf("unexpected dequeue")
	}
	// Lease not expired yet -> invisible.
	if _, err := q.Dequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected invisible during lease, got %v", err)
	}
	// Advance past the visibility timeout -> redelivered.
	clk.Advance(6 * time.Second)
	d2, err := q.Dequeue()
	if err != nil {
		t.Fatalf("expected redelivery, got %v", err)
	}
	if d2.ID != m.ID {
		t.Fatalf("expected same message redelivered")
	}
}

func TestDurabilityRecovery(t *testing.T) {
	dir := t.TempDir()
	clk := newFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	// First session: enqueue 3, ack 1, lease+nack 1 into dead via low max.
	q1, err := Open(dir, Options{
		VisibilityTimeout: 10 * time.Second,
		MaxAttempts:       1,
		BaseBackoff:       time.Millisecond,
		Clock:             clk,
	})
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	a, _, _ := q1.Enqueue("a", "ka")
	_, _, _ = q1.Enqueue("b", "kb")
	c, _, _ := q1.Enqueue("c", "kc")

	// Ack a.
	da, _ := q1.Dequeue()
	if da.ID != a.ID {
		t.Fatalf("expected a first")
	}
	if err := q1.Ack(da.ID); err != nil {
		t.Fatalf("Ack a: %v", err)
	}
	// Dead-letter c (MaxAttempts=1: first nack dead-letters).
	// Dequeue b then c.
	db, _ := q1.Dequeue()
	dc, _ := q1.Dequeue()
	if dc.ID != c.ID {
		t.Fatalf("expected c, got %s", dc.ID)
	}
	dead, _ := q1.Nack(dc.ID)
	if !dead {
		t.Fatalf("c should be dead-lettered at MaxAttempts=1")
	}
	_ = db
	if err := q1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Second session: reopen and verify recovered state.
	clk2 := newFixedClock(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	q2, err := Open(dir, Options{
		VisibilityTimeout: 10 * time.Second,
		MaxAttempts:       1,
		Clock:             clk2,
	})
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer q2.Close()

	// a was acked -> gone.
	if _, err := q2.Get(a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a should be gone after recovery")
	}
	// c was dead-lettered -> present and dead.
	gc, err := q2.Get(c.ID)
	if err != nil {
		t.Fatalf("Get c after recovery: %v", err)
	}
	if gc.State != StateDead {
		t.Fatalf("c should be dead after recovery, got %s", gc.State)
	}
	// b was leased in session 1; its lease is long expired by clk2 -> ready.
	s := q2.Stats()
	if s.Dead != 1 {
		t.Fatalf("expected 1 dead after recovery, got %+v", s)
	}
	if s.Ready != 1 {
		t.Fatalf("expected 1 ready (b reclaimed) after recovery, got %+v", s)
	}

	// Dedup index recovered: re-enqueuing kc (acked, freed) creates new; kb (still live) dedups.
	_, createdB, _ := q2.Enqueue("b2", "kb")
	if createdB {
		t.Fatalf("kb should still dedup after recovery")
	}
}

func TestRecoveryToleratesTornTail(t *testing.T) {
	dir := t.TempDir()
	clk := newFixedClock(time.Now())
	q, err := Open(dir, Options{Clock: clk})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	q.Enqueue("good", "")
	q.Close()

	// Append a torn (partial) JSON line to simulate a crash mid-write.
	logPath := dir + "/queue.log"
	f, err := openAppend(logPath)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	f.WriteString(`{"op":"enqueue","id":"msg-`)
	f.Close()

	q2, err := Open(dir, Options{Clock: clk})
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer q2.Close()
	s := q2.Stats()
	if s.Total != 1 {
		t.Fatalf("expected 1 recovered message despite torn tail, got %+v", s)
	}
}
