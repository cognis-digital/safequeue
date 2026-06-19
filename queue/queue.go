// Package queue implements a durable, idempotent, at-least-once job queue.
//
// Messages are enqueued with an idempotency key; enqueuing the same key twice
// returns the original message instead of creating a duplicate. Consumers
// Dequeue a message (taking a lease with a visibility timeout), then either
// Ack it (remove) or Nack it (retry with exponential backoff up to a maximum
// number of attempts, after which it is moved to a dead-letter set).
//
// Durability is provided by an append-only log on disk: every state change is
// written as a record, and Open replays the log to rebuild in-memory state,
// so the queue survives process restarts.
package queue

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Errors returned by queue operations.
var (
	// ErrNotFound is returned when an ID does not correspond to a live message.
	ErrNotFound = errors.New("safequeue: message not found")
	// ErrNotLeased is returned when Ack/Nack targets a message that is not
	// currently leased (e.g. already acked, dead, or never leased).
	ErrNotLeased = errors.New("safequeue: message is not leased")
	// ErrEmpty is returned by Dequeue when no message is currently available.
	ErrEmpty = errors.New("safequeue: no message available")
	// ErrClosed is returned when operating on a closed queue.
	ErrClosed = errors.New("safequeue: queue is closed")
)

// Options configures a queue. The zero value is usable; Open fills in sensible
// defaults for any unset field.
type Options struct {
	// VisibilityTimeout is how long a dequeued message stays invisible before
	// it becomes available again (lease duration). Default: 30s.
	VisibilityTimeout time.Duration
	// MaxAttempts is the number of delivery attempts before a message is
	// dead-lettered. Default: 5.
	MaxAttempts int
	// BaseBackoff is the first retry delay; subsequent delays double each
	// attempt (capped by MaxBackoff). Default: 1s.
	BaseBackoff time.Duration
	// MaxBackoff caps the exponential backoff delay. Default: 5m.
	MaxBackoff time.Duration
	// Clock provides the current time. Default: RealClock. Tests inject a fake.
	Clock Clock
}

func (o *Options) applyDefaults() {
	if o.VisibilityTimeout <= 0 {
		o.VisibilityTimeout = 30 * time.Second
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.BaseBackoff <= 0 {
		o.BaseBackoff = time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 5 * time.Minute
	}
	if o.Clock == nil {
		o.Clock = RealClock{}
	}
}

// Stats is a point-in-time snapshot of queue contents by state.
type Stats struct {
	Ready    int `json:"ready"`
	Leased   int `json:"leased"`
	Dead     int `json:"dead"`
	Acked    int `json:"acked"`
	Total    int `json:"total"`
	DedupKey int `json:"dedup_keys"`
}

// Queue is a durable, idempotent job queue. It is safe for concurrent use.
type Queue struct {
	opts Options

	mu     sync.Mutex
	msgs   map[string]*Message // live messages by ID (ready/leased/dead)
	dedup  map[string]string   // idempotency key -> message ID
	order  []string            // FIFO insertion order of IDs
	seq    uint64              // monotonic counter for ID generation
	file   *os.File
	writer *bufio.Writer
	closed bool
}

// Open opens (or creates) a queue backed by a log file in dir. If the log
// already exists, its records are replayed to recover prior state.
func Open(dir string, opts Options) (*Queue, error) {
	opts.applyDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("safequeue: create data dir: %w", err)
	}
	logPath := filepath.Join(dir, "queue.log")

	q := &Queue{
		opts:  opts,
		msgs:  make(map[string]*Message),
		dedup: make(map[string]string),
	}

	// Replay existing records (if any) before opening for appending.
	if err := q.replay(logPath); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("safequeue: open log: %w", err)
	}
	q.file = f
	q.writer = bufio.NewWriter(f)
	return q, nil
}

// replay reads the log file and rebuilds in-memory state from its records.
func (q *Queue) replay(logPath string) error {
	f, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh queue
		}
		return fmt.Errorf("safequeue: open log for replay: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r record
		if err := json.Unmarshal(line, &r); err != nil {
			// A torn final line (partial write before a crash) is tolerated:
			// stop replay at the first unparsable record.
			break
		}
		q.applyRecord(r)
	}
	return sc.Err()
}

// applyRecord mutates in-memory state to reflect a single log record. It is
// used both during replay and (with a freshly built record) during live ops.
func (q *Queue) applyRecord(r record) {
	switch r.Op {
	case opEnqueue:
		m := &Message{
			ID:             r.ID,
			Payload:        r.Payload,
			IdempotencyKey: r.IdempotencyKey,
			Attempts:       0,
			State:          StateReady,
			EnqueuedAt:     r.EnqueuedAt,
			VisibleAt:      r.VisibleAt,
		}
		q.msgs[m.ID] = m
		q.order = append(q.order, m.ID)
		if m.IdempotencyKey != "" {
			q.dedup[m.IdempotencyKey] = m.ID
		}
		q.bumpSeq(m.ID)
	case opLease:
		if m, ok := q.msgs[r.ID]; ok {
			m.State = StateLeased
			m.Attempts = r.Attempts
			m.LeaseExpiresAt = r.LeaseExpiresAt
		}
	case opAck:
		if m, ok := q.msgs[r.ID]; ok {
			m.State = StateAcked
			q.removeLive(r.ID)
		}
	case opNack:
		if m, ok := q.msgs[r.ID]; ok {
			m.State = StateReady
			m.Attempts = r.Attempts
			m.VisibleAt = r.VisibleAt
			m.LeaseExpiresAt = time.Time{}
		}
	case opDead:
		if m, ok := q.msgs[r.ID]; ok {
			m.State = StateDead
			m.Attempts = r.Attempts
		}
	}
}

// removeLive removes an acked message from the live set and dedup index but
// keeps FIFO order entries harmless (they are skipped when not present).
func (q *Queue) removeLive(id string) {
	if m, ok := q.msgs[id]; ok {
		if m.IdempotencyKey != "" && q.dedup[m.IdempotencyKey] == id {
			delete(q.dedup, m.IdempotencyKey)
		}
		delete(q.msgs, id)
	}
}

// bumpSeq ensures the ID generator stays ahead of any recovered IDs so newly
// generated IDs never collide with recovered ones.
func (q *Queue) bumpSeq(id string) {
	var n uint64
	if _, err := fmt.Sscanf(id, "msg-%d", &n); err == nil {
		if n >= q.seq {
			q.seq = n + 1
		}
	}
}

// append serializes a record, writes it to the log, and flushes+syncs so it is
// durable before the operation returns.
func (q *Queue) append(r record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := q.writer.Write(b); err != nil {
		return err
	}
	if err := q.writer.WriteByte('\n'); err != nil {
		return err
	}
	if err := q.writer.Flush(); err != nil {
		return err
	}
	return q.file.Sync()
}

// commit appends a record durably and then applies it to memory. Memory is
// only mutated after the write succeeds, keeping disk and memory consistent.
func (q *Queue) commit(r record) error {
	if err := q.append(r); err != nil {
		return err
	}
	q.applyRecord(r)
	return nil
}

// nextID returns a fresh, monotonically increasing message ID.
func (q *Queue) nextID() string {
	id := fmt.Sprintf("msg-%d", q.seq)
	q.seq++
	return id
}

// Enqueue adds a message with the given payload. If idempotencyKey is
// non-empty and a live message already exists for it, the existing message is
// returned and no new message is created (dedupe). The returned bool reports
// whether the message is newly created (true) or a deduped existing one
// (false).
func (q *Queue) Enqueue(payload, idempotencyKey string) (*Message, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, false, ErrClosed
	}

	if idempotencyKey != "" {
		if existingID, ok := q.dedup[idempotencyKey]; ok {
			if m, ok := q.msgs[existingID]; ok {
				return m.clone(), false, nil
			}
		}
	}

	now := q.opts.Clock.Now()
	id := q.nextID()
	r := record{
		Op:             opEnqueue,
		ID:             id,
		Payload:        payload,
		IdempotencyKey: idempotencyKey,
		EnqueuedAt:     now,
		VisibleAt:      now,
	}
	if err := q.commit(r); err != nil {
		// Roll back the consumed sequence number on write failure.
		q.seq--
		return nil, false, err
	}
	return q.msgs[id].clone(), true, nil
}

// Dequeue leases the oldest available (ready and visible) message, marking it
// invisible for the configured visibility timeout, and returns a copy. If no
// message is available it returns ErrEmpty.
func (q *Queue) Dequeue() (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}

	now := q.opts.Clock.Now()
	q.reclaimExpired(now)

	for _, id := range q.order {
		m, ok := q.msgs[id]
		if !ok {
			continue
		}
		if m.State != StateReady {
			continue
		}
		if m.VisibleAt.After(now) {
			continue
		}
		r := record{
			Op:             opLease,
			ID:             m.ID,
			Attempts:       m.Attempts + 1,
			LeaseExpiresAt: now.Add(q.opts.VisibilityTimeout),
		}
		if err := q.commit(r); err != nil {
			return nil, err
		}
		return q.msgs[m.ID].clone(), nil
	}
	return nil, ErrEmpty
}

// reclaimExpired returns leased messages whose lease has expired back to the
// ready state so they can be redelivered (at-least-once semantics).
func (q *Queue) reclaimExpired(now time.Time) {
	for _, id := range q.order {
		m, ok := q.msgs[id]
		if !ok {
			continue
		}
		if m.State == StateLeased && !m.LeaseExpiresAt.IsZero() && !m.LeaseExpiresAt.After(now) {
			// Treat an expired lease like an implicit nack without consuming an
			// extra attempt beyond what the lease already counted.
			m.State = StateReady
			m.LeaseExpiresAt = time.Time{}
			m.VisibleAt = now
		}
	}
}

// Ack removes a leased message from the queue, marking it successfully
// processed. It errors if the ID is unknown or not currently leased.
func (q *Queue) Ack(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	m, ok := q.msgs[id]
	if !ok {
		return ErrNotFound
	}
	if m.State != StateLeased {
		return ErrNotLeased
	}
	return q.commit(record{Op: opAck, ID: id})
}

// Nack returns a leased message for retry. If the message has not exhausted
// MaxAttempts it becomes ready again after an exponential backoff delay;
// otherwise it is moved to the dead-letter set. The returned bool reports
// whether the message was dead-lettered (true) or scheduled for retry (false).
func (q *Queue) Nack(id string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false, ErrClosed
	}
	m, ok := q.msgs[id]
	if !ok {
		return false, ErrNotFound
	}
	if m.State != StateLeased {
		return false, ErrNotLeased
	}

	if m.Attempts >= q.opts.MaxAttempts {
		if err := q.commit(record{Op: opDead, ID: id, Attempts: m.Attempts}); err != nil {
			return false, err
		}
		return true, nil
	}

	now := q.opts.Clock.Now()
	delay := q.backoff(m.Attempts)
	r := record{
		Op:        opNack,
		ID:        id,
		Attempts:  m.Attempts,
		VisibleAt: now.Add(delay),
	}
	if err := q.commit(r); err != nil {
		return false, err
	}
	return false, nil
}

// backoff computes the exponential backoff delay for the given attempt count
// (1-based), capped at MaxBackoff.
func (q *Queue) backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := q.opts.BaseBackoff
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= q.opts.MaxBackoff {
			return q.opts.MaxBackoff
		}
	}
	if delay > q.opts.MaxBackoff {
		delay = q.opts.MaxBackoff
	}
	return delay
}

// Get returns a copy of the message with the given ID, or ErrNotFound.
func (q *Queue) Get(id string) (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	m, ok := q.msgs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return m.clone(), nil
}

// Stats returns a snapshot of current queue state by lifecycle stage.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.opts.Clock.Now()
	q.reclaimExpired(now)
	var s Stats
	for _, m := range q.msgs {
		switch m.State {
		case StateReady:
			s.Ready++
		case StateLeased:
			s.Leased++
		case StateDead:
			s.Dead++
		}
	}
	s.Total = len(q.msgs)
	s.DedupKey = len(q.dedup)
	return s
}

// Close flushes pending writes and closes the underlying log file. The queue
// is unusable afterwards.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	if q.writer != nil {
		if err := q.writer.Flush(); err != nil {
			q.file.Close()
			return err
		}
	}
	if q.file != nil {
		return q.file.Close()
	}
	return nil
}
