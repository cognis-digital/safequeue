package queue

import "time"

// State is the lifecycle stage of a message inside the queue.
type State string

const (
	// StateReady means the message is available to be dequeued.
	StateReady State = "ready"
	// StateLeased means the message has been handed to a consumer and is
	// invisible until its lease expires or it is acked/nacked.
	StateLeased State = "leased"
	// StateAcked means the message was processed successfully and removed.
	StateAcked State = "acked"
	// StateDead means the message exhausted its retries and was moved to the
	// dead-letter set.
	StateDead State = "dead"
)

// Message is the unit of work stored in the queue. It is what consumers see
// on Dequeue.
type Message struct {
	ID             string    `json:"id"`
	Payload        string    `json:"payload"`
	IdempotencyKey string    `json:"idempotency_key"`
	Attempts       int       `json:"attempts"`
	State          State     `json:"state"`
	EnqueuedAt     time.Time `json:"enqueued_at"`
	VisibleAt      time.Time `json:"visible_at"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

// opType identifies the kind of mutation captured in a log record.
type opType string

const (
	opEnqueue opType = "enqueue"
	opLease   opType = "lease"
	opAck     opType = "ack"
	opNack    opType = "nack"
	opDead    opType = "dead"
)

// record is a single append-only log entry. Replaying the records in order
// fully reconstructs queue state, which is how durability and recovery work.
type record struct {
	Op             opType    `json:"op"`
	ID             string    `json:"id"`
	Payload        string    `json:"payload,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Attempts       int       `json:"attempts,omitempty"`
	EnqueuedAt     time.Time `json:"enqueued_at,omitempty"`
	VisibleAt      time.Time `json:"visible_at,omitempty"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
}

// clone returns a copy of the message so callers cannot mutate internal state.
func (m *Message) clone() *Message {
	cp := *m
	return &cp
}
