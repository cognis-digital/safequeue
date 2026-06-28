# safequeue

A durable, idempotent transaction/job queue for Go — a small library plus an
optional HTTP service. Producers enqueue messages with an idempotency key
(duplicate keys are de-duplicated); consumers receive a message under a lease,
then **ack** it (remove) or **nack** it (retry with exponential backoff, then
dead-letter). The queue is file-backed and recovers its full state on restart.

- **At-least-once delivery.** A leased message that is neither acked nor nacked
  before its visibility timeout becomes available again.
- **Idempotent enqueue.** Enqueuing the same idempotency key twice returns the
  original message instead of creating a duplicate.
- **Durable.** Every state change is written to an append-only log and `fsync`'d;
  reopening the data directory replays the log to rebuild state. A torn final
  record (partial write before a crash) is tolerated.
- **Standard library only.** No third-party dependencies.

Maintainer: **Cognis Digital**
License: **COCL 1.0**


<!-- cognis:example:start -->
## 🔎 Example output

**Sample result format** _(illustrative values — run on your own data for real findings):_

```
{
  "queue": [
    {
      "id": 1,
      "name": "my_queue",
      "size": 5,
      "messages": [
        {"data": "msg_1"},
        {"data": "msg_2"},
        {"data": "msg_3"}
      ]
    }
  ],
  "stats": {
    "enqueued": 10,
    "dequeued": 5,
    "size": 5
  }
}
```

<!-- cognis:example:end -->

## Install

```
go get github.com/cognis-digital/safequeue/queue
```

Module: `github.com/cognis-digital/safequeue` (Go 1.22+).

## Library usage

```go
package main

import (
	"fmt"
	"log"

	"github.com/cognis-digital/safequeue/queue"
)

func main() {
	q, err := queue.Open("./data", queue.Options{}) // defaults applied
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Producer: enqueue with an idempotency key.
	msg, created, _ := q.Enqueue("send-welcome-email:user-42", "user-42")
	fmt.Println(msg.ID, "created:", created)

	// Consumer: lease a message, process it, then ack or nack.
	m, err := q.Dequeue()
	if err == queue.ErrEmpty {
		return // nothing available right now
	}
	if processOK(m) {
		q.Ack(m.ID)
	} else {
		dead, _ := q.Nack(m.ID) // schedules retry, or dead-letters if exhausted
		_ = dead
	}
}

func processOK(*queue.Message) bool { return true }
```

### Options (and defaults)

| Field               | Default | Meaning                                                       |
| ------------------- | ------- | ------------------------------------------------------------- |
| `VisibilityTimeout` | `30s`   | How long a leased message stays invisible before redelivery.  |
| `MaxAttempts`       | `5`     | Delivery attempts before a message is dead-lettered.          |
| `BaseBackoff`       | `1s`    | First retry delay; doubles each attempt up to `MaxBackoff`.   |
| `MaxBackoff`        | `5m`    | Upper bound on the backoff delay.                             |
| `Clock`             | real    | Time source; injectable for deterministic tests.              |

### API

| Method                                   | Description                                                                                  |
| ---------------------------------------- | -------------------------------------------------------------------------------------------- |
| `Open(dir, opts) (*Queue, error)`        | Open/create a queue; replays the log in `dir` to recover state.                              |
| `Enqueue(payload, key) (*Message, bool, error)` | Add a message. Returns `(msg, created, err)`; `created=false` means deduped by `key`. |
| `Dequeue() (*Message, error)`            | Lease the oldest available message. Returns `ErrEmpty` if none. Increments `Attempts`.       |
| `Ack(id) error`                          | Remove a leased message (success). `ErrNotFound` / `ErrNotLeased` on misuse.                 |
| `Nack(id) (bool, error)`                 | Retry a leased message after backoff, or dead-letter it. Returns `dead`.                     |
| `Get(id) (*Message, error)`              | Return a copy of a message by ID.                                                            |
| `Stats() Stats`                          | Snapshot of counts by state.                                                                 |
| `Close() error`                          | Flush and close the log file.                                                                |

## Delivery semantics

1. **Enqueue** adds a message in the `ready` state. If a non-empty idempotency
   key is supplied and a live message already exists for it, that existing
   message is returned and nothing new is created.
2. **Dequeue** picks the oldest `ready` message whose visibility time has
   passed, marks it `leased`, sets a lease expiry `now + VisibilityTimeout`,
   increments `Attempts`, and returns a copy.
3. **Ack** deletes the leased message. Its idempotency key is freed for reuse.
4. **Nack** either:
   - schedules a retry: the message returns to `ready` but stays invisible
     until `now + backoff(Attempts)`, where `backoff` is exponential
     (`BaseBackoff * 2^(attempts-1)`, capped at `MaxBackoff`); or
   - **dead-letters** it (state `dead`, never redelivered) once `Attempts`
     reaches `MaxAttempts`.
5. **Visibility timeout.** If a leased message is neither acked nor nacked
   before its lease expires, the next `Dequeue`/`Stats` reclaims it back to
   `ready` (at-least-once: the message may be delivered more than once, so
   consumers should be idempotent).

Ordering is FIFO by enqueue time among currently-available messages.

## HTTP service

Run the server:

```
go run ./cmd/safequeue -addr :8080 -data ./data
```

Flags: `-addr` (listen address), `-data` (data dir), `-visibility`,
`-max-attempts`, `-base-backoff`, `-max-backoff`.

### Endpoints

| Method & path  | Request body                              | Response                                  |
| -------------- | ----------------------------------------- | ----------------------------------------- |
| `POST /enqueue`| `{"payload":"...","idempotency_key":"..."}` (key optional) | `{"message":{...},"created":bool}` |
| `POST /dequeue`| _(none)_                                  | `200` leased message, or `404` if empty   |
| `POST /ack`    | `{"id":"..."}`                            | `{"ok":true}` (`404`/`409` on misuse)     |
| `POST /nack`   | `{"id":"..."}`                            | `{"dead":bool}` (`404`/`409` on misuse)   |
| `GET /stats`   | _(none)_                                  | `{"ready":N,"leased":N,"dead":N,...}`      |
| `GET /healthz` | _(none)_                                  | `{"ok":true}`                             |

Status codes: `400` invalid/missing fields, `404` unknown id or empty queue,
`405` wrong method, `409` message not in a leased state.

### Example

```
# enqueue (idempotent on key "k1")
curl -s -XPOST localhost:8080/enqueue \
  -d '{"payload":"resize-image:42","idempotency_key":"k1"}'

# dequeue -> {"id":"msg-0", ...}
ID=$(curl -s -XPOST localhost:8080/dequeue | sed -E 's/.*"id":"([^"]+)".*/\1/')

# ack
curl -s -XPOST localhost:8080/ack -d "{\"id\":\"$ID\"}"

# stats
curl -s localhost:8080/stats
```

## Storage format

The data directory holds a single append-only file, `queue.log`. Each line is a
JSON record describing one mutation (`enqueue`, `lease`, `ack`, `nack`,
`dead`). Replaying records in order reconstructs queue state. Writes are
flushed and `fsync`'d before an operation returns, so an acknowledged operation
is durable.

## Development

```
go build ./...
go vet ./...
go test ./...
```

Tests inject a fake clock and use millisecond backoffs, so the suite never
sleeps on the real wall clock.

## License

License: COCL 1.0. See the canonical COCL text distributed with Cognis Digital
projects.
