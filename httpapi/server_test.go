package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cognis-digital/safequeue/queue"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	q, err := queue.Open(t.TempDir(), queue.Options{
		VisibilityTimeout: 10 * 1e9, // 10s
		MaxAttempts:       2,
		BaseBackoff:       1e6, // 1ms
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return New(q)
}

func do(t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestHTTPEnqueueDequeueAckFlow(t *testing.T) {
	s := newTestServer(t)

	// Enqueue.
	rec := do(t, s, http.MethodPost, "/enqueue", map[string]string{"payload": "job-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("enqueue status %d: %s", rec.Code, rec.Body.String())
	}
	var enq enqueueResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &enq); err != nil {
		t.Fatalf("decode enqueue: %v", err)
	}
	if !enq.Created || enq.Message.Payload != "job-1" {
		t.Fatalf("unexpected enqueue response: %+v", enq)
	}

	// Dequeue.
	rec = do(t, s, http.MethodPost, "/dequeue", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("dequeue status %d", rec.Code)
	}
	var msg queue.Message
	if err := json.Unmarshal(rec.Body.Bytes(), &msg); err != nil {
		t.Fatalf("decode dequeue: %v", err)
	}
	if msg.ID != enq.Message.ID || msg.State != queue.StateLeased {
		t.Fatalf("unexpected dequeue: %+v", msg)
	}

	// Ack.
	rec = do(t, s, http.MethodPost, "/ack", map[string]string{"id": msg.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("ack status %d: %s", rec.Code, rec.Body.String())
	}

	// Dequeue now empty -> 404.
	rec = do(t, s, http.MethodPost, "/dequeue", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on empty dequeue, got %d", rec.Code)
	}
}

func TestHTTPIdempotency(t *testing.T) {
	s := newTestServer(t)
	body := map[string]string{"payload": "p", "idempotency_key": "k1"}

	rec := do(t, s, http.MethodPost, "/enqueue", body)
	var first enqueueResponse
	json.Unmarshal(rec.Body.Bytes(), &first)
	if !first.Created {
		t.Fatalf("first enqueue should be created")
	}

	rec = do(t, s, http.MethodPost, "/enqueue", body)
	var second enqueueResponse
	json.Unmarshal(rec.Body.Bytes(), &second)
	if second.Created {
		t.Fatalf("duplicate key should not be created")
	}
	if second.Message.ID != first.Message.ID {
		t.Fatalf("dedup id mismatch")
	}
}

func TestHTTPNackDeadLetter(t *testing.T) {
	s := newTestServer(t) // MaxAttempts=2

	do(t, s, http.MethodPost, "/enqueue", map[string]string{"payload": "x"})

	// Attempt 1: dequeue + nack -> not dead.
	rec := do(t, s, http.MethodPost, "/dequeue", nil)
	var m queue.Message
	json.Unmarshal(rec.Body.Bytes(), &m)
	rec = do(t, s, http.MethodPost, "/nack", map[string]string{"id": m.ID})
	var nr nackResponse
	json.Unmarshal(rec.Body.Bytes(), &nr)
	if nr.Dead {
		t.Fatalf("should not be dead after attempt 1")
	}

	// Stats: 0 ready visible yet (backoff), but message still counted in total.
	rec = do(t, s, http.MethodGet, "/stats", nil)
	var st queue.Stats
	json.Unmarshal(rec.Body.Bytes(), &st)
	if st.Total != 1 {
		t.Fatalf("expected total 1, got %+v", st)
	}
}

func TestHTTPAckErrors(t *testing.T) {
	s := newTestServer(t)

	// Ack unknown id -> 404.
	rec := do(t, s, http.MethodPost, "/ack", map[string]string{"id": "nope"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	// Ack a ready (not leased) message -> 409.
	rec = do(t, s, http.MethodPost, "/enqueue", map[string]string{"payload": "y"})
	var enq enqueueResponse
	json.Unmarshal(rec.Body.Bytes(), &enq)
	rec = do(t, s, http.MethodPost, "/ack", map[string]string{"id": enq.Message.ID})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 for non-leased ack, got %d", rec.Code)
	}
}

func TestHTTPValidation(t *testing.T) {
	s := newTestServer(t)

	// Missing payload.
	rec := do(t, s, http.MethodPost, "/enqueue", map[string]string{})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing payload, got %d", rec.Code)
	}

	// Wrong method.
	rec = do(t, s, http.MethodGet, "/enqueue", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}

	// Missing id on ack.
	rec = do(t, s, http.MethodPost, "/ack", map[string]string{})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing id, got %d", rec.Code)
	}
}

func TestHTTPStats(t *testing.T) {
	s := newTestServer(t)
	do(t, s, http.MethodPost, "/enqueue", map[string]string{"payload": "a"})
	do(t, s, http.MethodPost, "/enqueue", map[string]string{"payload": "b"})

	rec := do(t, s, http.MethodGet, "/stats", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats status %d", rec.Code)
	}
	var st queue.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if st.Ready != 2 || st.Total != 2 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}
