// Package httpapi exposes a queue over a small JSON/HTTP interface.
//
// Endpoints:
//
//	POST /enqueue  {"payload":"...","idempotency_key":"..."} -> message + created flag
//	POST /dequeue                                            -> leased message (404 if empty)
//	POST /ack      {"id":"..."}                              -> {"ok":true}
//	POST /nack     {"id":"..."}                              -> {"dead":bool}
//	GET  /stats                                              -> stats snapshot
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/cognis-digital/safequeue/queue"
)

// Server wraps a queue and serves the JSON/HTTP API.
type Server struct {
	q   *queue.Queue
	mux *http.ServeMux
}

// New builds a Server backed by q and registers its routes.
func New(q *queue.Queue) *Server {
	s := &Server{q: q, mux: http.NewServeMux()}
	s.mux.HandleFunc("/enqueue", s.handleEnqueue)
	s.mux.HandleFunc("/dequeue", s.handleDequeue)
	s.mux.HandleFunc("/ack", s.handleAck)
	s.mux.HandleFunc("/nack", s.handleNack)
	s.mux.HandleFunc("/stats", s.handleStats)
	s.mux.HandleFunc("/healthz", s.handleHealth)
	return s
}

// ServeHTTP makes Server an http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type enqueueRequest struct {
	Payload        string `json:"payload"`
	IdempotencyKey string `json:"idempotency_key"`
}

type enqueueResponse struct {
	Message *queue.Message `json:"message"`
	Created bool           `json:"created"`
}

type idRequest struct {
	ID string `json:"id"`
}

type nackResponse struct {
	Dead bool `json:"dead"`
}

type okResponse struct {
	OK bool `json:"ok"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req enqueueRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Payload == "" {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}
	m, created, err := s.q.Enqueue(req.Payload, req.IdempotencyKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, enqueueResponse{Message: m, Created: created})
}

func (s *Server) handleDequeue(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	m, err := s.q.Dequeue()
	if err != nil {
		if errors.Is(err, queue.ErrEmpty) {
			writeError(w, http.StatusNotFound, "no message available")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req idRequest
	if !decode(w, r, &req) {
		return
	}
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if err := s.q.Ack(req.ID); err != nil {
		writeOpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, okResponse{OK: true})
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req idRequest
	if !decode(w, r, &req) {
		return
	}
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	dead, err := s.q.Nack(req.ID)
	if err != nil {
		writeOpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nackResponse{Dead: dead})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, s.q.Stats())
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, okResponse{OK: true})
}

// writeOpError maps queue errors to appropriate HTTP status codes.
func writeOpError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, queue.ErrNotLeased):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("Allow", method)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return false
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
