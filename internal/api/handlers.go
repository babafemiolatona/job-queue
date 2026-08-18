package api

import (
	"encoding/json"
	"errors"
	"job-queue/internal/queue"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
)

const defaultMaxRetries = 3

type createJobRequest struct {
	Type       string          `json:"type"`
	Queue      string          `json:"queue"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries int             `json:"max_retries"`
	RunAfter   *time.Time      `json:"run_after"`
	DedupKey   string          `json:"dedup_key"`
}

type createJobResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if req.Payload == nil {
		req.Payload = json.RawMessage("{}")
	}
	if req.MaxRetries <= 0 {
		req.MaxRetries = defaultMaxRetries
	}

	job := &queue.Job{
		Type:       req.Type,
		Queue:      req.Queue,
		Payload:    req.Payload,
		MaxRetries: req.MaxRetries,
		DedupKey:   req.DedupKey,
	}
	if req.RunAfter != nil {
		job.RunAfter = *req.RunAfter
	}

	var id string
	var err error
	switch {
	case req.RunAfter != nil && req.RunAfter.After(time.Now()):
		err = s.broker.EnqueueDelayed(r.Context(), job, *req.RunAfter)
		id = job.ID
	default:
		id, err = s.broker.Enqueue(r.Context(), job)
	}

	if err != nil {
		s.logger.Error("failed to enqueue job", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}

	if id != job.ID {
		existing, gerr := s.broker.GetJob(r.Context(), id)
		if gerr == nil {
			writeJSON(w, http.StatusOK, createJobResponse{ID: id, Status: string(existing.Status)})
			return
		}
		s.logger.Warn("dedup hit but failed to load existing job", "id", id, "err", gerr)
	}

	writeJSON(w, http.StatusCreated, createJobResponse{ID: id, Status: string(job.Status)})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := s.broker.GetJob(r.Context(), id)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		s.logger.Error("failed to get job", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.broker.Stats(r.Context())
	if err != nil {
		s.logger.Error("failed to get stats", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}
	writeJSON(w, http.StatusOK, stats)
}
