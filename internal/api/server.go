package api

import (
	"job-queue/internal/broker"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	broker *broker.Broker
	logger *slog.Logger
	router http.Handler
}

func NewServer(broker *broker.Broker, logger *slog.Logger) *Server {
	s := &Server{broker: broker, logger: logger}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(s.requestLogger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Post("/jobs", s.createJob)
	r.Get("/jobs/{id}", s.getJob)
	r.Get("/stats", s.getStats)
	r.Method(http.MethodGet, "/metrics", promhttp.Handler())

	s.router = r
	return s
}

func (s *Server) Handler() http.Handler {
	return s.router
}
