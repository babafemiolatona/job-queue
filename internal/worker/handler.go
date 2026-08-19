package worker

import (
	"context"
	"job-queue/internal/queue"
)

type Handler func(ctx context.Context, job *queue.Job) error

func NewRegistry(handlers map[string]Handler) *Registry {
	r := &Registry{handlers: map[string]Handler{}}
	for name, handler := range handlers {
		r.handlers[name] = handler
	}
	return r
}

type Registry struct {
	handlers map[string]Handler
}

func (r *Registry) Lookup(jobType string) (Handler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}
