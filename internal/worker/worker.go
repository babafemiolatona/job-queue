package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"job-queue/internal/broker"
	"job-queue/internal/queue"

	"github.com/redis/go-redis/v9"
)

type Options struct {
	Broker               *broker.Broker
	Logger               *slog.Logger
	Queue                string
	Consumer             string
	Concurrency          int
	BatchSize            int
	PollTimeout          time.Duration
	LeaseDuration        time.Duration
	LeaseRefreshInterval time.Duration
	ReclaimInterval      time.Duration
	MinIdle              time.Duration
	ShutdownTimeout      time.Duration
	ProcessedTTL         time.Duration
	Registry             *Registry
}

type Worker struct {
	broker   *broker.Broker
	logger   *slog.Logger
	queue    string
	consumer string
	opts     Options
}

func New(opts Options) *Worker {
	if opts.Queue == "" {
		opts.Queue = "default"
	}
	if opts.Consumer == "" {
		opts.Consumer = "worker"
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 10
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 10
	}
	if opts.PollTimeout <= 0 {
		opts.PollTimeout = 2 * time.Second
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = 60 * time.Second
	}
	if opts.LeaseRefreshInterval <= 0 {
		opts.LeaseRefreshInterval = 15 * time.Second
	}
	if opts.ReclaimInterval == 0 {
		opts.ReclaimInterval = 30 * time.Second
	}
	if opts.MinIdle <= 0 {
		opts.MinIdle = 60 * time.Second
	}
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = 30 * time.Second
	}
	if opts.ProcessedTTL == 0 {
		opts.ProcessedTTL = 24 * time.Hour
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	return &Worker{
		broker:   opts.Broker,
		logger:   opts.Logger,
		queue:    opts.Queue,
		consumer: opts.Consumer,
		opts:     opts,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	go w.reclaimLoop(ctx)

	sem := make(chan struct{}, w.opts.Concurrency)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker shutting down, draining in-flight jobs")
			return w.drain(&wg)
		default:
		}

		msgs, err := w.broker.ReadGroup(ctx, w.queue, w.consumer, int64(w.opts.BatchSize), w.opts.PollTimeout)
		if err != nil {
			w.logger.Error("read group failed", "err", err)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		for _, m := range msgs {
			select {
			case <-ctx.Done():
				return w.drain(&wg)
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(m redis.XMessage) {
				defer wg.Done()
				defer func() { <-sem }()
				w.process(m)
			}(m)
		}
	}
}

func (w *Worker) drain(wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		w.logger.Info("worker drained in-flight jobs")
		return nil
	case <-time.After(w.opts.ShutdownTimeout):
		w.logger.Warn("shutdown timeout, abandoning in-flight jobs to reclaimer")
		return nil
	}
}

func (w *Worker) process(m redis.XMessage) {
	ctx := context.Background()

	id, _ := m.Values["job_id"].(string)
	if id == "" {
		w.logger.Error("message missing job_id", "stream_id", m.ID)
		_ = w.broker.Ack(ctx, w.queue, m.ID)
		return
	}

	job, err := w.broker.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			w.logger.Warn("job metadata missing, acking", "job_id", id)
			_ = w.broker.Ack(ctx, w.queue, m.ID)
			return
		}
		w.logger.Error("failed to load job", "job_id", id, "err", err)
		return
	}

	if done, err := w.broker.IsProcessed(ctx, id); err == nil && done {
		w.logger.Info("duplicate delivery, skipping", "job_id", id)
		_ = w.broker.Ack(ctx, w.queue, m.ID)
		return
	}

	if err := w.broker.MarkRunning(ctx, job, time.Now().Add(w.opts.LeaseDuration)); err != nil {
		w.logger.Error("failed to mark running", "job_id", id, "err", err)
	}

	runErr := w.runWithLease(ctx, job, func() error {
		h, ok := w.opts.Registry.Lookup(job.Type)
		if !ok {
			return fmt.Errorf("unknown job type %q", job.Type)
		}
		return w.safeHandle(h, job)
	})

	if runErr == nil {
		if _, err := w.broker.MarkProcessed(ctx, id, w.opts.ProcessedTTL); err != nil {
			w.logger.Error("failed to mark processed", "job_id", id, "err", err)
		}
		if err := w.broker.MarkSucceeded(ctx, job); err != nil {
			w.logger.Error("failed to mark succeeded", "job_id", id, "err", err)
		}
		w.logger.Info("job succeeded", "job_id", id, "type", job.Type)
	} else {
		if job.Attempt >= job.MaxRetries {
			if err := w.broker.EnqueueDLQ(ctx, job, runErr); err != nil {
				w.logger.Error("failed to enqueue dlq", "job_id", id, "err", err)
			}
			w.logger.Warn("job moved to DLQ", "job_id", id, "type", job.Type, "attempts", job.Attempt+1)
		} else {
			if err := w.broker.ScheduleRetry(ctx, job, runErr); err != nil {
				w.logger.Error("failed to schedule retry", "job_id", id, "err", err)
			}
			w.logger.Warn("job scheduled for retry", "job_id", id, "type", job.Type, "attempt", job.Attempt)
		}
	}

	if err := w.broker.Ack(ctx, w.queue, m.ID); err != nil {
		w.logger.Error("failed to ack", "job_id", id, "stream_id", m.ID, "err", err)
	}
}

func (w *Worker) runWithLease(ctx context.Context, job *queue.Job, run func() error) error {
	refreshCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		t := time.NewTicker(w.opts.LeaseRefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-refreshCtx.Done():
				return
			case <-t.C:
				if err := w.broker.SetLease(refreshCtx, job.ID, time.Now().Add(w.opts.LeaseDuration)); err != nil {
					w.logger.Warn("lease refresh failed", "job_id", job.ID, "err", err)
				}
			}
		}
	}()

	return run()
}

func (w *Worker) safeHandle(h Handler, job *queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
			w.logger.Error("handler panicked", "job_id", job.ID, "type", job.Type, "panic", r)
		}
	}()
	return h(context.Background(), job)
}

func (w *Worker) reclaimLoop(ctx context.Context) {
	t := time.NewTicker(w.opts.ReclaimInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			msgs, err := w.broker.ReclaimStale(ctx, w.queue, w.consumer, w.opts.MinIdle, int64(w.opts.BatchSize*2))
			if err != nil {
				w.logger.Error("reclaim failed", "err", err)
				continue
			}
			if len(msgs) > 0 {
				w.logger.Info("reclaimed stale messages", "count", len(msgs))
			}
		}
	}
}
