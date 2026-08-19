package scheduler

import (
	"context"
	"log/slog"
	"time"

	"job-queue/internal/broker"
)

type Options struct {
	Broker        *broker.Broker
	Logger        *slog.Logger
	LockKey       string
	LockTTL       time.Duration
	RenewInterval time.Duration
	TickInterval  time.Duration
	BatchSize     int64
	Instance      string
}

type Scheduler struct {
	broker *broker.Broker
	logger *slog.Logger
	opts   Options
}

func New(opts Options) *Scheduler {
	if opts.LockKey == "" {
		opts.LockKey = "scheduler"
	}
	if opts.LockTTL == 0 {
		opts.LockTTL = 10 * time.Second
	}
	if opts.RenewInterval == 0 {
		opts.RenewInterval = 3 * time.Second
	}
	if opts.TickInterval == 0 {
		opts.TickInterval = time.Second
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 100
	}
	if opts.Instance == "" {
		opts.Instance = "scheduler"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Scheduler{broker: opts.Broker, logger: opts.Logger, opts: opts}
}

func (s *Scheduler) Run(ctx context.Context) error {
	for {
		token := broker.LockToken(s.opts.Instance)
		acquired, err := s.broker.AcquireLock(ctx, s.opts.LockKey, token, s.opts.LockTTL)
		if err != nil {
			s.logger.Error("failed to acquire lock", "err", err)
			if !sleep(ctx, s.opts.RenewInterval) {
				return nil
			}
			continue
		}

		if acquired {
			s.logger.Info("became leader", "instance", s.opts.Instance)
			s.lead(ctx, token)

			_ = s.broker.ReleaseLock(context.Background(), s.opts.LockKey, token)
			if ctx.Err() != nil {
				return nil
			}
		} else {
			s.logger.Info("another scheduler holds the lock, standing by", "instance", s.opts.Instance)
		}

		if !sleep(ctx, s.opts.RenewInterval) {
			return nil
		}
	}
}

func (s *Scheduler) lead(ctx context.Context, token string) {
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go s.renewLoop(lctx, cancel, token)

	ticker := time.NewTicker(s.opts.TickInterval)
	defer ticker.Stop()
	summary := time.NewTicker(30 * time.Second)
	defer summary.Stop()

	for {
		select {
		case <-lctx.Done():
			return
		case <-ticker.C:
			s.promote(lctx)
		case <-summary.C:
			s.logBacklog(lctx)
		}
	}
}

func (s *Scheduler) renewLoop(ctx context.Context, cancel context.CancelFunc, token string) {
	ticker := time.NewTicker(s.opts.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := s.broker.RenewLock(ctx, s.opts.LockKey, token, s.opts.LockTTL)
			if err != nil {
				s.logger.Error("lock renewal failed", "err", err)
				continue
			}
			if !ok {
				s.logger.Warn("lost leadership (lock expired or taken over)", "instance", s.opts.Instance)
				cancel()
				return
			}
		}
	}
}

func (s *Scheduler) promote(ctx context.Context) {
	now := time.Now()
	for _, zset := range []string{broker.ZSetDelayed, broker.ZSetRetry} {
		n, err := s.broker.PromoteDue(ctx, zset, now, s.opts.BatchSize)
		if err != nil {
			s.logger.Error("promote failed", "zset", zset, "err", err)
			continue
		}
		if n > 0 {
			s.logger.Info("promoted jobs", "zset", zset, "count", n)
		}
	}
}

func (s *Scheduler) logBacklog(ctx context.Context) {
	stats, err := s.broker.Stats(ctx)
	if err != nil {
		s.logger.Error("failed to read backlog stats", "err", err)
		return
	}
	s.logger.Info("backlog", "delayed", stats.Delayed, "retry", stats.Retry, "dlq", stats.DLQ)
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
