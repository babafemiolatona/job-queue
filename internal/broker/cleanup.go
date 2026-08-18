package broker

import (
	"context"
	"log/slog"
	"time"

	"job-queue/internal/queue"
)

func (b *Broker) CleanupLoop(ctx context.Context, interval time.Duration) {
	if interval == 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := b.SweepTerminalJobs(ctx); err != nil {
				slog.Error("cleanup sweep failed", "err", err)
			}
		}
	}
}

func (b *Broker) SweepTerminalJobs(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := b.rdb.Scan(ctx, cursor, "job:*", 100).Result()
		if err != nil {
			return err
		}
		for _, key := range keys {
			status, err := b.rdb.HGet(ctx, key, "status").Result()
			if err != nil {
				continue
			}
			if isTerminal(queue.Status(status)) {
				ttl := jobHashTTL
				if queue.Status(status) == queue.StatusDead {
					ttl = jobHashTTLDead
				}
				b.rdb.Expire(ctx, key, ttl)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

func isTerminal(s queue.Status) bool {
	return s == queue.StatusSucceeded || s == queue.StatusFailed || s == queue.StatusDead
}
