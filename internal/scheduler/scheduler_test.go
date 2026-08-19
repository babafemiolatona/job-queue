package scheduler_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/broker"
	"job-queue/internal/queue"
	"job-queue/internal/scheduler"
	"job-queue/internal/tests"
)

func TestMain(m *testing.M) {
	os.Exit(tests.RunRedisMain(m))
}

func newBroker(t *testing.T) *broker.Broker {
	t.Helper()
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	b := broker.New(broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
	t.Cleanup(func() { b.Close() })
	return b
}

func newScheduler(b *broker.Broker, instance string) *scheduler.Scheduler {
	return scheduler.New(scheduler.Options{
		Broker:        b,
		Logger:        slog.New(slog.NewTextHandler(os.Stderr, nil)),
		LockKey:       "scheduler",
		LockTTL:       2 * time.Second,
		RenewInterval: 200 * time.Millisecond,
		TickInterval:  100 * time.Millisecond,
		BatchSize:     10,
		Instance:      instance,
	})
}

func runScheduler(t *testing.T, s *scheduler.Scheduler) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	})
	return cancel
}

func TestSchedulerPromotesDelayed(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	j := &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"success"}`),
		MaxRetries: 3,
	}
	if err := b.EnqueueDelayed(ctx, j, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}

	runScheduler(t, newScheduler(b, "inst-1"))

	deadline := time.Now().Add(5 * time.Second)
	for {
		length, _ := tests.Redis().XLen(ctx, "queue:default").Result()
		if length == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for delayed job to be promoted")
		}
		time.Sleep(50 * time.Millisecond)
	}

	got, _ := b.GetJob(ctx, j.ID)
	if got.Status != queue.StatusPending {
		t.Errorf("status = %s, want pending", got.Status)
	}
}

func TestSchedulerPromotesRetry(t *testing.T) {
	b := broker.New(broker.Options{
		RedisOptions: &redis.Options{Addr: tests.RedisAddr()},
		BackoffBase:  100 * time.Millisecond,
		BackoffMax:   time.Second,
	})
	t.Cleanup(func() { b.Close() })
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"always_fail"}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	job, _ := b.GetJob(ctx, id)
	if err := b.ScheduleRetry(ctx, job, nil); err != nil {
		t.Fatal(err)
	}

	runScheduler(t, newScheduler(b, "inst-2"))

	deadline := time.Now().Add(5 * time.Second)
	for {
		length, _ := tests.Redis().XLen(ctx, "queue:default").Result()
		zlen, _ := tests.Redis().ZCard(ctx, broker.ZSetRetry).Result()
		if length >= 1 && zlen == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: stream=%d retry=%d", length, zlen)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSchedulerLeaderElectionAndRelease(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	cancel1 := runScheduler(t, newScheduler(b, "inst-a"))
	runScheduler(t, newScheduler(b, "inst-b"))

	// Eventually exactly one of the two holds the lock.
	waitFor(t, 5*time.Second, "lock acquired", func() bool {
		n, _ := tests.Redis().Exists(ctx, "lock:scheduler").Result()
		return n == 1
	})

	// Stop the leader; its token must be released and the other takes over.
	cancel1()
	waitFor(t, 5*time.Second, "lock transferred to second scheduler", func() bool {
		exists, _ := tests.Redis().Exists(ctx, "lock:scheduler").Result()
		return exists == 1
	})
}

func TestSchedulerReleasesLockOnExit(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	cancel := runScheduler(t, newScheduler(b, "inst-c"))
	waitFor(t, 5*time.Second, "lock acquired", func() bool {
		n, _ := tests.Redis().Exists(ctx, "lock:scheduler").Result()
		return n == 1
	})

	cancel()
	waitFor(t, 5*time.Second, "lock released on exit", func() bool {
		n, _ := tests.Redis().Exists(ctx, "lock:scheduler").Result()
		return n == 0
	})
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
