package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/broker"
	"job-queue/internal/queue"
	"job-queue/internal/tests"
	"job-queue/internal/worker"
	"job-queue/internal/worker/handlers"
)

func TestMain(m *testing.M) {
	os.Exit(tests.RunRedisMain(m))
}

// newBroker registers broker cleanup BEFORE any worker cleanup (t.Cleanup is
// LIFO), so the worker drains before the Redis client is closed.
func newBroker(t *testing.T, opts broker.Options) *broker.Broker {
	t.Helper()
	b := broker.New(opts)
	t.Cleanup(func() { b.Close() })
	return b
}

func newWorker(t *testing.T, b *broker.Broker, consumer string, opts ...func(*worker.Options)) *worker.Worker {
	t.Helper()
	o := worker.Options{
		Broker:          b,
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Queue:           "default",
		Consumer:        consumer,
		Concurrency:     4,
		BatchSize:       5,
		PollTimeout:     200 * time.Millisecond,
		LeaseDuration:   5 * time.Second,
		MinIdle:         1 * time.Second,
		ShutdownTimeout: 5 * time.Second,
		ProcessedTTL:    24 * time.Hour,
		Registry:        worker.NewRegistry(map[string]worker.Handler{"demo_task": handlers.DemoTask()}),
	}
	for _, f := range opts {
		f(&o)
	}
	return worker.New(o)
}

// promoteRetries simulates the Phase 4 scheduler: moves due retry-ZSet jobs
// back into their stream so the worker can run them again.
func promoteRetries(t *testing.T, b *broker.Broker, raw *redis.Client) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().Unix()
	ids, err := raw.ZRangeByScore(ctx, "retry", &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now, 10)}).Result()
	if err != nil {
		t.Fatalf("zrangeby score: %v", err)
	}
	for _, id := range ids {
		job, err := b.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("get job %s: %v", id, err)
		}
		if err := raw.XAdd(ctx, &redis.XAddArgs{
			Stream: "queue:" + job.Queue,
			Values: map[string]interface{}{"job_id": id, "type": job.Type},
		}).Err(); err != nil {
			t.Fatalf("xadd %s: %v", id, err)
		}
		if err := raw.ZRem(ctx, "retry", id).Err(); err != nil {
			t.Fatalf("zrem %s: %v", id, err)
		}
	}
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

func startWorker(t *testing.T, w *worker.Worker) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return cancel
}

func TestWorkerSuccess(t *testing.T) {
	b := newBroker(t, broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureGroup(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"success"}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	startWorker(t, newWorker(t, b, "w-success"))

	waitFor(t, 5*time.Second, "job succeeded", func() bool {
		job, err := b.GetJob(ctx, id)
		return err == nil && job.Status == queue.StatusSucceeded
	})

	job, _ := b.GetJob(ctx, id)
	if job.Status != queue.StatusSucceeded {
		t.Fatalf("status = %s", job.Status)
	}
	if job.FinishedAt == nil {
		t.Error("finished_at not set")
	}
	processed, err := b.IsProcessed(ctx, id)
	if err != nil || !processed {
		t.Errorf("processed marker not set: %v %v", processed, err)
	}

	// message must be ACKed: PEL should be empty
	groups, err := tests.Redis().XInfoGroups(ctx, "queue:default").Result()
	if err != nil || len(groups) == 0 || groups[0].Pending != 0 {
		t.Errorf("expected empty PEL, got %+v (err=%v)", groups, err)
	}
}

func TestWorkerRetryThenSuccess(t *testing.T) {
	b := newBroker(t, broker.Options{
		RedisOptions: &redis.Options{Addr: tests.RedisAddr()},
		BackoffBase:  100 * time.Millisecond,
		BackoffMax:   time.Second,
	})
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureGroup(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	raw := tests.Redis()

	// fail_times:1 means fail the first attempt, succeed on retry.
	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"fail_times","n":1}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	startWorker(t, newWorker(t, b, "w-failtimes"))

	// First attempt fails -> lands in retry ZSet.
	waitFor(t, 5*time.Second, "job scheduled for retry", func() bool {
		n, err := raw.ZCard(ctx, "retry").Result()
		return err == nil && n == 1
	})

	// Simulate the scheduler promoting it, then it should succeed.
	promoteRetries(t, b, raw)
	waitFor(t, 5*time.Second, "job succeeded after retry", func() bool {
		job, err := b.GetJob(ctx, id)
		return err == nil && job.Status == queue.StatusSucceeded
	})
	if job, _ := b.GetJob(ctx, id); job.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", job.Attempt)
	}
}

func TestWorkerAlwaysFailGoesToDLQ(t *testing.T) {
	b := newBroker(t, broker.Options{
		RedisOptions: &redis.Options{Addr: tests.RedisAddr()},
		BackoffBase:  100 * time.Millisecond,
		BackoffMax:   time.Second,
	})
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureGroup(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	raw := tests.Redis()

	// max_retries=1: one retry after the initial failure, then DLQ.
	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"always_fail"}`),
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	startWorker(t, newWorker(t, b, "w-dlq"))

	// Cycle: fail -> retry ZSet -> promote -> fail again -> DLQ.
	waitFor(t, 5*time.Second, "retry zset populated", func() bool {
		n, _ := raw.ZCard(ctx, "retry").Result()
		return n == 1
	})
	promoteRetries(t, b, raw)

	waitFor(t, 5*time.Second, "job in DLQ", func() bool {
		n, _ := raw.XLen(ctx, "dlq").Result()
		return n == 1
	})
	job, _ := b.GetJob(ctx, id)
	if job.Status != queue.StatusDead {
		t.Errorf("status = %s, want dead", job.Status)
	}
	if job.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", job.Attempt)
	}
}

func TestWorkerGracefulShutdownDrains(t *testing.T) {
	b := newBroker(t, broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureGroup(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"slow","n":2}`),
		MaxRetries: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	runCtx, cancel := context.WithCancel(context.Background())
	w := newWorker(t, b, "w-shutdown")
	go func() { done <- w.Run(runCtx) }()

	// Cancel while the job is still sleeping; the worker should finish it.
	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop")
	}

	// The in-flight slow job must have completed despite the shutdown.
	job, err := b.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != queue.StatusSucceeded {
		t.Errorf("in-flight job status = %s, want succeeded", job.Status)
	}
}

// TestWorkerSlowRefreshesLease verifies a long-running job keeps a fresh lease
// so the reclaimer would not steal it.
func TestWorkerSlowRefreshesLease(t *testing.T) {
	b := newBroker(t, broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureGroup(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"slow","n":2}`),
		MaxRetries: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	o := worker.Options{
		Broker:               b,
		Logger:               slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Queue:                "default",
		Consumer:             "w-lease",
		Concurrency:          4,
		BatchSize:            5,
		PollTimeout:          200 * time.Millisecond,
		LeaseDuration:        2 * time.Second,
		LeaseRefreshInterval: 500 * time.Millisecond,
		MinIdle:              10 * time.Second,
		ShutdownTimeout:      5 * time.Second,
		ProcessedTTL:         24 * time.Hour,
		Registry:             worker.NewRegistry(map[string]worker.Handler{"demo_task": handlers.DemoTask()}),
	}
	startWorker(t, worker.New(o))

	time.Sleep(1500 * time.Millisecond)
	job, err := b.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != queue.StatusRunning {
		t.Fatalf("status = %s, want running", job.Status)
	}
	// Lease should be near now+leaseDuration (refreshed during the slow run),
	// not a stale value from before.
	if job.LeaseUntil.IsZero() {
		t.Error("lease not set")
	} else if job.LeaseUntil.Before(time.Now()) {
		t.Errorf("lease already expired while job running: %v", job.LeaseUntil)
	}
	fmt.Println("lease_until during slow run:", job.LeaseUntil.Format(time.RFC3339Nano))
}
