package broker_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/broker"
	"job-queue/internal/queue"
	"job-queue/internal/tests"
)

func TestAcquireLockAndFencing(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	ok, err := b.AcquireLock(ctx, "test", "a", time.Second)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}

	// A second instance cannot acquire while held.
	ok, err = b.AcquireLock(ctx, "test", "b", time.Second)
	if err != nil || ok {
		t.Fatalf("second acquire should fail: ok=%v err=%v", ok, err)
	}

	// Wrong token cannot renew or release (fencing).
	renewed, err := b.RenewLock(ctx, "test", "b", time.Second)
	if err != nil || renewed {
		t.Fatalf("wrong-token renew should fail: %v %v", renewed, err)
	}
	if err := b.ReleaseLock(ctx, "test", "b"); err != nil {
		t.Fatalf("wrong-token release: %v", err)
	}

	// Correct token renews.
	renewed, err = b.RenewLock(ctx, "test", "a", time.Second)
	if err != nil || !renewed {
		t.Fatalf("correct renew: %v %v", renewed, err)
	}

	// Correct token releases; then a new instance can acquire.
	if err := b.ReleaseLock(ctx, "test", "a"); err != nil {
		t.Fatalf("correct release: %v", err)
	}
	ok, err = b.AcquireLock(ctx, "test", "c", time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire after release: ok=%v err=%v", ok, err)
	}
}

func TestPromoteDelayed(t *testing.T) {
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

	n, err := b.PromoteDue(ctx, broker.ZSetDelayed, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("promoted = %d, want 1", n)
	}

	// Job is now in the stream and pending.
	length, _ := tests.Redis().XLen(ctx, "queue:default").Result()
	if length != 1 {
		t.Fatalf("stream length = %d, want 1", length)
	}
	got, _ := b.GetJob(ctx, j.ID)
	if got.Status != queue.StatusPending {
		t.Errorf("status = %s, want pending", got.Status)
	}
	zlen, _ := tests.Redis().ZCard(ctx, broker.ZSetDelayed).Result()
	if zlen != 0 {
		t.Errorf("delayed zset size = %d, want 0", zlen)
	}
}

func TestPromoteRetry(t *testing.T) {
	b := broker.New(broker.Options{
		RedisOptions: &redis.Options{Addr: tests.RedisAddr()},
		BackoffBase:  100 * time.Millisecond,
		BackoffMax:   time.Second,
	})
	defer b.Close()
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

	// Wait for the tiny backoff to elapse, then promote.
	time.Sleep(200 * time.Millisecond)
	n, err := b.PromoteDue(ctx, broker.ZSetRetry, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("promoted = %d, want 1", n)
	}
	length, _ := tests.Redis().XLen(ctx, "queue:default").Result()
	if length < 1 {
		t.Fatalf("stream length = %d, want >= 1", length)
	}
}

func TestRequeueDLQ(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"always_fail"}`),
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	job, _ := b.GetJob(ctx, id)
	if err := b.EnqueueDLQ(ctx, job, nil); err != nil {
		t.Fatal(err)
	}

	if err := b.RequeueDLQ(ctx, id); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	// Back in the stream, reset for a fresh run.
	length, _ := tests.Redis().XLen(ctx, "queue:default").Result()
	if length < 1 {
		t.Fatalf("stream length = %d, want >= 1", length)
	}
	got, _ := b.GetJob(ctx, id)
	if got.Status != queue.StatusPending || got.Attempt != 0 || got.Error != "" {
		t.Errorf("job not reset: %+v", got)
	}
	if dlqLen, _ := tests.Redis().XLen(ctx, "dlq").Result(); dlqLen != 0 {
		t.Errorf("dlq length = %d, want 0", dlqLen)
	}

	// Requeuing again must fail since it is no longer in the DLQ.
	if err := b.RequeueDLQ(ctx, id); err == nil {
		t.Error("expected ErrNotInDLQ on second requeue")
	} else if err != broker.ErrNotInDLQ {
		t.Errorf("unexpected error: %v", err)
	}
}
