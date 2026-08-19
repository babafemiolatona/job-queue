package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/broker"
	"job-queue/internal/queue"
	"job-queue/internal/tests"
)

func TestMain(m *testing.M) {
	os.Exit(tests.RunRedisMain(m))
}

func newBroker(t *testing.T) *broker.Broker {
	t.Helper()
	if err := tests.Redis().FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	return broker.New(broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
}

func TestEnqueueGetRoundtrip(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	id, err := b.Enqueue(ctx, &queue.Job{
		Type:       "demo_task",
		Queue:      "default",
		Payload:    json.RawMessage(`{"mode":"success"}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := b.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.ID != id {
		t.Errorf("id mismatch: %s != %s", got.ID, id)
	}
	if got.Type != "demo_task" || got.Queue != "default" || got.MaxRetries != 3 {
		t.Errorf("unexpected job fields: %+v", got)
	}
	if got.Status != queue.StatusPending {
		t.Errorf("expected status pending, got %s", got.Status)
	}
	if string(got.Payload) != `{"mode":"success"}` {
		t.Errorf("payload mismatch: %s", got.Payload)
	}
	if got.CreatedAt.IsZero() {
		t.Error("created_at should be set")
	}
}

func TestDedupReturnsSameID(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	first, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", DedupKey: "key-1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	second, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", DedupKey: "key-1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if first != second {
		t.Errorf("same dedup_key returned different ids: %s != %s", first, second)
	}

	other, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", DedupKey: "key-2"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if other == first {
		t.Error("different dedup_key returned the same id")
	}
}

func TestEnqueueDelayed(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()
	runAt := time.Now().Add(time.Hour)

	j := &queue.Job{Type: "demo_task", Queue: "default"}
	if err := b.EnqueueDelayed(ctx, j, runAt); err != nil {
		t.Fatalf("enqueue delayed: %v", err)
	}
	if j.ID == "" {
		t.Fatal("delayed job got no id")
	}

	got, err := b.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Status != queue.StatusScheduled {
		t.Errorf("expected status scheduled, got %s", got.Status)
	}
	if !got.RunAfter.Equal(runAt) {
		t.Errorf("run_after mismatch: %v != %v", got.RunAfter, runAt)
	}

	score, err := tests.Redis().ZScore(ctx, "delayed", j.ID).Result()
	if err != nil {
		t.Fatalf("delayed zset lookup: %v", err)
	}
	if int64(score) != runAt.Unix() {
		t.Errorf("zset score = %d, want %d", int64(score), runAt.Unix())
	}
}

func TestScheduleRetry(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	id, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", MaxRetries: 3})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, err := b.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if err := b.ScheduleRetry(ctx, job, fmt.Errorf("boom")); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}

	if job.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", job.Attempt)
	}
	if job.Status != queue.StatusFailed {
		t.Errorf("status = %s, want failed", job.Status)
	}
	if job.Error != "boom" {
		t.Errorf("error = %q, want boom", job.Error)
	}

	score, err := tests.Redis().ZScore(ctx, "retry", id).Result()
	if err != nil {
		t.Fatalf("retry zset lookup: %v", err)
	}

	want := time.Now().Add(time.Second).Unix()
	if diff := int64(score) - want; diff < -1 || diff > 1 {
		t.Errorf("retry score = %d, want ~%d", int64(score), want)
	}
}

func TestEnqueueDLQ(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	id, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", Queue: "default"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, err := b.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if err := b.EnqueueDLQ(ctx, job, fmt.Errorf("gave up")); err != nil {
		t.Fatalf("enqueue dlq: %v", err)
	}

	if job.Status != queue.StatusDead {
		t.Errorf("status = %s, want dead", job.Status)
	}
	if job.Error != "gave up" {
		t.Errorf("error = %q, want gave up", job.Error)
	}
	if job.FinishedAt == nil {
		t.Error("finished_at should be set on dead job")
	}

	n, err := tests.Redis().XLen(ctx, "dlq").Result()
	if err != nil {
		t.Fatalf("dlq length: %v", err)
	}
	if n != 1 {
		t.Errorf("dlq length = %d, want 1", n)
	}
}

func TestReclaimStale(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()
	queueName := "reclaim"

	if err := b.EnsureGroup(ctx, queueName); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	id, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", Queue: queueName})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := tests.Redis().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "consumers:reclaim",
		Consumer: "worker-A",
		Streams:  []string{"queue:reclaim", ">"},
		Count:    1,
	}).Result(); err != nil {
		t.Fatalf("read group: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	msgs, err := b.ReclaimStale(ctx, queueName, "worker-B", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("reclaim stale: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("reclaimed %d messages, want 1", len(msgs))
	}
	if msgs[0].Values["job_id"] != id {
		t.Errorf("reclaimed job_id = %v, want %s", msgs[0].Values["job_id"], id)
	}
}

func TestStats(t *testing.T) {
	b := newBroker(t)
	ctx := context.Background()

	if _, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := b.EnqueueDelayed(ctx, &queue.Job{Type: "demo_task"}, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("enqueue delayed: %v", err)
	}

	st, err := b.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Delayed != 1 {
		t.Errorf("delayed = %d, want 1", st.Delayed)
	}

	found := false
	for _, q := range st.Queues {
		if q.Queue == "default" {
			found = true
			if q.Ready != 2 {
				t.Errorf("default ready = %d, want 2", q.Ready)
			}
		}
	}
	if !found {
		t.Error("default queue missing from stats")
	}
}
