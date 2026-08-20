package metrics_test

import (
	"context"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"job-queue/internal/broker"
	"job-queue/internal/metrics"
	"job-queue/internal/queue"
	"job-queue/internal/tests"
)

func TestMain(m *testing.M) {
	os.Exit(tests.RunRedisMain(m))
}

func TestCounterIncrements(t *testing.T) {
	m := metrics.New()

	m.Enqueued.WithLabelValues("default").Inc()
	m.Enqueued.WithLabelValues("default").Inc()
	m.Succeeded.WithLabelValues("demo_task").Inc()
	m.Dead.WithLabelValues("demo_task").Inc()

	if n := testutil.ToFloat64(m.Enqueued.WithLabelValues("default")); n != 2 {
		t.Errorf("enqueued = %v, want 2", n)
	}
	if n := testutil.ToFloat64(m.Succeeded.WithLabelValues("demo_task")); n != 1 {
		t.Errorf("succeeded = %v, want 1", n)
	}
	if n := testutil.ToFloat64(m.Dead.WithLabelValues("demo_task")); n != 1 {
		t.Errorf("dead = %v, want 1", n)
	}
}

func TestQueueCollector(t *testing.T) {
	if err := tests.Redis().FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	b := broker.New(broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
	defer b.Close()
	ctx := context.Background()

	if _, err := b.Enqueue(ctx, &queue.Job{Type: "demo_task", Queue: "default"}); err != nil {
		t.Fatal(err)
	}

	m := metrics.New()
	m.Registry.MustRegister(metrics.NewQueueCollector(b.Stats))

	// queue_depth should be non-zero after the enqueue.
	count, err := testutil.GatherAndCount(m.Registry, "queue_depth")
	if err != nil {
		t.Fatal(err)
	}
	if count < 1 {
		t.Errorf("queue_depth samples = %d, want >= 1", count)
	}
}

func TestProcessingHistogram(t *testing.T) {
	m := metrics.New()
	for i := 0; i < 3; i++ {
		m.ProcessingTime.WithLabelValues("demo_task").Observe(0.25)
	}
	count, err := testutil.GatherAndCount(m.Registry, "job_processing_seconds")
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Error("no histogram samples recorded")
	}
}
