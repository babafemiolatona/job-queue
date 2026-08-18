package broker

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/queue"
)

func (b *Broker) Enqueue(ctx context.Context, j *queue.Job) (string, error) {
	if j.ID == "" {
		j.ID = queue.NewID()
	}
	if j.Queue == "" {
		j.Queue = defaultQueue
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now().UTC()
	}
	if j.Status == "" {
		j.Status = queue.StatusPending
	}
	if j.RunAfter.IsZero() {
		j.RunAfter = time.Now().UTC()
	}

	if j.DedupKey != "" {
		ok, err := b.rdb.SetNX(ctx, dedupKey(j.DedupKey), j.ID, dedupTTL).Result()
		if err != nil {
			return "", err
		}
		if !ok {
			return b.rdb.Get(ctx, dedupKey(j.DedupKey)).Result()
		}
	}

	if err := b.saveMetadata(ctx, j); err != nil {
		return "", err
	}
	if err := b.registerQueue(ctx, j.Queue); err != nil {
		return "", err
	}
	if err := b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: queueKey(j.Queue),
		Values: map[string]interface{}{
			"job_id": j.ID,
			"type":   j.Type,
		},
	}).Err(); err != nil {
		return "", err
	}
	return j.ID, nil
}

func (b *Broker) EnqueueDelayed(ctx context.Context, j *queue.Job, runAfter time.Time) error {
	if j.ID == "" {
		j.ID = queue.NewID()
	}
	if j.Queue == "" {
		j.Queue = defaultQueue
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now().UTC()
	}
	j.Status = queue.StatusScheduled
	j.RunAfter = runAfter

	if err := b.saveMetadata(ctx, j); err != nil {
		return err
	}
	return b.rdb.ZAdd(ctx, delayedKey, redis.Z{Score: float64(runAfter.Unix()), Member: j.ID}).Err()
}

func (b *Broker) GetJob(ctx context.Context, id string) (*queue.Job, error) {
	m, err := b.rdb.HGetAll(ctx, jobKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, redis.Nil
	}
	return unmarshalJob(m)
}

func (b *Broker) ScheduleRetry(ctx context.Context, j *queue.Job, failErr error) error {
	j.Attempt++
	j.Status = queue.StatusFailed
	if failErr != nil {
		j.Error = failErr.Error()
	}

	if err := b.saveMetadata(ctx, j); err != nil {
		return err
	}
	at := time.Now().UTC().Add(b.backoff(j.Attempt))
	return b.rdb.ZAdd(ctx, retryKey, redis.Z{Score: float64(at.Unix()), Member: j.ID}).Err()
}

func (b *Broker) EnqueueDLQ(ctx context.Context, j *queue.Job, failErr error) error {
	j.Status = queue.StatusDead
	now := time.Now().UTC()
	j.FinishedAt = &now
	if failErr != nil {
		j.Error = failErr.Error()
	}

	if err := b.saveMetadata(ctx, j); err != nil {
		return err
	}
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqKey,
		Values: map[string]interface{}{
			"job_id": j.ID,
			"type":   j.Type,
			"queue":  j.Queue,
			"error":  j.Error,
		},
	}).Err()
}

func (b *Broker) MarkTerminal(ctx context.Context, id string) error {
	ttl := jobHashTTL
	if st, err := b.rdb.HGet(ctx, jobKey(id), "status").Result(); err == nil && st == string(queue.StatusDead) {
		ttl = jobHashTTLDead
	}
	return b.rdb.Expire(ctx, jobKey(id), ttl).Err()
}

func (b *Broker) saveMetadata(ctx context.Context, j *queue.Job) error {
	return b.rdb.HSet(ctx, jobKey(j.ID), marshalJob(j)).Err()
}

func (b *Broker) backoff(attempt int) time.Duration {
	d := b.backoffBase << (attempt - 1)
	if d > b.backoffMax {
		d = b.backoffMax
	}
	return d
}

func marshalJob(j *queue.Job) map[string]interface{} {
	return map[string]interface{}{
		"id":          j.ID,
		"type":        j.Type,
		"queue":       j.Queue,
		"payload":     string(j.Payload),
		"status":      string(j.Status),
		"priority":    strconv.Itoa(j.Priority),
		"attempt":     strconv.Itoa(j.Attempt),
		"max_retries": strconv.Itoa(j.MaxRetries),
		"run_after":   formatTime(j.RunAfter),
		"lease_until": formatTime(j.LeaseUntil),
		"created_at":  formatTime(j.CreatedAt),
		"finished_at": formatTimePtr(j.FinishedAt),
		"error":       j.Error,
		"dedup_key":   j.DedupKey,
	}
}

func unmarshalJob(m map[string]string) (*queue.Job, error) {
	j := &queue.Job{
		ID:       m["id"],
		Type:     m["type"],
		Queue:    m["queue"],
		Payload:  []byte(m["payload"]),
		Status:   queue.Status(m["status"]),
		Error:    m["error"],
		DedupKey: m["dedup_key"],
	}

	var err error
	if j.Priority, err = atoiSafe(m["priority"]); err != nil {
		return nil, err
	}
	if j.Attempt, err = atoiSafe(m["attempt"]); err != nil {
		return nil, err
	}
	if j.MaxRetries, err = atoiSafe(m["max_retries"]); err != nil {
		return nil, err
	}
	if j.RunAfter, err = parseTime(m["run_after"]); err != nil {
		return nil, err
	}
	if j.LeaseUntil, err = parseTime(m["lease_until"]); err != nil {
		return nil, err
	}
	if j.CreatedAt, err = parseTime(m["created_at"]); err != nil {
		return nil, err
	}
	if f := parseTimePtr(m["finished_at"]); f != nil {
		j.FinishedAt = f
	}
	return j, nil
}

func atoiSafe(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.Atoi(s)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func formatTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func parseTimePtr(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	return &t
}
