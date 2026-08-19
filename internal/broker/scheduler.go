package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/queue"
)

const lockKeyPrefix = "lock:"

const (
	ZSetDelayed = "delayed"
	ZSetRetry   = "retry"
)

var (
	ErrNotInDLQ = errors.New("job not in dlq")

	renewLockScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		end
		return 0`)

	releaseLockScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0`)
)

func (b *Broker) AcquireLock(ctx context.Context, name, token string, ttl time.Duration) (bool, error) {
	ok, err := b.rdb.SetNX(ctx, lockKeyPrefix+name, token, ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (b *Broker) RenewLock(ctx context.Context, name, token string, ttl time.Duration) (bool, error) {
	n, err := renewLockScript.Run(ctx, b.rdb,
		[]string{lockKeyPrefix + name}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (b *Broker) ReleaseLock(ctx context.Context, name, token string) error {
	_, err := releaseLockScript.Run(ctx, b.rdb, []string{lockKeyPrefix + name}, token).Result()
	return err
}

func (b *Broker) PromoteDue(ctx context.Context, zset string, now time.Time, batch int64) (int64, error) {
	ids, err := b.rdb.ZRangeByScore(ctx, zset, &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(now.Unix(), 10),
	}).Result()
	if err != nil {
		return 0, err
	}

	var promoted int64
	for _, id := range ids {
		if promoted >= batch {
			break
		}
		job, err := b.GetJob(ctx, id)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				_ = b.rdb.ZRem(ctx, zset, id).Err()
				continue
			}
			return promoted, err
		}
		if err := b.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: queueKey(job.Queue),
			Values: map[string]interface{}{
				"job_id": job.ID,
				"type":   job.Type,
			},
		}).Err(); err != nil {
			return promoted, err
		}
		if err := b.rdb.HSet(ctx, jobKey(job.ID), "status", string(queue.StatusPending)).Err(); err != nil {
			return promoted, err
		}
		if err := b.rdb.ZRem(ctx, zset, job.ID).Err(); err != nil {
			return promoted, err
		}
		promoted++
	}
	return promoted, nil
}

func (b *Broker) RequeueDLQ(ctx context.Context, id string) error {
	job, err := b.GetJob(ctx, id)
	if err != nil {
		return err
	}

	msgs, err := b.rdb.XRange(ctx, dlqKey, "-", "+").Result()
	if err != nil {
		return err
	}
	var dlqID string
	for _, m := range msgs {
		if m.Values["job_id"] == id {
			dlqID = m.ID
			break
		}
	}
	if dlqID == "" {
		return ErrNotInDLQ
	}
	if err := b.rdb.XDel(ctx, dlqKey, dlqID).Err(); err != nil {
		return err
	}

	job.Status = queue.StatusPending
	job.Attempt = 0
	job.Error = ""
	job.FinishedAt = nil
	job.LeaseUntil = time.Time{}
	if err := b.saveMetadata(ctx, job); err != nil {
		return err
	}
	if err := b.registerQueue(ctx, job.Queue); err != nil {
		return err
	}
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: queueKey(job.Queue),
		Values: map[string]interface{}{
			"job_id": job.ID,
			"type":   job.Type,
		},
	}).Err()
}

func LockToken(instance string) string {
	return fmt.Sprintf("%s:%d", instance, time.Now().UnixNano())
}
