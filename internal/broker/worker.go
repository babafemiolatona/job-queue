package broker

import (
	"context"
	"errors"
	"job-queue/internal/queue"
	"time"

	"github.com/redis/go-redis/v9"
)

func (b *Broker) ReadGroup(ctx context.Context, queue, consumer string, count int64, block time.Duration) ([]redis.XMessage, error) {
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName(queue),
		Consumer: consumer,
		Streams:  []string{queueKey(queue), ">"},
		Block:    block,
		Count:    count,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

func (b *Broker) Ack(ctx context.Context, queue string, ids ...string) error {
	return b.rdb.XAck(ctx, queueKey(queue), groupName(queue), ids...).Err()
}

func (b *Broker) MarkProcessed(ctx context.Context, id string, ttl time.Duration) (bool, error) {
	return b.rdb.SetNX(ctx, processedKey(id), "1", ttl).Result()
}

func (b *Broker) IsProcessed(ctx context.Context, id string) (bool, error) {
	n, err := b.rdb.Exists(ctx, processedKey(id)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (b *Broker) MarkRunning(ctx context.Context, j *queue.Job, until time.Time) error {
	j.Status = queue.StatusRunning
	j.LeaseUntil = until
	return b.saveMetadata(ctx, j)
}

func (b *Broker) MarkSucceeded(ctx context.Context, j *queue.Job) error {
	j.Status = queue.StatusSucceeded

	j.Error = ""
	now := time.Now().UTC()
	j.FinishedAt = &now

	if err := b.saveMetadata(ctx, j); err != nil {
		return err
	}
	return b.MarkTerminal(ctx, j.ID)
}

func processedKey(id string) string { return "processed:" + id }
