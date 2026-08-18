package broker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func (b *Broker) SetLease(ctx context.Context, id string, until time.Time) error {
	return b.rdb.HSet(ctx, jobKey(id), "lease_until", until.Format(time.RFC3339Nano)).Err()
}

func (b *Broker) ReclaimStale(ctx context.Context, queue, consumer string, minIdle time.Duration, count int64) ([]redis.XMessage, error) {
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   queueKey(queue),
		Group:    groupName(queue),
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    count,
	}).Result()
	return msgs, err
}
