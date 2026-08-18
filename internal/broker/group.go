package broker

import (
	"context"
	"strings"
)

func (b *Broker) EnsureGroup(ctx context.Context, queue string) error {
	if queue == "" {
		queue = defaultQueue
	}
	err := b.rdb.XGroupCreateMkStream(ctx, queueKey(queue), groupName(queue), "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return b.rdb.SAdd(ctx, queuesSetKey(), queue).Err()
}

func (b *Broker) registerQueue(ctx context.Context, queue string) error {
	return b.rdb.SAdd(ctx, queuesSetKey(), queue).Err()
}
