package broker

import "context"

type QueueStats struct {
	Queue    string `json:"queue"`
	Ready    int64  `json:"ready"`
	InFlight int64  `json:"in_flight"`
}

type Stats struct {
	Queues  []QueueStats `json:"queues"`
	Delayed int64        `json:"delayed"`
	Retry   int64        `json:"retry"`
	DLQ     int64        `json:"dlq"`
}

func (b *Broker) Stats(ctx context.Context) (*Stats, error) {
	s := &Stats{}

	queues, err := b.rdb.SMembers(ctx, queuesSetKey()).Result()
	if err != nil {
		return nil, err
	}
	for _, q := range queues {
		length, err := b.rdb.XLen(ctx, queueKey(q)).Result()
		if err != nil {
			return nil, err
		}
		groups, err := b.rdb.XInfoGroups(ctx, queueKey(q)).Result()
		if err != nil {
			return nil, err
		}
		var inFlight int64
		for _, g := range groups {
			inFlight += g.Pending
		}
		s.Queues = append(s.Queues, QueueStats{
			Queue:    q,
			Ready:    length,
			InFlight: inFlight,
		})
	}

	s.Delayed, _ = b.rdb.ZCard(ctx, delayedKey).Result()
	s.Retry, _ = b.rdb.ZCard(ctx, retryKey).Result()
	s.DLQ, _ = b.rdb.XLen(ctx, dlqKey).Result()
	return s, nil
}
