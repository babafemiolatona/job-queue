package broker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultQueue = "default"

	jobHashTTL     = 7 * 24 * time.Hour
	jobHashTTLDead = 30 * 24 * time.Hour
	dedupTTL       = 24 * time.Hour

	backoffBase = time.Second
	backoffMax  = 5 * time.Minute

	delayedKey = "delayed"
	retryKey   = "retry"
	dlqKey     = "dlq"
)

type Options struct {
	RedisOptions *redis.Options
	BackoffBase  time.Duration
	BackoffMax   time.Duration
}

type Broker struct {
	rdb         *redis.Client
	backoffBase time.Duration
	backoffMax  time.Duration
}

func New(opts Options) *Broker {
	if opts.BackoffBase == 0 {
		opts.BackoffBase = backoffBase
	}
	if opts.BackoffMax == 0 {
		opts.BackoffMax = backoffMax
	}
	return &Broker{
		rdb:         redis.NewClient(opts.RedisOptions),
		backoffBase: opts.BackoffBase,
		backoffMax:  opts.BackoffMax,
	}
}

func (b *Broker) Ping(ctx context.Context) error {
	return b.rdb.Ping(ctx).Err()
}

func (b *Broker) Close() error {
	return b.rdb.Close()
}

func queueKey(name string) string   { return "queue:" + name }
func groupName(queue string) string { return "consumers:" + queue }
func jobKey(id string) string       { return "job:" + id }
func dedupKey(k string) string      { return "dedup:" + k }
func queuesSetKey() string          { return "queues" }
