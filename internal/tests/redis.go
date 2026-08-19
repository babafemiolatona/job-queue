package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	redisAddr   string
)

func RunRedisMain(m *testing.M) int {
	pool, err := dockertest.NewPool("")
	if err != nil {
		panic("connect to docker: " + err.Error())
	}
	rsrc, err := pool.Run("redis", "7-alpine", nil)
	if err != nil {
		panic("start redis container: " + err.Error())
	}

	redisAddr = rsrc.GetHostPort("6379/tcp")
	redisClient = redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return redisClient.Ping(ctx).Err()
	}); err != nil {
		panic("redis not ready: " + err.Error())
	}

	code := m.Run()
	_ = pool.Purge(rsrc)
	return code
}

func Redis() *redis.Client {
	if redisClient == nil {
		panic("tests.Redis() requires tests.RunRedisMain in TestMain")
	}
	return redisClient
}

func RedisAddr() string {
	if redisAddr == "" {
		panic("tests.RedisAddr() requires tests.RunRedisMain in TestMain")
	}
	return redisAddr
}
