package config

import (
	"os"
	"strconv"
	"time"
)

type WorkerConfig struct {
	Queue                string
	Consumer             string
	Concurrency          int
	BatchSize            int
	PollTimeout          time.Duration
	LeaseDuration        time.Duration
	LeaseRefreshInterval time.Duration
	ReclaimInterval      time.Duration
	MinIdle              time.Duration
	ShutdownTimeout      time.Duration
	ProcessedTTL         time.Duration
}

type Config struct {
	ServiceName string
	RedisAddr   string
	HTTPAddr    string
	LogLevel    string
	Worker      WorkerConfig
}

func Load(serviceName string) Config {
	return Config{
		ServiceName: serviceName,
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		Worker: WorkerConfig{
			Queue:                getEnv("WORKER_QUEUE", "default"),
			Consumer:             getEnv("WORKER_CONSUMER", "worker"),
			Concurrency:          getEnvInt("WORKER_CONCURRENCY", 10),
			BatchSize:            getEnvInt("WORKER_BATCH_SIZE", 10),
			PollTimeout:          getEnvDuration("WORKER_POLL_TIMEOUT", 2*time.Second),
			LeaseDuration:        getEnvDuration("WORKER_LEASE_DURATION", 60*time.Second),
			LeaseRefreshInterval: getEnvDuration("WORKER_LEASE_REFRESH", 15*time.Second),
			ReclaimInterval:      getEnvDuration("WORKER_RECLAIM_INTERVAL", 30*time.Second),
			MinIdle:              getEnvDuration("WORKER_MIN_IDLE", 60*time.Second),
			ShutdownTimeout:      getEnvDuration("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
			ProcessedTTL:         getEnvDuration("WORKER_PROCESSED_TTL", 24*time.Hour),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
