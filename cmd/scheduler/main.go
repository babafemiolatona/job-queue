package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/broker"
	"job-queue/internal/config"
	"job-queue/internal/scheduler"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load("scheduler")

	b := broker.New(broker.Options{RedisOptions: &redis.Options{Addr: cfg.RedisAddr}})
	defer b.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := b.Ping(ctx); err != nil {
		logger.Error("redis not reachable", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	s := scheduler.New(scheduler.Options{
		Broker:        b,
		Logger:        logger,
		LockKey:       cfg.Scheduler.LockKey,
		LockTTL:       cfg.Scheduler.LockTTL,
		RenewInterval: cfg.Scheduler.RenewInterval,
		TickInterval:  cfg.Scheduler.TickInterval,
		BatchSize:     cfg.Scheduler.BatchSize,
		Instance:      hostname + ":" + cfg.ServiceName,
	})

	logger.Info("scheduler starting",
		"instance", hostname+":"+cfg.ServiceName,
		"lock_key", "lock:"+cfg.Scheduler.LockKey,
		"tick_interval", cfg.Scheduler.TickInterval,
	)

	if err := s.Run(ctx); err != nil {
		logger.Error("scheduler failed", "err", err)
		os.Exit(1)
	}
	logger.Info("scheduler stopped")
}
