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
	"job-queue/internal/worker"
	"job-queue/internal/worker/handlers"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load("worker")

	b := broker.New(broker.Options{RedisOptions: &redis.Options{Addr: cfg.RedisAddr}})
	defer b.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := b.Ping(ctx); err != nil {
		logger.Error("redis not reachable", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}
	if err := b.EnsureGroup(ctx, cfg.Worker.Queue); err != nil {
		logger.Error("failed to ensure group", "err", err)
		os.Exit(1)
	}

	w := worker.New(worker.Options{
		Broker:               b,
		Logger:               logger,
		Queue:                cfg.Worker.Queue,
		Consumer:             cfg.Worker.Consumer,
		Concurrency:          cfg.Worker.Concurrency,
		BatchSize:            cfg.Worker.BatchSize,
		PollTimeout:          cfg.Worker.PollTimeout,
		LeaseDuration:        cfg.Worker.LeaseDuration,
		LeaseRefreshInterval: cfg.Worker.LeaseRefreshInterval,
		ReclaimInterval:      cfg.Worker.ReclaimInterval,
		MinIdle:              cfg.Worker.MinIdle,
		ShutdownTimeout:      cfg.Worker.ShutdownTimeout,
		ProcessedTTL:         cfg.Worker.ProcessedTTL,
		Registry:             worker.NewRegistry(map[string]worker.Handler{"demo_task": handlers.DemoTask()}),
	})

	logger.Info("worker starting",
		"queue", cfg.Worker.Queue,
		"consumer", cfg.Worker.Consumer,
		"concurrency", cfg.Worker.Concurrency,
	)

	if err := w.Run(ctx); err != nil {
		logger.Error("worker failed", "err", err)
		os.Exit(1)
	}
	logger.Info("worker stopped")
}
