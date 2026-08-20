package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"job-queue/internal/broker"
	"job-queue/internal/config"
	"job-queue/internal/metrics"
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

	m := metrics.New()

	metricsSrv := &http.Server{Addr: cfg.Worker.MetricsAddr, Handler: promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})}

	go func() {
		logger.Info("metrics server listening", "addr", cfg.Worker.MetricsAddr)

		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "err", err)
		}
	}()

	w := worker.New(worker.Options{
		Broker:               b,
		Logger:               logger,
		Metrics:              m,
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

	// Gracefully stop the metrics server before exiting.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	logger.Info("worker stopped")
}
