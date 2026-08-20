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

	"job-queue/internal/api"
	"job-queue/internal/broker"
	"job-queue/internal/config"
	"job-queue/internal/metrics"

	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load("api")

	b := broker.New(broker.Options{RedisOptions: &redis.Options{Addr: cfg.RedisAddr}})
	defer b.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := b.Ping(ctx); err != nil {
		logger.Error("redis not reachable", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}
	go b.CleanupLoop(ctx, time.Minute)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: api.NewServer(b, logger, metrics.New()).Handler(),
	}

	go func() {
		logger.Info("api listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down api")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	logger.Info("api stopped")
}
