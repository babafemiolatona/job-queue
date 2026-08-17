package main

import (
	"log/slog"
	"os"

	"job-queue/internal/config"
)

func main() {
	cfg := config.Load("api")
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("starting service", "service", cfg.ServiceName)
	slog.Info("service started", "service", cfg.ServiceName, "redis", cfg.RedisAddr)
}
