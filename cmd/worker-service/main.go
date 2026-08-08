// Command worker-service is the composition root for Kafka projection workers.
// The same executable is deployed with WORKER_ROLE set to inventory,
// notification, or analytics.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/realtime-data-platform/internal/config"
	"github.com/example/realtime-data-platform/internal/platform"
	"github.com/example/realtime-data-platform/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	settings := config.LoadWorker()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := platform.NewPostgresPool(ctx, settings.DatabaseURL)
	if err != nil {
		logger.Error("create database pool", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := platform.WaitForPostgres(ctx, db, 30, time.Second); err != nil {
		logger.Error("wait for database", "error", err)
		os.Exit(1)
	}

	var cache *redis.Client
	if settings.Role == worker.RoleInventory {
		cache = platform.NewRedisClient(settings.RedisAddress)
		defer cache.Close()
	}

	handler, err := worker.NewHandler(db, cache, settings.Role, logger)
	if err != nil {
		logger.Error("create worker handler", "error", err)
		os.Exit(1)
	}
	consumer := worker.NewConsumer(settings.KafkaBrokers, settings.Role+"-service", handler, settings.WorkerCount, logger)
	logger.Info("worker service started", "role", settings.Role, "workers", settings.WorkerCount)
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
