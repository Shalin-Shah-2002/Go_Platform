// Command order-service is the composition root for the HTTP order service.
// It wires infrastructure adapters to the order application layer and starts
// the HTTP server plus the dashboard's Kafka observer.
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

	"github.com/example/realtime-data-platform/internal/config"
	"github.com/example/realtime-data-platform/internal/events"
	"github.com/example/realtime-data-platform/internal/order"
	"github.com/example/realtime-data-platform/internal/platform"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	settings := config.LoadOrder()
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

	// Composition happens here; the application layer does not know which
	// database or HTTP implementation is being used.
	repository := order.NewPostgresRepository(db)
	service := order.NewService(repository)
	eventStore := events.NewStore()
	observer := events.NewOrderObserver(settings.KafkaBrokers, eventStore)
	go func() {
		if err := observer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("order event observer stopped", "error", err)
		}
	}()

	handler := order.NewHandler(service, db, eventStore, logger)
	server := &http.Server{
		Addr:              ":" + settings.HTTPPort,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("order service started", "port", settings.HTTPPort)
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown http server", "error", err)
		}
	}
}
