package platform

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPostgresPool creates the shared PostgreSQL connection pool for a process.
func NewPostgresPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// WaitForPostgres waits for a dependency that may still be starting in Docker
// or Kubernetes. It returns the last database error after all attempts fail.
func WaitForPostgres(ctx context.Context, db *pgxpool.Pool, attempts int, interval time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := db.Ping(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("postgresql did not become ready after %d attempts: %w", attempts, lastErr)
}
