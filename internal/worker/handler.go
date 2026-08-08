package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	RoleInventory    = "inventory"
	RoleNotification = "notification"
	RoleAnalytics    = "analytics"
)

// Handler applies the role-specific projection after a Kafka message is read.
type Handler struct {
	db     *pgxpool.Pool
	redis  *redis.Client
	role   string
	logger *slog.Logger
}

// NewHandler creates a projection handler. Redis is only required by the
// inventory role and may be nil for notification and analytics.
func NewHandler(db *pgxpool.Pool, cache *redis.Client, role string, logger *slog.Logger) (*Handler, error) {
	switch role {
	case RoleInventory, RoleNotification, RoleAnalytics:
	default:
		return nil, fmt.Errorf("unsupported worker role %q", role)
	}
	if role == RoleInventory && cache == nil {
		return nil, fmt.Errorf("redis client is required for inventory role")
	}
	return &Handler{db: db, redis: cache, role: role, logger: logger}, nil
}

// Handle parses one raw Debezium message and applies its projection. Returning
// an error tells the consumer not to commit the Kafka offset.
func (h *Handler) Handle(ctx context.Context, raw []byte) error {
	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("decode Debezium event: %w", err)
	}
	if event.Payload.After == nil || event.Payload.Op == "d" {
		return nil
	}

	eventID, err := event.ID()
	if err != nil {
		return err
	}

	// Claiming the event and writing the SQL projection use the same
	// transaction. If the projection fails, the claim is rolled back and Kafka
	// can safely redeliver the event.
	tx, err := h.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `INSERT INTO processed_events (consumer_name, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, h.role, eventID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		// Re-applying the Redis value is harmless and repairs the cache if a
		// previous delivery committed PostgreSQL but failed before caching.
		if h.role == RoleInventory {
			return h.redis.Set(ctx, "inventory:order:"+event.Payload.After.ID, "RESERVED", 24*time.Hour).Err()
		}
		return nil
	}

	switch h.role {
	case RoleAnalytics:
		_, err = tx.Exec(ctx, `INSERT INTO analytics_order_events (order_id, customer_id, total_cents) VALUES ($1::uuid,$2,$3) ON CONFLICT DO NOTHING`, event.Payload.After.ID, event.Payload.After.CustomerID, event.Payload.After.TotalCents)
	case RoleNotification:
		_, err = tx.Exec(ctx, `INSERT INTO notifications (order_id, status) VALUES ($1::uuid, 'SENT')`, event.Payload.After.ID)
	case RoleInventory:
		_, err = tx.Exec(ctx, `INSERT INTO inventory_reservations (order_id, status) VALUES ($1::uuid, 'RESERVED') ON CONFLICT DO NOTHING`, event.Payload.After.ID)
	}
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if h.role == RoleInventory {
		err = h.redis.Set(ctx, "inventory:order:"+event.Payload.After.ID, "RESERVED", 24*time.Hour).Err()
	}
	return err
}
