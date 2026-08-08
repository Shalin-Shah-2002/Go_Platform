package order

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository persists orders and their items in one transaction.
type PostgresRepository struct {
	db *pgxpool.Pool
}

// NewPostgresRepository creates the PostgreSQL adapter.
func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

// Create writes the order header and all item rows atomically.
func (r *PostgresRepository) Create(ctx context.Context, draft OrderDraft) (Response, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Response{}, err
	}
	defer tx.Rollback(ctx)

	var createdAt Response
	createdAt.ID = draft.ID
	createdAt.CustomerID = draft.CustomerID
	createdAt.Status = "PENDING"
	createdAt.TotalCents = draft.TotalCents
	createdAt.Items = draft.Items
	if err := tx.QueryRow(ctx,
		"INSERT INTO orders (id, customer_id, total_cents) VALUES ($1, $2, $3) RETURNING created_at",
		draft.ID, draft.CustomerID, draft.TotalCents,
	).Scan(&createdAt.CreatedAt); err != nil {
		return Response{}, err
	}

	for _, item := range draft.Items {
		if _, err := tx.Exec(ctx,
			"INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents) VALUES ($1, $2, $3, $4)",
			draft.ID, item.ProductID, item.Quantity, item.UnitPriceCents,
		); err != nil {
			return Response{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Response{}, err
	}
	return createdAt, nil
}
