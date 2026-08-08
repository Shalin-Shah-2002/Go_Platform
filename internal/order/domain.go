package order

import (
	"time"

	"github.com/google/uuid"
)

// CreateOrderRequest is the public input model for creating an order.
type CreateOrderRequest struct {
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
}

// OrderItem is one product line in an order. Money is represented in cents.
type OrderItem struct {
	ProductID      string `json:"product_id"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// OrderDraft is the validated data sent from the application service to the
// persistence adapter.
type OrderDraft struct {
	ID         uuid.UUID
	CustomerID string
	Items      []OrderItem
	TotalCents int64
}

// Response is returned by the order application service.
type Response struct {
	ID         uuid.UUID   `json:"id"`
	CustomerID string      `json:"customer_id"`
	Status     string      `json:"status"`
	TotalCents int64       `json:"total_cents"`
	Items      []OrderItem `json:"items"`
	CreatedAt  time.Time   `json:"created_at"`
}

// ValidationError identifies client input that cannot be accepted.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }
