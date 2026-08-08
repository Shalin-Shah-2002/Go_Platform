package order

import (
	"context"
	"math"

	"github.com/google/uuid"
)

// Service contains order validation and business rules.
type Service struct {
	repository Repository
}

// NewService creates the order application service.
func NewService(repository Repository) *Service {
	return &Service{repository: repository}
}

// Create validates an order, calculates its total, assigns an ID, and delegates
// durable storage to the repository.
func (s *Service) Create(ctx context.Context, request CreateOrderRequest) (Response, error) {
	if request.CustomerID == "" {
		return Response{}, &ValidationError{Message: "customer_id is required"}
	}
	if len(request.Items) == 0 {
		return Response{}, &ValidationError{Message: "at least one item is required"}
	}

	var total int64
	for _, item := range request.Items {
		if item.ProductID == "" {
			return Response{}, &ValidationError{Message: "product_id is required"}
		}
		if item.Quantity <= 0 {
			return Response{}, &ValidationError{Message: "quantity must be greater than zero"}
		}
		if item.UnitPriceCents < 0 {
			return Response{}, &ValidationError{Message: "unit_price_cents cannot be negative"}
		}
		if int64(item.Quantity) > math.MaxInt64/item.UnitPriceCents && item.UnitPriceCents != 0 {
			return Response{}, &ValidationError{Message: "order total is too large"}
		}
		lineTotal := int64(item.Quantity) * item.UnitPriceCents
		if total > math.MaxInt64-lineTotal {
			return Response{}, &ValidationError{Message: "order total is too large"}
		}
		total += lineTotal
	}

	return s.repository.Create(ctx, OrderDraft{
		ID:         uuid.New(),
		CustomerID: request.CustomerID,
		Items:      request.Items,
		TotalCents: total,
	})
}
