package order

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type fakeRepository struct {
	draft OrderDraft
}

func (f *fakeRepository) Create(_ context.Context, draft OrderDraft) (Response, error) {
	f.draft = draft
	return Response{ID: draft.ID, CustomerID: draft.CustomerID, TotalCents: draft.TotalCents, Items: draft.Items}, nil
}

func TestServiceCreateCalculatesTotalAndAssignsID(t *testing.T) {
	repository := &fakeRepository{}
	service := NewService(repository)

	response, err := service.Create(context.Background(), CreateOrderRequest{
		CustomerID: "cust-1",
		Items: []OrderItem{
			{ProductID: "prod-1", Quantity: 2, UnitPriceCents: 1500},
			{ProductID: "prod-2", Quantity: 1, UnitPriceCents: 500},
		},
	})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if response.TotalCents != 3500 {
		t.Fatalf("total = %d, want 3500", response.TotalCents)
	}
	if response.ID == uuid.Nil {
		t.Fatal("expected a generated order ID")
	}
	if repository.draft.ID != response.ID {
		t.Fatal("repository received a different order ID")
	}
}

func TestServiceCreateRejectsInvalidRequest(t *testing.T) {
	service := NewService(&fakeRepository{})
	cases := []CreateOrderRequest{
		{Items: []OrderItem{{ProductID: "prod-1", Quantity: 1}}},
		{CustomerID: "cust-1"},
		{CustomerID: "cust-1", Items: []OrderItem{{ProductID: "prod-1", Quantity: 0}}},
		{CustomerID: "cust-1", Items: []OrderItem{{ProductID: "prod-1", Quantity: 1, UnitPriceCents: -1}}},
	}

	for _, request := range cases {
		if _, err := service.Create(context.Background(), request); err == nil {
			t.Fatalf("expected validation error for %#v", request)
		}
	}
}
