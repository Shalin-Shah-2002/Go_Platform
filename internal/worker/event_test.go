package worker

import "testing"

func TestEventIDUsesPostgresLSN(t *testing.T) {
	var event Event
	event.Payload.After = &struct {
		ID         string `json:"id"`
		CustomerID string `json:"customer_id"`
		TotalCents int64  `json:"total_cents"`
	}{ID: "7a7c19da-3178-4d65-813f-d24cbbcb404b"}
	event.Payload.Source.Table = "orders"
	event.Payload.Source.LSN = 27019416

	id, err := event.ID()
	if err != nil {
		t.Fatalf("event ID: %v", err)
	}
	if id != "orders:27019416" {
		t.Fatalf("event ID = %q, want orders:27019416", id)
	}
}

func TestEventIDRejectsInvalidOrderID(t *testing.T) {
	var event Event
	event.Payload.After = &struct {
		ID         string `json:"id"`
		CustomerID string `json:"customer_id"`
		TotalCents int64  `json:"total_cents"`
	}{ID: "not-a-uuid"}

	if _, err := event.ID(); err == nil {
		t.Fatal("expected invalid UUID error")
	}
}
