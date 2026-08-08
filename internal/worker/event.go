package worker

import (
	"fmt"

	"github.com/google/uuid"
)

// Event is the small part of a Debezium order envelope consumed by workers.
type Event struct {
	Payload struct {
		After *struct {
			ID         string `json:"id"`
			CustomerID string `json:"customer_id"`
			TotalCents int64  `json:"total_cents"`
		} `json:"after"`
		Source struct {
			Table string `json:"table"`
			LSN   int64  `json:"lsn"`
		} `json:"source"`
		Op string `json:"op"`
	} `json:"payload"`
}

// ID returns a stable identifier for one CDC change. The PostgreSQL LSN is
// preferred because the same order can legitimately be updated more than once.
func (e Event) ID() (string, error) {
	if e.Payload.After == nil {
		return "", nil
	}
	if _, err := uuid.Parse(e.Payload.After.ID); err != nil {
		return "", fmt.Errorf("invalid order id %q: %w", e.Payload.After.ID, err)
	}
	if e.Payload.Source.LSN != 0 {
		return fmt.Sprintf("%s:%d", e.Payload.Source.Table, e.Payload.Source.LSN), nil
	}
	return fmt.Sprintf("%s:%s:%s", e.Payload.Source.Table, e.Payload.After.ID, e.Payload.Op), nil
}
