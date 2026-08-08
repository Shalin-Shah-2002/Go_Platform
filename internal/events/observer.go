package events

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

// Store is a small in-memory store for the dashboard's latest raw event.
// Kafka remains the durable source; this store is only a local inspection aid.
type Store struct {
	mu   sync.RWMutex
	data []byte
}

// NewStore creates an empty event store.
func NewStore() *Store {
	return &Store{}
}

// Set replaces the stored event with a private copy of data.
func (s *Store) Set(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data[:0], data...)
}

// Get returns a private copy so callers can use it after the read lock is
// released without racing with the next Kafka message.
func (s *Store) Get() ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.data) == 0 {
		return nil, false
	}
	return append([]byte(nil), s.data...), true
}

// Observer reads raw order CDC events for the dashboard.
type Observer struct {
	brokers []string
	topic   string
	groupID string
	store   *Store
}

// NewOrderObserver creates a reader for the Debezium orders topic.
func NewOrderObserver(brokers []string, store *Store) *Observer {
	return &Observer{
		brokers: brokers,
		topic:   "platform.public.orders",
		groupID: "dashboard-observer",
		store:   store,
	}
}

// Run blocks until the context is cancelled or Kafka returns an error.
func (o *Observer) Run(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     o.brokers,
		Topic:       o.topic,
		GroupID:     o.groupID,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		o.store.Set(message.Value)
	}
}
