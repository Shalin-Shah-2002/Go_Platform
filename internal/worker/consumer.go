package worker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/segmentio/kafka-go"
)

// Consumer reads Kafka messages and distributes them to a bounded worker pool.
type Consumer struct {
	reader      *kafka.Reader
	handler     *Handler
	workerCount int
	logger      *slog.Logger
}

// NewConsumer creates a consumer for the shared Debezium orders topic.
func NewConsumer(brokers []string, groupID string, handler *Handler, workerCount int, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       "platform.public.orders",
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
		}),
		handler:     handler,
		workerCount: workerCount,
		logger:      logger,
	}
}

// Run fetches messages, processes them concurrently, and commits offsets only
// after a worker has completed successfully.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()
	if c.workerCount < 1 {
		c.workerCount = 1
	}

	messages := make(chan kafka.Message, c.workerCount*8)
	var workers sync.WaitGroup
	for i := 0; i < c.workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for message := range messages {
				if err := c.handler.Handle(ctx, message.Value); err != nil {
					c.logger.Error("process Kafka event", "error", err)
					continue
				}
				if err := c.reader.CommitMessages(ctx, message); err != nil {
					c.logger.Error("commit Kafka offset", "error", err)
				}
			}
		}()
	}

	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			break
		}
		select {
		case messages <- message:
		case <-ctx.Done():
			break
		}
	}
	close(messages)
	workers.Wait()
	return ctx.Err()
}
