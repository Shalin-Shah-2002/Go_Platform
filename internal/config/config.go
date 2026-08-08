package config

import (
	"os"
	"strings"
)

// OrderConfig contains configuration needed by the HTTP order service.
type OrderConfig struct {
	DatabaseURL  string
	HTTPPort     string
	KafkaBrokers []string
}

// WorkerConfig contains configuration needed by a Kafka projection worker.
type WorkerConfig struct {
	Role         string
	DatabaseURL  string
	KafkaBrokers []string
	RedisAddress string
	WorkerCount  int
}

// LoadOrder reads order-service configuration from the environment.
func LoadOrder() OrderConfig {
	return OrderConfig{
		DatabaseURL:  getenv("DATABASE_URL", "postgres://platform:platform@localhost:5432/platform?sslmode=disable"),
		HTTPPort:     getenv("ORDER_SERVICE_PORT", "8080"),
		KafkaBrokers: splitList(getenv("KAFKA_BROKERS", "localhost:9094")),
	}
}

// LoadWorker reads worker-service configuration from the environment.
func LoadWorker() WorkerConfig {
	return WorkerConfig{
		Role:         getenv("WORKER_ROLE", "analytics"),
		DatabaseURL:  getenv("DATABASE_URL", "postgres://platform:platform@localhost:5432/platform?sslmode=disable"),
		KafkaBrokers: splitList(getenv("KAFKA_BROKERS", "localhost:9094")),
		RedisAddress: getenv("REDIS_ADDR", "localhost:6379"),
		WorkerCount:  4,
	}
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
