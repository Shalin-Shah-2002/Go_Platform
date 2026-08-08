package platform

import "github.com/redis/go-redis/v9"

// NewRedisClient creates a Redis client. The caller owns its lifecycle.
func NewRedisClient(address string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: address})
}
