package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/Eomaxl/RaurPay/internal/config"
	"github.com/go-redis/redis/v8"
)

// ConnectionManager manages Redis connections
type ConnectionManager struct {
	client *redis.Client
	config config.RedisConfig
}

// NewConnectionManager creates a new redis connection manager
func NewConnectionManager(cfg config.RedisConfig) *ConnectionManager {
	return &ConnectionManager{
		config: cfg,
	}
}

// Connect establishes connection to Redis
func (rm *ConnectionManager) Connect() error {
	rm.client = redis.NewClient(&redis.Options{
		Addr:         rm.config.Addr,
		Password:     rm.config.Password,
		DB:           rm.config.DB,
		PoolSize:     rm.config.PoolSize,
		MinIdleConns: rm.config.MinIdleConns,
		DialTimeout:  rm.config.DialTimeout,
		ReadTimeout:  rm.config.ReadTimeout,
		WriteTimeout: rm.config.WriteTimeout,
		IdleTimeout:  rm.config.IdleTimeout,
	})

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rm.client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("Failed to connect to Redis : %w", err)
	}

	return nil
}

// Client returns to Redis client
func (rm *ConnectionManager) Client() *redis.Client {
	return rm.client
}

// Close closes the Redis connection
func (rm *ConnectionManager) Close() error {
	if rm.client != nil {
		return rm.client.Close()
	}
	return nil
}

// Healthcheck performs a health check on the redis connection
func (rm *ConnectionManager) Healthcheck(ctx context.Context) error {
	if rm.client == nil {
		return fmt.Errorf("Redis client is nil")
	}

	_, err := rm.client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("Redis health check failed : %w", err)
	}

	return nil
}

// Set sets a key-value pair with expiration
func (rm *ConnectionManager) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return rm.client.Set(ctx, key, value, expiration).Err()
}

// Get gets a value by key
func (rm *ConnectionManager) Get(ctx context.Context, key string) (string, error) {
	return rm.client.Get(ctx, key).Result()
}

// Delete deletes a key
func (rm *ConnectionManager) Delete(ctx context.Context, key string) error {
	return rm.client.Del(ctx, key).Err()
}

// Exists checks if a key exists
func (rm *ConnectionManager) Exists(ctx context.Context, key string) (bool, error) {
	result, err := rm.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return result > 0, err
}
