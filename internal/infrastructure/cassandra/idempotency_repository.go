package cassandra

import (
	"context"
	"time"
)

// IdempotencyRecord represents a stored idempotency key with metadata
type IdempotencyRecord struct {
	Key         string                 `json:"key"`
	PaymentID   string                 `json:"payment_id"`
	RequestHash string                 `json:"request_hash"`
	Response    map[string]interface{} `json:"response"`
	Status      string                 `json:"status"`
	CreatedAt   time.Time              `json:"created_at"`
	ExpiresAt   time.Time              `json:"expires_at"`
	Metadata    map[string]string      `json:"metadata"`
}

// IdempotencyRepository defines the interface for idempotency storage
type IdempotencyRepository interface {
	Store(ctx context.Context, record *IdempotencyRecord) error
	Get(ctx context.Context, key string) (*IdempotencyRecord, error)
	Delete(ctx context.Context, key string) error
	DeleteExpired(ctx context.Context, before time.Time) error
	GetExpiredKeys(ctx context.Context, before time.Time, limit int) ([]string, error)
}
