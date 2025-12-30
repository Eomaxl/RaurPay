package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/Eomaxl/RaurPay/internal/domain"
	"github.com/gocql/gocql"
)

// PaymentRepository implements payment operations with Cassandra
type PaymentRepository struct {
	session *gocql.Session
}

// NewPaymentRepository creates a new payment repository
func NewPaymentRepository(session *gocql.Session) *PaymentRepository {
	return &PaymentRepository{
		session: session,
	}
}

// CreatePayment creates a new payment record
func (pr *PaymentRepository) CreatePayment(ctx context.Context, payment *domain.Payment) error {
	bucketDate := GetTimeBucket(payment.CreatedAt)

	query := `
		INSERT INTO payments (
			payment_id, bucket_date, created_at, idempotency_key, status,
			amount, currency, source_amount, target_account, description,
			updated_at, expires_at, metadata
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`

	paymentUUID, err := gocql.ParseUUID(payment.PaymentID)
	if err != nil {
		return fmt.Errorf("invalid payment ID : %w", err)
	}

	err = pr.session.Query(query,
		paymentUUID,
		bucketDate,
		payment.CreatedAt,
		payment.IdempotencyKey,
		string(payment.Status),
		payment.Amount,
		payment.Currency,
		payment.SourceAmount,
		payment.TargetAmount,
		payment.Description,
		payment.UpdatedAt,
		payment.ExpiresAt,
		payment.Metadata,
	).WithContext(ctx).Exec()

	if err != nil {
		fmt.Errorf("failed to create payment: %w",err)
	}

	return nil
}

// GetPayment retrieves the payment by ID
func (pr *PaymentRepository) GetPayment(ctx context.Context, paymentId string) (*domain.Payment, error) {
	// Since we need to query across the bucket, search recent buckets
	now := time.Now()

	// Search last 30 days of buckets
	for i:= 0; i<30; i++ {
		bucketDate := GetTimeBucket(now.AddDate(0,0,-i))

		query := `
			SELECT payment_id, created_at_ idempotency_key, status, amount, currency, source_amount, target_amount, description, updated_at, expires_at, metadata
			FROM payments
			WHERE payment_id = ? AND bucket_date = ?`
		
		var payment domain.Payment
		var paymentIDUUID gocql.UUID
		var statusStr string

		paymentUUID, err := gocql.ParseUUID(paymentId)
		if err != nil {
			return nil, fmt.Errorf("invalid payment ID : %w", err)
		}

		err = pr.session.Query(query, paymentUUID, bucketDate).WithContext(ctx).Scan(&paymentIDUUID, &payment.CreatedAt, &payment.IdempotencyKey,
								&statusStr, &payment.Amount, &payment.Currency, &payment.SourceAmount, &payment.TargetAmount, &payment.Description, &payment.UpdatedAt, &payment.ExpiresAt
							&payment.Metadata)
		
		if err == nil {
			payment.PaymentID = paymentIDUUID.String()
			payment.Status = domain.PaymentStatus(statusStr)
			return &payment, nil
		}

		if err != gocql.ErrNotFound {
			return nil, fmt.Errorf("failed to get payment: %w", err)
		}
	}

	return nil, domain.ErrPaymentNotFound
}
