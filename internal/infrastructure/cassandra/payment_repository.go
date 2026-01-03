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
		payment.SourceAccount,
		payment.TargetAccount,
		payment.Description,
		payment.UpdatedAt,
		payment.ExpiresAt,
		payment.Metadata,
	).WithContext(ctx).Exec()

	if err != nil {
		return fmt.Errorf("failed to create payment: %w", err)
	}

	return nil
}

// GetPayment retrieves the payment by ID
func (pr *PaymentRepository) GetPayment(ctx context.Context, paymentId string) (*domain.Payment, error) {
	// Since we need to query across the bucket, search recent buckets
	now := time.Now()

	// Search last 30 days of buckets
	for i := 0; i < 30; i++ {
		bucketDate := GetTimeBucket(now.AddDate(0, 0, -i))

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
			&statusStr, &payment.Amount, &payment.Currency, &payment.SourceAccount, &payment.TargetAccount, &payment.Description, &payment.UpdatedAt, &payment.ExpiresAt,
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

// UpdatePaymentStatus updates the status of a payment
func (pr *PaymentRepository) UpdatePaymentStatus(ctx context.Context, paymentID string, status domain.PaymentStatus, updatedAt time.Time) error {
	// First, find the payment to get its bucket
	payment, err := pr.GetPayment(ctx, paymentID)
	if err != nil {
		return err
	}

	bucketDate := GetTimeBucket(payment.CreatedAt)

	query := `
		UPDATE payments
		SET status = ?, updated_at = ?
		WHERE payment_id = ? AND bucket_date = ? AND created_at = ?`

	paymentUUID, err := gocql.ParseUUID(paymentID)
	if err != nil {
		return fmt.Errorf("invalid payment ID: %w", err)
	}

	err = pr.session.Query(
		query,
		string(status),
		updatedAt,
		paymentUUID,
		bucketDate,
		payment.CreatedAt,
	).WithContext(ctx).Exec()

	if err != nil {
		return fmt.Errorf("failed to update payment status: %w", err)
	}

	return nil
}

// GetPaymentsByAccount retrieves payments for a specific account within a time range
func (pr *PaymentRepository) GetPaymentsByAccount(ctx context.Context, accountID string, startTime, endTime time.Time, limit int) ([]domain.Payment, error) {
	var payments []domain.Payment

	// Generate all bucket dates in the time range
	buckets := pr.generateTimeBuckets(startTime, endTime)

	for _, bucket := range buckets {
		// Query for source account
		sourceQuery := `
			SELECT payment_id, created_at, idempotency_key, status, amount, currency,
			source_account, target_account, description, updated_at, expires_at, metadata
			FROM payments
			WHERE bucket_date = ? AND source_account = ?
			AND created_at >= ? and created_at <= ?
			ORDER BY created_at DESC
			LIMIT ? ALLOW FILTERING`

		sourcePayments, err := pr.executePaymentQuery(ctx, sourceQuery, bucket, accountID, startTime, endTime, limit)

		if err != nil {
			return nil, err
		}
		payments = append(payments, sourcePayments...)

		// Query for target account
		targetQuery := `
			SELECT payment_id, created_at, idempotency_key, status, amount, currency,
				   source_account, target_account, description, updated_at, expires_at, metadata
			FROM payments 
			WHERE bucket_date = ? AND target_account = ? 
			AND created_at >= ? AND created_at <= ?
			ORDER BY created_at DESC
			LIMIT ? ALLOW FILTERING`

		targetPayments, err := pr.executePaymentQuery(ctx, targetQuery, bucket, accountID, startTime, endTime, limit)
		if err != nil {
			return nil, err
		}
		payments = append(payments, targetPayments...)

		if len(payments) >= limit {
			break
		}
	}

	// Remove duplicates and sort by created_at
	uniquePayments := pr.deduplicatePayments(payments)

	// Limit results
	if len(uniquePayments) > limit {
		uniquePayments = uniquePayments[:limit]
	}

	return uniquePayments, nil
}

// GetPaymentsByStatus retrieves payments by status within a time range
func (pr *PaymentRepository) GetPaymentsByStatus(ctx context.Context, status domain.PaymentStatus, startTime, endTime time.Time, limit int) ([]domain.Payment, error) {
	var payments []domain.Payment

	// Generate all bucket dates in the time range
	buckets := pr.generateTimeBuckets(startTime, endTime)

	for _, bucket := range buckets {
		query := `
			SELECT payment_id, created_at, idempotency_key, status, amount, currency, source_account, target_account, description, updated_at, expires_at, metadata
			FROM payments
			WHERE bucket_date = ? AND status = ? AND created_at >= ? AND created_at <= ? ORDER BY created_at DESC LIMIT ? ALLOW FILTERING`

		bucketPayments, err := pr.executePaymentQuery(ctx, query, bucket, string(status), startTime, endTime, limit)
		if err != nil {
			return nil, err
		}

		payments = append(payments, bucketPayments...)

		if len(payments) > limit {
			break
		}
	}

	// Limit result
	if len(payments) > limit {
		payments = payments[:limit]
	}

	return payments, nil
}

// GetExpiredPayments retrieves payments that have expired
func (pr *PaymentRepository) GetExpiredPayments(ctx context.Context, asOfTime time.Time, limit int) ([]domain.Payment, error) {
	var payments []domain.Payment

	now := time.Now()
	for i := 0; i < 7; i++ {
		bucketDate := GetTimeBucket(now.AddDate(0, 0, -i))

		query := `
			SELECT payment_id, created_at, idempotency_key, status, amount, currency, source_amount, target_amount, description, updated_at, expires_at, metadata
			FROM payments WHERE bucket_date = ? AND expires_at = ? AND status = ? ALLOW FILTERING`

		bucketPayments, err := pr.executePaymentQuery(ctx, query, bucketDate, asOfTime, string(domain.PaymentStatusAuthorized), time.Time{}, time.Time{}, limit)
		if err != nil {
			return nil, err
		}

		payments = append(payments, bucketPayments...)

		if len(payments) >= limit {
			break
		}
	}

	if len(payments) > limit {
		payments = payments[:limit]
	}

	return payments, nil

}

// executePaymentQuery executes a payment query and returns results
func (pr *PaymentRepository) executePaymentQuery(ctx context.Context, query string, args ...interface{}) ([]domain.Payment, error) {
	var payments []domain.Payment

	iter := pr.session.Query(query, args...).WithContext(ctx).Iter()

	for {
		var payment domain.Payment
		var paymentUUID gocql.UUID
		var statusStr string

		if !iter.Scan(&paymentUUID, &payment.CreatedAt, &payment.IdempotencyKey, &statusStr, &payment.Amount, &payment.Currency, &payment.SourceAccount, &payment.TargetAccount,
			&payment.Description, &payment.UpdatedAt, &payment.ExpiresAt, &payment.Metadata) {
			break
		}

		payment.PaymentID = paymentUUID.String()
		payment.Status = domain.PaymentStatus(statusStr)
		payments = append(payments, payment)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to execute payment query: %w", err)
	}

	return payments, nil
}

func (pr *PaymentRepository) deduplicatePayments(payments []domain.Payment) []domain.Payment {
	seen := make(map[string]bool)
	var unique []domain.Payment

	for _, payment := range payments {
		if !seen[payment.PaymentID] {
			seen[payment.PaymentID] = true
			unique = append(unique, payment)
		}
	}

	return unique
}

// generateTimeBuckets generates all date buckets between start and end times
func (pr *PaymentRepository) generateTimeBuckets(startTime, endTime time.Time) []string {
	var buckets []string

	current := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())
	end := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, endTime.Location())

	for current.Before(end) || current.Equal(end) {
		buckets = append(buckets, GetTimeBucket(current))
		current = current.AddDate(0, 0, 1)
	}

	return buckets
}
