package cassandra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Eomaxl/RaurPay/internal/domain"
	"github.com/gocql/gocql"
	"github.com/patrickmn/go-cache"
)

// PaymentRepository implements payment operations with Cassandra
type PaymentRepository struct {
	session  *gocql.Session
	prepared *PreparedPaymentStatements
	cache    *cache.Cache
	metrics  *RepositoryMetrics
	mu       *sync.RWMutex
}

type PreparedPaymentStatements struct {
	insertPayment       *gocql.Query
	getPayment          *gocql.Query
	updatePaymentStatus *gocql.Query
	getByIdempotencyKey *gocql.Query
	getBySourceAccount  *gocql.Query
	getByTargetAccount  *gocql.Query
	getByStatus         *gocql.Query
	insertPaymentEvent  *gocql.Query
}

type RepositoryMetrics struct {
	TotalQueries   int64
	CacheHits      int64
	CacheMisses    int64
	FailedQueries  int64
	AverageLatency time.Duration
	mu             sync.RWMutex
}

// NewPaymentRepository creates a new payment repository
func NewPaymentRepository(session *gocql.Session) (*PaymentRepository, error) {
	repo := &PaymentRepository{
		session:  session,
		prepared: &PreparedPaymentStatements{},
		cache:    cache.New(5*time.Minute, 10*time.Minute),
		metrics:  &RepositoryMetrics{},
	}

	// Initialize prepared statements
	if err := repo.initPreparedStatements(); err != nil {
		return nil, fmt.Errorf("failed to initialize prepared statements: %w", err)
	}

	return repo, nil
}

func (pr *PaymentRepository) initPreparedStatements() error {
	var err error

	// Insert payment
	pr.prepared.insertPayment = pr.session.Query(`
	INSERT INTO payments(payment_id, bucket_date, created_at, idempotency_key, status, amount, currency, source_account, target_account, desceiption, updated_at, expires_at, metadata)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`)

	// Get payment by ID ( will be used at different bucket_dates)
	pr.prepared.getPayment = pr.session.Query(`
	SELECT payment_id, created_at, idempotency_key, status, amount, currency, source_account, target_account, description, updated_at, expires_at, metadata
	FROM payments
	WHERE payment_id = ? AND bucket_date = ?
	`)

	// Update payment status
	pr.prepared.updatePaymentStatus = pr.session.Query(`
	UPDATE payment
	SET status = ? , updated_at = ?
	WHERE payment_id = ? AND bucket_date = ? AND created_at = ?
	`)

	// Get idempotency key (using materialized view)
	pr.prepared.getByIdempotencyKey = pr.session.Query(`
	SELECT payment_id, bucket_date, created_at, status, amount, currency, source_account, target_account
	FROM payments_by_idempotency_key
	WHERE idempotency_key = ?
	LIMIT 1
	`)

	// Insert payment event
	pr.prepared.insertPaymentEvent = pr.session.Query(`
		INSERT INTO payment_events (payment_id, event_id, event_type, old_status, new_status, created_at, metadata) VALUES (?,?,?,?,?,?,?)
	`)

	if err != nil {
		return fmt.Errorf("failed to prepare statements : %w", err)
	}

	return nil
}

// CreatePayment creates a new payment record
func (pr *PaymentRepository) CreatePayment(ctx context.Context, payment *domain.Payment) error {
	start := time.Now()
	defer pr.recordMetrics(start, nil)

	bucketDate := GetTimeBucket(payment.CreatedAt)

	paymentUUID, err := gocql.ParseUUID(payment.PaymentID)
	if err != nil {
		pr.recordMetrics(start, err)
		return fmt.Errorf("invalid payment ID: %w", err)
	}

	err = pr.executeWithRetry(ctx, func() error {
		return pr.prepared.insertPayment.Bind(
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
	})

	if err != nil {
		pr.recordMetrics(start, err)
		return fmt.Errorf("failed to create payment : %w", err)
	}

	// Store in cache
	cacheKey := fmt.Sprintf("payment:%s", payment.PaymentID)
	pr.cache.Set(cacheKey, payment, cache.DefaultExpiration)

	// Record payment creation event
	_ = pr.recordPaymentEvent(ctx, payment.PaymentID, "CREATED", "", string(payment.Status))

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

func (pr *PaymentRepository) recordMetrics(start time.Time, err error) {
	pr.metrics.mu.Lock()
	defer pr.metrics.mu.Unlock()

	pr.metrics.TotalQueries++

	if err != nil {
		pr.metrics.FailedQueries++
	}

	duration := time.Since(start)
	if pr.metrics.TotalQueries == 1 {
		pr.metrics.AverageLatency = duration
	} else {
		pr.metrics.AverageLatency = (pr.metrics.AverageLatency + duration) / 2
	}
}

func (pr *PaymentRepository) recordPaymentEvent(ctx context.Context, paymentID, eventType, oldStatus, newStatus string) error {
	paymentUUID, err := gocql.ParseUUID(paymentID)
	if err != nil {
		return err
	}

	eventID := gocql.TimeUUID()
	now := time.Now()

	return pr.prepared.insertPaymentEvent.Bind(
		paymentUUID,
		eventID,
		eventType,
		oldStatus,
		newStatus,
		now,
		map[string]string{},
	).WithContext(ctx).Exec()
}

func (pr *PaymentRepository) executeWithRetry(ctx context.Context, operation func() error) error {
	maxRetries := 3
	baseDelay := 100 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := operation()
		if err != nil {
			return nil
		}

		// Check if error is retryable
		if !pr.isRetryableError(err) {
			return err
		}

		// Last attempt - return error
		if attempt == maxRetries-1 {
			return fmt.Errorf("failed after %d retries : %w", maxRetries, err)
		}

		// Expotential backoff
		delay := baseDelay * time.Duration(1<<uint(attempt))
		select {
		case <-time.After(delay):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("max retries exceeded")
}

// isRetryableError determines if an error should trigger a retry
func (pr *PaymentRepository) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	retryableErrors := []string{
		"connection",
		"timeout",
		"overloaded",
		"unavailable",
		"read timeout",
		"write timeout",
	}

	for _, retryable := range retryableErrors {
		if contains(errStr, retryable) {
			return true
		}
	}

	return false
}

// GetMetrics returns repository metrics
func (pr *PaymentRepository) GetMetrics() RepositoryMetrics {
	pr.metrics.mu.RLock()
	defer pr.metrics.mu.Unlock()

	return RepositoryMetrics{
		TotalQueries:   pr.metrics.TotalQueries,
		CacheHits:      pr.metrics.CacheHits,
		CacheMisses:    pr.metrics.CacheMisses,
		FailedQueries:  pr.metrics.FailedQueries,
		AverageLatency: pr.metrics.AverageLatency,
	}
}

// ClearCache clears the repository cache
func (pr *PaymentRepository) ClearCache() {
	pr.cache.Flush()
}

// GetCacheStatus returns cache statistics
func (pr *PaymentRepository) GetCacheStatus() map[string]interface{} {
	pr.metrics.mu.RLock()
	defer pr.metrics.mu.Unlock()

	totalRequest := pr.metrics.CacheHits + pr.metrics.CacheMisses
	hitRate := float64(0)
	if totalRequest > 0 {
		hitRate = float64(pr.metrics.CacheHits) / float64(totalRequest) * 100
	}

	return map[string]interface{}{
		"cache_hits":       pr.metrics.CacheHits,
		"cache_misses":     pr.metrics.CacheMisses,
		"hit_rate_percent": hitRate,
		"cache_item_count": pr.cache.ItemCount(),
	}
}
