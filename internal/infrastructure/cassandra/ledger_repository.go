package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/Eomaxl/RaurPay/internal/domain"
	"github.com/gocql/gocql"
	"github.com/shopspring/decimal"
)

// LedgerRepository implements ledger operations with Cassandra
type LedgerRepository struct {
	session *gocql.Session
}

// NewLedgerRepository creates a new ledger repository
func NewLedgerRepository(session *gocql.Session) *LedgerRepository {
	return &LedgerRepository{
		session: session,
	}
}

// CreateLedgerEntry creates a new immutable ledger entry
func (lr *LedgerRepository) CreateLedgerEntry(ctx context.Context, entry *domain.LedgerEntry) error {
	bucketDate := GetTimeBucket(entry.Timestamp)

	query := `
		INSERT INTO ledger_enteries (
			account_id, bucket_date, entry_timestamp, entry_id, transaction_id, amount, entry_type, description, metadata)
			VALUES (?,?,?,?,?,?,?,?,?)`

	entryUUID, err := gocql.ParseUUID(entry.EntryID)
	if err != nil {
		return fmt.Errorf("invalid entry ID: %w", err)
	}

	transactionUUID, err := gocql.ParseUUID(entry.TransactionID)
	if err != nil {
		return fmt.Errorf("failed to create ledger entry: %w", err)
	}

	err = lr.session.Query(query,
		entry.AccountID,
		bucketDate,
		entry.Timestamp,
		entryUUID,
		transactionUUID,
		entry.Amount,
		string(entry.EntryType),
		entry.Description,
		entry.Metadata,
	).WithContext(ctx).Exec()

	if err != nil {
		return fmt.Errorf("failed to created ledger entry: %w", err)
	}

	return nil
}

// GetLedgerEntry retrieves a specfic ledger entry
func (lr *LedgerRepository) GetLedgerEntry(ctx context.Context, accountId, entryId string) (*domain.LedgerEntry, error) {
	// Since we need to query across time buckets, we'll need to search recent buckets
	// In production, you'd maintain an index or use a different approach
	now := time.Now()

	// Search last 30 days of buckets
	for i := 0; i < 30; i++ {
		bucketDate := GetTimeBucket(now.AddDate(0, 0, -i))

		query := `
		SELECT entry_id, transaction_id, account_id, amount, entry_type, description, entry_timestamp, metadata FROM ledger_entries WHERE account_id ? AND bucket_date = ? AND entry_id = ?`

		var entry domain.LedgerEntry
		var entryIDUUID gocql.UUID
		var transactionIDUUID gocql.UUID
		var entryTypeStr string

		entryUUID, err := gocql.ParseUUID(entryId)
		if err != nil {
			return nil, fmt.Errorf("invalid entry ID: %w", err)
		}

		err = lr.session.Query(query, accountId, bucketDate, entryUUID).
			WithContext(ctx).
			Scan(&entryIDUUID, &transactionIDUUID, &entry.AccountID, &entry.Amount, &entryTypeStr, &entry.Description, &entry.Timestamp, &entry.Metadata)

		if err == nil {
			entry.EntryID = entryIDUUID.String()
			entry.TransactionID = transactionIDUUID.String()
			entry.EntryType = domain.EntryType(entryTypeStr)
			return &entry, nil
		}

		if err != gocql.ErrNotFound {
			return nil, fmt.Errorf("failed to get ledger entry: %w", err)
		}
	}
	return nil, domain.ErrPaymentNotFound
}

// GetAccountLedgerHistory retrieves ledger history for an account within a time range
func (lr *LedgerRepository) GetAccountLedgerHistory(ctx context.Context, accountID string, startTime, endTime time.Time, limit int) ([]domain.LedgerEntry, error) {
	var entries []domain.LedgerEntry

	// Generate all bucket dates in the time range
	buckets := lr.generateTimeBuckets(startTime, endTime)

	for _, bucket := range buckets {
		query := `
			SELECT entry_id, transaction_id, account_id, amount, entry_type, description, entry_timestamp, metadata
			FROM ledger_entries
			WHERE account_id = ? AND bucket_date = ?
			AND entry_timestamp >= ? AND entry_timestamp <= ?
			ORDER BY entry_timestamp DESC
			LIMIT ?
		`

		iter := lr.session.Query(query, accountID, bucket, startTime, endTime, limit).WithContext(ctx).Iter()

		for {
			var entry domain.LedgerEntry
			var entryIDUUID gocql.UUID
			var transactionIDUUID gocql.UUID
			var entryTypeStr string

			if !iter.Scan(&entryIDUUID, &transactionIDUUID, &entry.AccountID, &entry.Amount, &entryTypeStr, &entry.Description, &entry.Timestamp, &entry.Metadata) {
				break
			}

			entry.EntryID = entryIDUUID.String()

			entry.EntryID = entryIDUUID.String()
			entry.TransactionID = transactionIDUUID.String()
			entry.EntryType = domain.EntryType(entryTypeStr)
			entries = append(entries, entry)

			if len(entries) >= limit {
				break
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("failed to get ledger history: %w", err)
		}

		if len(entries) >= limit {
			break
		}
	}
	return entries, nil
}

// GetTransactionEntries retrieves all entries for a specific transaction
func (lr *LedgerRepository) GetTransactionEntries(ctx context.Context, transactionID string) ([]domain.LedgerEntry, error) {
	// This requires a secondary index or denormalization in production
	// For now, we'll implement a basic version that searches recent buckets
	var entries []domain.LedgerEntry
	now := time.Now()

	transactionUUID, err := gocql.ParseUUID(transactionID)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction ID: %w", err)
	}

	// Search last 7 days of buckets across all accounts (not efficient, but functional)
	for i := 0; i < 7; i++ {
		bucketDate := GetTimeBucket(now.AddDate(0, 0, -i))

		// This would require ALLOW FILTERING in production - not recommended
		// Better to maintain a transaction_entries table
		query := `
			SELECT entry_id, transaction_id, account_id, amount, entry_type, 
				   description, entry_timestamp, metadata
			FROM ledger_entries 
			WHERE bucket_date = ? AND transaction_id = ? ALLOW FILTERING`

		iter := lr.session.Query(query, bucketDate, transactionUUID).
			WithContext(ctx).Iter()

		for {
			var entry domain.LedgerEntry
			var entryIDUUID gocql.UUID
			var transactionIDUUID gocql.UUID
			var entryTypeStr string

			if !iter.Scan(&entryIDUUID, &transactionIDUUID, &entry.AccountID,
				&entry.Amount, &entryTypeStr, &entry.Description,
				&entry.Timestamp, &entry.Metadata) {
				break
			}

			entry.EntryID = entryIDUUID.String()
			entry.TransactionID = transactionIDUUID.String()
			entry.EntryType = domain.EntryType(entryTypeStr)
			entries = append(entries, entry)
		}

		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("failed to get transaction entries: %w", err)
		}
	}

	return entries, nil
}

// CalculateAccountBalance calculates account balance from ledger entries
func (lr *LedgerRepository) CalculateAccountBalance(ctx context.Context, accountID string, asOfTime time.Time) (decimal.Decimal, error) {
	balance := decimal.Zero

	// Get all entries up to the specified time
	entries, err := lr.GetAccountLedgerHistory(ctx, accountID, time.Time{}, asOfTime, 10000)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to get ledger history for balance calculation: %w", err)
	}

	for _, entry := range entries {
		switch entry.EntryType {
		case domain.EntryTypeDebit:
			balance = balance.Sub(entry.Amount)
		case domain.EntryTypeCredit:
			balance = balance.Add(entry.Amount)
		}
	}

	return balance, nil
}

// ValidateDoubleEntry validates that a set of entries follows double-entry accounting
func (lr *LedgerRepository) ValidateDoubleEntry(entries []domain.LedgerEntry) error {
	totalDebits := decimal.Zero
	totalCredits := decimal.Zero

	for _, entry := range entries {
		switch entry.EntryType {
		case domain.EntryTypeDebit:
			totalDebits = totalDebits.Add(entry.Amount)
		case domain.EntryTypeCredit:
			totalCredits = totalCredits.Add(entry.Amount)
		default:
			return fmt.Errorf("invalid entry type: %s", entry.EntryType)
		}
	}

	if !totalDebits.Equal(totalCredits) {
		return domain.ErrDoubleEntryViolation
	}

	return nil
}

// CreateTransaction creates multiple ledger entries as a transaction
func (lr *LedgerRepository) CreateTransaction(ctx context.Context, transaction *domain.Transaction) error {
	// Validate double-entry accounting
	if err := lr.ValidateDoubleEntry(transaction.Entries); err != nil {
		return err
	}

	// Create batch for atomic writes within the same partition
	// Note: Cross-partition atomicity requires additional coordination
	batch := lr.session.NewBatch(gocql.LoggedBatch)

	for _, entry := range transaction.Entries {
		bucketDate := GetTimeBucket(entry.Timestamp)

		entryUUID, err := gocql.ParseUUID(entry.EntryID)
		if err != nil {
			return fmt.Errorf("invalid entry ID: %w", err)
		}

		transactionUUID, err := gocql.ParseUUID(entry.TransactionID)
		if err != nil {
			return fmt.Errorf("invalid transaction ID: %w", err)
		}

		batch.Query(`
			INSERT INTO ledger_entries (
				account_id, bucket_date, entry_timestamp, entry_id, 
				transaction_id, amount, entry_type, description, metadata
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.AccountID,
			bucketDate,
			entry.Timestamp,
			entryUUID,
			transactionUUID,
			entry.Amount,
			string(entry.EntryType),
			entry.Description,
			entry.Metadata,
		)
	}

	err := lr.session.ExecuteBatch(batch.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	return nil
}

// generateTimeBuckets generates all date buckets between start and end times
func (lr *LedgerRepository) generateTimeBuckets(startTime, endTime time.Time) []string {
	var buckets []string

	current := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())
	end := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, endTime.Location())

	for current.Before(end) || current.Equal(end) {
		buckets = append(buckets, GetTimeBucket(current))
		current = current.AddDate(0, 0, 1)
	}

	return buckets
}
