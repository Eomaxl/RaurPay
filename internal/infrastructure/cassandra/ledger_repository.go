package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/Eomaxl/RaurPay/internal/domain"
	"github.com/gocql/gocql"
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
