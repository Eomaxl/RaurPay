package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

// SchemaManager handles Cassandra schema creation and migration
type SchemaManager struct {
	session *gocql.Session
}

// NewSchemaManager creates a new schema manager instance
func NewSchemaManager(session *gocql.Session) *SchemaManager {
	return &SchemaManager{
		session: session,
	}
}

// CreateTables creates all required table with time-bucketed partitioning
func (sm *SchemaManager) CreateTables() error {
	tables := []string{
		sm.createPaymentsTable(),
		sm.createLedgerEntriesTable(),
		sm.createAccountBalancesTable(),
		sm.createOutboxEventsTable(),
		sm.createIdempotencyKeysTable(),
	}

	for _, tableQuery := range tables {
		if err := sm.session.Query(tableQuery).Exec(); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}

// createPaymentsTable creates the payments table with time-bucketed partitioning
func (sm *SchemaManager) createPaymentsTable() string {
	return `
		CREATE TABLE IF NOT EXISTS payments (
			payment_id UUID,
			bucket_date DATE,
			created_at TIMESTAMP,
			idempotency_key TEXT,
			status TEXT,
			amount DECIMAL,
			currency TEXT,
			source_account TEXT,
			target_account TEXT,
			description TEXT,
			updated_at TIMESTAMP,
			expires_at TIMESTAMP,
			metadata MAP<TEXT,TEXT>
			PRIMARY KEY ((payment_id, bucket_date), created_at)
		) WITH CLUSTERING ORDER BY (created_at DESC)
		 AND gc_grace_seconds = 864000
		 AND compaction = {
		 	'class':'TimeWindowCompactionStrategy',
			'compaction_window_unit':'DAYS',
			'compaction_window_size': 1
		 }`
}

// createLedgersTanle creates the ledger entries table with time-bucketed partitioning
func (sm *SchemaManager) createLedgerEntriesTable() string {
	return `
		CREATE TABLE IF NOT EXISTS ledger_entries (
			account_id TEXT,
			bucket_date DATE,
			entry_timestamp TIMESTAMP,
			entry_id UUID,
			transaction_id UUID,
			amount DECIMAL,
			entry_type TEXT,
			description TEXT,
			metadata MAP<TEXT, TEXT>
			PRIMARY KEY ((account_id, bucket_date), entry_timestamp. entry_id)
		) WITH CLUSTERING ORDER BY (entry_timestamp DESC, entry_id ASC)
		 AND gc_grace_seconds = 0
		 AND compaction = {
		 	'class': 'TimeWindowCompactionStrategy',
			'compaction_window_unit':'DAYS',
			'compaction_window_size': 1
		 }`
}

// createAccountBalancesTable creates the account balances table for read models
func (sm *SchemaManager) createAccountBalancesTable() string {
	return `
		CREATE TABLE IF NOT EXISTS account_balances (
			account_id TEXT,
			balance DECIMAL,
			available_balance DECIMAL,
			reserved_balance DECIMAL,
			currency TEXT,
			status TEXT,
			last_updated TIMESTAMP,
			version BIGINT,
			created_at TIMESTAMP,
			PRIMARY KEY (account_id)
		) WITH gc_grace_seconds = 864000
		 AND compaction = {
		 	'class':'LeveledCompactionStrategy'
		 }`
}

// createOutboxEventsTable creates the outbox events table for transactional consistency
func (sm *SchemaManager) createOutboxEventsTable() string {
	return `
		CREATE TABLE IF NOT EXISTS outbox_events (
			service_name TEXT,
			bucket_hour TIMESTAMP,
			event_id UUID,
			event_type TEXT,
			event_data TEXT,
			created_at TIMESTAMP,
			processed BOOLEAN,
			processed_at TIMESTAMP,
			PRIMARY KEY ((service_name, bucket_hour), created_at, event_id)
		) WITH CLUSTERING ORDER BY (created_at ASC, event_id ASC)
		 AND gc_grace_seconds = 86400
		 AND compaction = {
		 	'class': 'TimeWindowCompactionStrategy',
			'compaction_window_unit':'HOURS',
			'compaction_window_size': 1
		 }`
}

// createIdempotencyKeysTable creates the idempotency keys table
func (sm *SchemaManager) createIdempotencyKeysTable() string {
	return `
		CREATE TABLE IF NOT EXISTS idempotency_keys (
			key TEXT,
			payment_id TEXT,
			request_hash TEXT,
			response TEXT,
			status TEXT,
			created_at TIMESTAMP,
			expires_at TIMESTAMP,
			metadata TEXT,
			PRIMARY_KEY (key)
			) WITH default_time_to_live = 86400
			AND gc_grace_seconds = 86400
			AND compaction = {
				'class':'LeveledCompactionStrategy'
			}`
}

// GetTimeBucket returns the date bucket for time-bucketed partitioning
func GetTimeBucket(t time.Time) string {
	return t.Format("2006-01-02")
}

// GetHourBucket return the hour bucket for the hourly partitioning
func GetHourBucket(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
}

// DropTables drop all tables  (for testing purpose)
func (sm *SchemaManager) DropTables() error {
	tables := []string{
		"payments",
		"ledger_entries",
		"account_balance",
		"outbox_events",
		"idempotency_keys",
	}

	for _, table := range tables {
		query := fmt.Sprintf("DROP TABLES IF EXISTS %s", table)
		if err := sm.session.Query(query).Exec(); err != nil {
			return fmt.Errorf("failed to drop table %s : %w", table, err)
		}
	}
	return nil
}
