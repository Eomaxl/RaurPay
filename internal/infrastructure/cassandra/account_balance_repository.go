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

// AccountBalanceRepository implements account balance operations
type AccountBalanceRepository struct {
	session  *gocql.Session
	prepared *PreparedPaymentStatements
	cache    *cache.Cache
	metrics  *RepositoryMetrics
	mu       sync.RWMutex
}

// PreparedAccountsStatements holds prepared statements for account operations
type PreparedAccountsStatements struct {
	createAccount *gocql.Query
	getAccount    *gocql.Query
	updateBalance *gocql.Query
	updateStatus  *gocql.Query
	reserveFunds  *gocql.Query
	releaseFunds  *gocql.Query
}

// NewAccountBalanceRepository creates an account balance repository
func NewAccountBalanceRepository(session *gocql.Session) (*AccountBalanceRepository, error) {
	repo := &AccountBalanceRepository{
		session:  session,
		prepared: &PreparedPaymentStatements{},
		cache:    cache.New(2*time.Minute, 5*time.Minute),
		metrics:  &RepositoryMetrics{},
	}

	if err := repo.initPreparedStatements(); err != nil {
		return nil, fmt.Errorf("failed to initialize prepared statements: %w", err)
	}

	return repo, nil
}

// initPreparedStatements prepare all frequently used queries
func (abr *AccountBalanceRepository) initPreparedStatements() error {
	abr.prepared.createAccount = abr.session.Query(`
		INSERT INTO account_balances (
			account_id, balance, available_balance, reserved_balance,
			currency, status, last_updated, version, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)

	// Get account
	abr.prepared.getAccount = abr.session.Query(`
		SELECT account_id, balance, available_balance, reserved_balance,
			   currency, status, last_updated, version, created_at
		FROM account_balances 
		WHERE account_id = ?
	`)

	// Update balance with version check (optimistic locking)
	abr.prepared.updateBalance = abr.session.Query(`
		UPDATE account_balances 
		SET balance = ?, available_balance = ?, reserved_balance = ?, 
			last_updated = ?, version = ?
		WHERE account_id = ? 
		IF version = ?
	`)

	// Update status
	abr.prepared.updateStatus = abr.session.Query(`
		UPDATE account_balances 
		SET status = ?, last_updated = ?, version = ?
		WHERE account_id = ? 
		IF version = ?
	`)

	return nil
}

// TODO
// CreateAccount creates a new account with initial balance
func (abr *AccountBalanceRepository) CreateAccount(ctx context.Context, account *domain.Account) error {
}
