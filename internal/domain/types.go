package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

// Paymentstatus represents the current state of a payment
type PaymentStatus string

const (
	PaymentStatusPending    PaymentStatus = "pending"
	PaymentStatusAuthorized PaymentStatus = "authorized"
	PaymentStatusCaptured   PaymentStatus = "captured"
	PaymentStatusReversed   PaymentStatus = "reversed"
	PaymentStatusFailed     PaymentStatus = "failed"
	PaymentStatusExpired    PaymentStatus = "expired"
)

// EntryType representa the type of ledger entry
type EntryType string

const (
	EntryTypeDebit  EntryType = "debit"
	EntryTypeCredit EntryType = "credit"
)

// Payment represents a payment in the system
type Payment struct {
	PaymentID      string            `json:"payment_id`
	IdempotencyKey string            `json:"idempotency_key"`
	Amount         decimal.Decimal   `json:"amount"`
	Currency       string            `json:"currency"`
	SourceAmount   string            `json:"source_account"`
	TargetAmount   string            `json:"target_account"`
	Description    string            `json:"description"`
	Status         PaymentStatus     `json:"status"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	ExpiresAt      *time.Time        `json:"expires_at,omitempty"`
	Metadata       map[string]string `json:"metadata"`
}

// LedgerEntry represent an entry in the double-entry ledger
type LedgerEntry struct {
	EntryID       string            `json:"entry_id"`
	TransactionID string            `json:"transaction_id"`
	AccountID     string            `json:"account_id"`
	Amount        decimal.Decimal   `json:"amount"`
	EntryType     EntryType         `json:"entry_type"`
	Description   string            `json:"description"`
	Timestamp     time.Time         `json:"timestamp"`
	Metadata      map[string]string `json:"metadata"`
}

// Account represents an account in the system
type Account struct {
	AccountID        string          `json:"account_id"`
	Balance          decimal.Decimal `json:"balance"`
	AvailableBalance decimal.Decimal `json:"available_balance"`
	ReservedBalance  decimal.Decimal `json:"reserved_balance"`
	Currency         string          `json:"currency"`
	Status           string          `json:"status"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	Version          int64           `json:"version"`
}

// Transaction represents a complete financial transaction
type Transaction struct {
	TransactionID string            `json:"transaction_id"`
	PaymentID     string            `json:"payment_id"`
	Entries       []LedgerEntry     `json:"entries"`
	TotalAmount   decimal.Decimal   `json:"total_amount"`
	Currency      string            `json:"currency"`
	Description   string            `json:"description"`
	CreatedAt     time.Time         `json:"created_at"`
	Metadata      map[string]string `json:"metadata"`
}

// ValidateDoubleEntry validates that debits equal credits in a transaction
func (t *Transaction) ValidateDoubleEntry() error {
	var totalDebits, totalCredits decimal.Decimal

	for _, entry := range t.Entries {
		switch entry.EntryType {
		case EntryTypeDebit:
			totalDebits = totalDebits.Add(entry.Amount)
		case EntryTypeCredit:
			totalCredits = totalCredits.Add(entry.Amount)
		}
	}

	if !totalDebits.Equal(totalCredits) {
		return ErrDoubleEntryViolation
	}

	return nil
}
