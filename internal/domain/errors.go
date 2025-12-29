package domain

import "errors"

var (
	// ErrDoubleEntryViolation indicates that debits don't equal credits
	ErrDoubleEntryViolation = errors.New("double-entry accounting violation: debits must equal credits")

	// ErrInsufficientFunds indicates insufficient account balance
	ErrInsufficientFunds = errors.New("Insufficient funds")

	// ErrAccountNotFound indicates account doesn't exist
	ErrAccountNotFound = errors.New("account not found")

	// ErrPaymentNotFound indicates payment doesn't exist
	ErrPaymentNotFound = errors.New("payment not found")

	// ErrInvalidPaymentStatus indicates invalid payment status transition
	ErrInvalidPaymentStatus = errors.New("invalid payment status")

	// ErrIdempotencyKeyConflict indicates idempotency key already exists
	ErrIdempotencyKeyConflict = errors.New("idempotency key conflict")

	// ErrInvalidAmount indicates invalid monetary amount
	ErrInvalidAmount = errors.New("invalid amount")
)
