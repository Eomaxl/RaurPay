package decimal

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Operations provieds safe decimal arithmetic operatiosn for financial calculations
type Operations struct {
	maxDecimalPlaces int32
}

// NewOperations creates a new decimal operations handler
func NewOperations(maxDecimalPlaces int32) *Operations {
	return &Operations{
		maxDecimalPlaces: maxDecimalPlaces,
	}
}

// Add performs addition of two decimal values
func (o *Operations) Add(a, b decimal.Decimal) decimal.Decimal {
	result := a.Add(b)
	return result.Truncate(o.maxDecimalPlaces)
}

// Subtract performs subtraction of two decimal values
func (o *Operations) Subtract(a, b decimal.Decimal) decimal.Decimal {
	result := a.Sub(b)
	return result.Truncate(o.maxDecimalPlaces)
}

// Multiply performs multiplication of two decimal values
func (o *Operations) Multiply(a, b decimal.Decimal) decimal.Decimal {
	result := a.Mul(b)
	return result.Truncate(o.maxDecimalPlaces)
}

// Divide performs division of two decimal values
func (o *Operations) Divide(a, b decimal.Decimal) (decimal.Decimal, error) {
	if b.IsZero() {
		return decimal.Zero, fmt.Errorf("division by zero")
	}
	result := a.Div(b)
	return result.Truncate(o.maxDecimalPlaces), nil
}

// IsPositive checks if a decimal value is positive
func (o *Operations) IsPositive(d decimal.Decimal) bool {
	return d.GreaterThan(decimal.Zero)
}

// IsNegative checks if a decimal value is negative
func (o *Operations) IsNegative(d decimal.Decimal) bool {
	return d.LessThan(decimal.Zero)
}

// IsZero checks if a decimal value is zero
func (o *Operations) IsZero(d decimal.Decimal) bool {
	return d.IsZero()
}

// Equal checks if two decimal values are equal
func (o *Operations) Equal(a, b decimal.Decimal) bool {
	return a.Equal(b)
}

// GreaterThan checks if a is greater than b
func (o *Operations) GreaterThan(a, b decimal.Decimal) bool {
	return a.GreaterThan(b)
}

// LessThan checks if a is less than b
func (o *Operations) LessThan(a, b decimal.Decimal) bool {
	return a.LessThan(b)
}

// Abs returns the absolute value of a decimal
func (o *Operations) Abs(d decimal.Decimal) decimal.Decimal {
	return d.Abs()
}

// ValidateAmount validates that an amount is valid for financial operations
func (o *Operations) ValidateAmount(amount decimal.Decimal) error {
	if amount.IsNegative() {
		return fmt.Errorf("amount cannot be negative")
	}

	// Check decimal places by truncating and comparing
	truncated := amount.Truncate(o.maxDecimalPlaces)
	if !amount.Equal(truncated) {
		return fmt.Errorf("amount has too many decimal places (max: %d)", o.maxDecimalPlaces)
	}

	return nil
}

// SumEntries calculates the sum of decimal values in a slice
func (o *Operations) SumEntries(amounts []decimal.Decimal) decimal.Decimal {
	sum := decimal.Zero
	for _, amount := range amounts {
		sum = o.Add(sum, amount)
	}
	return sum
}

// ValidateDoubleEntry validates that debits equal credits
func (o *Operations) ValidateDoubleEntry(debits, credits []decimal.Decimal) error {
	totalDebits := o.SumEntries(debits)
	totalCredits := o.SumEntries(credits)

	if !o.Equal(totalDebits, totalCredits) {
		return fmt.Errorf("double-entry violation: debits (%s) must equal credits (%s)",
			totalDebits.String(), totalCredits.String())
	}

	return nil
}
