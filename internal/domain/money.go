package domain

import (
	"errors"
	"math"
	"strings"
)

type Money struct {
	cents    int64
	currency string
}

var (
	ErrNegativeAmount    = errors.New("amount cannot be negative")
	ErrInvalidCurrency   = errors.New("invalid currency")
	ErrCurrencyMismatch  = errors.New("currency mismatch")
	ErrOverflow          = errors.New("amount overflow")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidAmount     = errors.New("amount must be greater than zero")
)

func validCurrency(c string) bool {
	if len(c) != 3 {
		return false
	}
	for _, r := range c {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func NewMoney(cents int64, currency string) (Money, error) {
	if cents < 0 {
		return Money{}, ErrNegativeAmount
	}

	currency = strings.ToUpper(currency)
	if !validCurrency(currency) {
		return Money{}, ErrInvalidCurrency
	}

	return Money{cents: cents, currency: currency}, nil
}

func (m Money) Cents() int64 {
	return m.cents
}

func (m Money) Currency() string {
	return m.currency
}

func (m Money) IsZero() bool {
	return m.cents == 0
}

func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	if m.cents > math.MaxInt64-other.cents {
		return Money{}, ErrOverflow
	}

	return Money{
		cents:    m.cents + other.cents,
		currency: m.currency,
	}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	if m.cents < other.cents {
		return Money{}, ErrInsufficientFunds
	}

	return Money{
		cents:    m.cents - other.cents,
		currency: m.currency,
	}, nil
}

func (m Money) LessThan(other Money) bool {
	return m.cents < other.cents
}
