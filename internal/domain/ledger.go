package domain

import "errors"

type LedgerDirection string

const (
	LedgerCredit LedgerDirection = "CREDIT"
	LedgerDebit  LedgerDirection = "DEBIT"
)

var (
	ErrInvalidLedgerDirection = errors.New("invalid ledger direction")
)

type LedgerEntry struct {
	ID            string
	WalletID      string
	TransactionID string
	Direction     LedgerDirection
	Amount        Money
	BalanceBefore Money
	BalanceAfter  Money
}

func NewLedgerEntry(
	id string,
	walletID string,
	transactionID string,
	direction LedgerDirection,
	amount Money,
	balanceBefore Money,
	balanceAfter Money,
) (*LedgerEntry, error) {
	if id == "" {
		return nil, errors.New("ledger entry id is required")
	}

	if walletID == "" {
		return nil, errors.New("wallet id is required")
	}

	if transactionID == "" {
		return nil, errors.New("transaction id is required")
	}

	if direction != LedgerCredit && direction != LedgerDebit {
		return nil, ErrInvalidLedgerDirection
	}

	if amount.IsZero() {
		return nil, ErrInvalidAmount
	}

	if amount.Currency() != balanceBefore.Currency() ||
		amount.Currency() != balanceAfter.Currency() {
		return nil, ErrCurrencyMismatch
	}

	return &LedgerEntry{
		ID:            id,
		WalletID:      walletID,
		TransactionID: transactionID,
		Direction:     direction,
		Amount:        amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
	}, nil
}
