package domain

import (
	"errors"
	"fmt"
)

var (
	ErrWalletNotFound       = errors.New("wallet not found")
	ErrWalletPlayerMismatch = errors.New("wallet does not belong to player")
)

type Wallet struct {
	ID       string
	PlayerID string
	Currency string
	Balance  Money
	Version  int64
}

func NewWallet(id, playerID, currency string) (*Wallet, error) {
	if id == "" || playerID == "" {
		return nil, fmt.Errorf("id and playerID are required")
	}

	balance, err := NewMoney(0, currency)
	if err != nil {
		return nil, err
	}

	return &Wallet{
		ID:       id,
		PlayerID: playerID,
		Currency: balance.Currency(),
		Balance:  balance,
		Version:  1,
	}, nil
}

func (w *Wallet) Credit(amount Money) error {
	if amount.IsZero() {
		return ErrInvalidAmount
	}

	newBalance, err := w.Balance.Add(amount)
	if err != nil {
		return err
	}

	w.Balance = newBalance

	return nil
}

func (w *Wallet) Debit(amount Money) error {
	if amount.IsZero() {
		return ErrInvalidAmount
	}

	newBalance, err := w.Balance.Sub(amount)
	if err != nil {
		return err
	}

	w.Balance = newBalance

	return nil
}
