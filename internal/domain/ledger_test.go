package domain

import (
	"errors"
	"testing"
)

func TestNewLedgerEntry(t *testing.T) {
	t.Run("valid credit", func(t *testing.T) {
		entry, err := NewLedgerEntry(
			"entry-1",
			"wallet-1",
			"tx-1",
			LedgerCredit,
			mustMoney(t, 400, "BRL"),
			mustMoney(t, 1000, "BRL"),
			mustMoney(t, 1400, "BRL"),
		)
		if err != nil {
			t.Fatalf("NewLedgerEntry: unexpected error: %v", err)
		}
		if entry.Direction != LedgerCredit {
			t.Errorf("Direction = %q, want CREDIT", entry.Direction)
		}
		if entry.BalanceAfter.Cents() != 1400 {
			t.Errorf("BalanceAfter = %d, want 1400", entry.BalanceAfter.Cents())
		}
	})

	t.Run("valid debit", func(t *testing.T) {
		entry, err := NewLedgerEntry(
			"entry-2",
			"wallet-1",
			"tx-2",
			LedgerDebit,
			mustMoney(t, 400, "BRL"),
			mustMoney(t, 1000, "BRL"),
			mustMoney(t, 600, "BRL"),
		)
		if err != nil {
			t.Fatalf("NewLedgerEntry: unexpected error: %v", err)
		}
		if entry.Direction != LedgerDebit {
			t.Errorf("Direction = %q, want DEBIT", entry.Direction)
		}
		if entry.BalanceAfter.Cents() != 600 {
			t.Errorf("BalanceAfter = %d, want 600", entry.BalanceAfter.Cents())
		}
	})

	t.Run("credit with wrong balance after", func(t *testing.T) {
		_, err := NewLedgerEntry(
			"entry-3",
			"wallet-1",
			"tx-3",
			LedgerCredit,
			mustMoney(t, 400, "BRL"),
			mustMoney(t, 1000, "BRL"),
			mustMoney(t, 1399, "BRL"),
		)
		if !errors.Is(err, ErrInvalidLedgerBalance) {
			t.Fatalf("expected ErrInvalidLedgerBalance, got %v", err)
		}
	})

	t.Run("debit with wrong balance after", func(t *testing.T) {
		_, err := NewLedgerEntry(
			"entry-4",
			"wallet-1",
			"tx-4",
			LedgerDebit,
			mustMoney(t, 400, "BRL"),
			mustMoney(t, 1000, "BRL"),
			mustMoney(t, 601, "BRL"),
		)
		if !errors.Is(err, ErrInvalidLedgerBalance) {
			t.Fatalf("expected ErrInvalidLedgerBalance, got %v", err)
		}
	})

	t.Run("debit without sufficient funds", func(t *testing.T) {
		_, err := NewLedgerEntry(
			"entry-5",
			"wallet-1",
			"tx-5",
			LedgerDebit,
			mustMoney(t, 600, "BRL"),
			mustMoney(t, 500, "BRL"),
			mustMoney(t, 0, "BRL"),
		)
		if !errors.Is(err, ErrInsufficientFunds) {
			t.Fatalf("expected ErrInsufficientFunds, got %v", err)
		}
	})
}

func TestDomainConflictErrors(t *testing.T) {
	if ErrWalletPlayerMismatch == nil {
		t.Error("ErrWalletPlayerMismatch is nil")
	}
	if ErrIdempotencyConflict == nil {
		t.Error("ErrIdempotencyConflict is nil")
	}
	if ErrExternalTransactionConflict == nil {
		t.Error("ErrExternalTransactionConflict is nil")
	}
	if ErrInvalidLedgerBalance == nil {
		t.Error("ErrInvalidLedgerBalance is nil")
	}
}
