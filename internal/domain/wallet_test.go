package domain

import (
	"errors"
	"testing"
)

func newTestWallet(t *testing.T, currency string, initialCents int64) *Wallet {
	t.Helper()

	w, err := NewWallet("w1", "p1", currency)
	if err != nil {
		t.Fatalf("NewWallet unexpected error: %v", err)
	}

	if initialCents > 0 {
		if err := w.Credit(mustMoney(t, initialCents, currency)); err != nil {
			t.Fatalf("Credit setup unexpected error: %v", err)
		}
	}

	return w
}

func TestNewWallet(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		playerID string
		currency string
		wantErr  bool
	}{
		{name: "valid", id: "w1", playerID: "p1", currency: "BRL"},
		{name: "lowercase currency is normalized", id: "w1", playerID: "p1", currency: "brl"},

		{name: "empty id", id: "", playerID: "p1", currency: "BRL", wantErr: true},
		{name: "empty player id", id: "w1", playerID: "", currency: "BRL", wantErr: true},
		{name: "empty currency", id: "w1", playerID: "p1", currency: "", wantErr: true},
		{name: "invalid currency", id: "w1", playerID: "p1", currency: "BR", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWallet(tt.id, tt.playerID, tt.currency)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got wallet %+v", w)
				}
				if w != nil {
					t.Errorf("expected nil wallet on error, got %+v", w)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if w.ID != tt.id {
				t.Errorf("ID = %q, want %q", w.ID, tt.id)
			}
			if w.PlayerID != tt.playerID {
				t.Errorf("PlayerID = %q, want %q", w.PlayerID, tt.playerID)
			}
			if w.Currency != "BRL" {
				t.Errorf("Currency = %q, want BRL", w.Currency)
			}
			if !w.Balance.IsZero() {
				t.Errorf("initial balance = %d, want 0", w.Balance.Cents())
			}
			if w.Balance.Currency() != "BRL" {
				t.Errorf("Balance currency = %q, want BRL", w.Balance.Currency())
			}
			if w.Version != 1 {
				t.Errorf("Version = %d, want 1", w.Version)
			}
		})
	}
}

func TestWallet_Credit(t *testing.T) {
	t.Run("increases balance", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 0)

		if err := w.Credit(mustMoney(t, 1000, "BRL")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if w.Balance.Cents() != 1000 {
			t.Errorf("balance = %d, want 1000", w.Balance.Cents())
		}
	})

	t.Run("accumulates multiple credits", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 0)

		_ = w.Credit(mustMoney(t, 300, "BRL"))
		_ = w.Credit(mustMoney(t, 700, "BRL"))

		if w.Balance.Cents() != 1000 {
			t.Errorf("balance = %d, want 1000", w.Balance.Cents())
		}
	})

	t.Run("currency mismatch keeps balance", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 500)

		err := w.Credit(mustMoney(t, 100, "USD"))
		if !errors.Is(err, ErrCurrencyMismatch) {
			t.Fatalf("expected ErrCurrencyMismatch, got %v", err)
		}
		if w.Balance.Cents() != 500 {
			t.Errorf("balance changed: %d", w.Balance.Cents())
		}
	})

	t.Run("overflow keeps balance", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 0)
		w.Balance = mustMoney(t, 9223372036854775807, "BRL") // math.MaxInt64

		err := w.Credit(mustMoney(t, 1, "BRL"))
		if err == nil {
			t.Fatal("expected overflow error")
		}
		if w.Balance.Cents() != 9223372036854775807 {
			t.Errorf("balance changed: %d", w.Balance.Cents())
		}
	})

	t.Run("zero Money value is rejected", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 500)

		// Money{} tem moeda vazia, então cai em currency mismatch.
		if err := w.Credit(Money{}); err == nil {
			t.Fatal("expected error for zero-value Money")
		}
		if w.Balance.Cents() != 500 {
			t.Errorf("balance changed: %d", w.Balance.Cents())
		}
	})

	// FALHA com o código atual: Credit não rejeita valor zero.
	t.Run("zero amount is rejected", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 500)

		if err := w.Credit(mustMoney(t, 0, "BRL")); err == nil {
			t.Fatal("expected error for zero amount")
		}
	})

	t.Run("does not change version", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 0)
		before := w.Version

		_ = w.Credit(mustMoney(t, 100, "BRL"))

		if w.Version != before {
			t.Errorf("Version = %d, want %d (persistence owns version)", w.Version, before)
		}
	})
}

func TestWallet_Debit(t *testing.T) {
	t.Run("decreases balance", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 1000)

		if err := w.Debit(mustMoney(t, 400, "BRL")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if w.Balance.Cents() != 600 {
			t.Errorf("balance = %d, want 600", w.Balance.Cents())
		}
	})

	t.Run("exact balance results in zero", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 1000)

		if err := w.Debit(mustMoney(t, 1000, "BRL")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !w.Balance.IsZero() {
			t.Errorf("balance = %d, want 0", w.Balance.Cents())
		}
	})

	t.Run("insufficient funds keeps balance", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 500)

		err := w.Debit(mustMoney(t, 501, "BRL"))
		if err == nil {
			t.Fatal("expected insufficient funds error")
		}
		if w.Balance.Cents() != 500 {
			t.Errorf("balance = %d, want 500", w.Balance.Cents())
		}
	})

	t.Run("debit on empty wallet fails", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 0)

		if err := w.Debit(mustMoney(t, 1, "BRL")); err == nil {
			t.Fatal("expected insufficient funds error")
		}
		if !w.Balance.IsZero() {
			t.Errorf("balance = %d, want 0", w.Balance.Cents())
		}
	})

	t.Run("currency mismatch keeps balance", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 1000)

		err := w.Debit(mustMoney(t, 100, "USD"))
		if err == nil {
			t.Fatal("expected currency mismatch error")
		}
		if w.Balance.Cents() != 1000 {
			t.Errorf("balance = %d, want 1000", w.Balance.Cents())
		}
	})

	t.Run("zero Money value is rejected", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 500)

		if err := w.Debit(Money{}); err == nil {
			t.Fatal("expected error for zero-value Money")
		}
		if w.Balance.Cents() != 500 {
			t.Errorf("balance changed: %d", w.Balance.Cents())
		}
	})

	// FALHA com o código atual: Debit não rejeita valor zero.
	t.Run("zero amount is rejected", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 500)

		if err := w.Debit(mustMoney(t, 0, "BRL")); err == nil {
			t.Fatal("expected error for zero amount")
		}
	})

	t.Run("does not change version", func(t *testing.T) {
		w := newTestWallet(t, "BRL", 1000)
		before := w.Version

		_ = w.Debit(mustMoney(t, 100, "BRL"))

		if w.Version != before {
			t.Errorf("Version = %d, want %d", w.Version, before)
		}
	})
}

func TestWallet_BalanceNeverNegative(t *testing.T) {
	w := newTestWallet(t, "BRL", 1000)

	_ = w.Debit(mustMoney(t, 600, "BRL"))
	_ = w.Debit(mustMoney(t, 600, "BRL")) // deve falhar
	_ = w.Debit(mustMoney(t, 400, "BRL"))

	if w.Balance.Cents() != 0 {
		t.Errorf("balance = %d, want 0", w.Balance.Cents())
	}
}
