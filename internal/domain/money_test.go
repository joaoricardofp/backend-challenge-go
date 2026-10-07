package domain

import (
	"errors"
	"math"
	"testing"
)

func mustMoney(t *testing.T, cents int64, currency string) Money {
	t.Helper()
	m, err := NewMoney(cents, currency)

	if err != nil {
		t.Fatal(err)
	}

	return m
}

func TestNewMoney(t *testing.T) {
	tests := []struct {
		name         string
		cents        int64
		currency     string
		wantErr      error
		wantCurrency string
	}{
		{name: "valid", cents: 1000, currency: "BRL", wantCurrency: "BRL"},
		{name: "negative amount", cents: -1, currency: "BRL", wantErr: ErrNegativeAmount},
		{name: "empty currency", cents: 100, currency: "", wantErr: ErrInvalidCurrency},
		{name: "currency too short", cents: 100, currency: "BR", wantErr: ErrInvalidCurrency},
		{name: "currency too long", cents: 100, currency: "BRLL", wantErr: ErrInvalidCurrency},
		{name: "digits are not a currency", cents: 100, currency: "123", wantErr: ErrInvalidCurrency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMoney(tt.cents, tt.currency)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				if got != (Money{}) {
					t.Errorf("expected zero Money on error, got %+v", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Cents() != tt.cents {
				t.Errorf("Cents() = %d, want %d", got.Cents(), tt.cents)
			}
			if got.Currency() != tt.wantCurrency {
				t.Errorf("currency = %s, want %s", got.Currency(), tt.wantCurrency)
			}
		})
	}
}

func TestMoney_IsZero(t *testing.T) {
	if !mustMoney(t, 0, "BRL").IsZero() {
		t.Error("expected zero money to be zero")
	}
	if mustMoney(t, 1, "BRL").IsZero() {
		t.Error("expected 1 cent not to be zero")
	}
}

func TestMoney_Add(t *testing.T) {
	t.Run("same currency", func(t *testing.T) {
		a := mustMoney(t, 700, "BRL")
		b := mustMoney(t, 300, "BRL")

		got, err := a.Add(b)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Cents() != 1000 || got.Currency() != "BRL" {
			t.Errorf("got %d %s, want 1000 BRL", got.Cents(), got.Currency())
		}
	})

	t.Run("does not mutate operands", func(t *testing.T) {
		a := mustMoney(t, 700, "BRL")
		b := mustMoney(t, 300, "BRL")

		if _, err := a.Add(b); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if a.Cents() != 700 || b.Cents() != 300 {
			t.Errorf("operands changed: a=%d b=%d", a.Cents(), b.Cents())
		}
	})

	t.Run("adding zero", func(t *testing.T) {
		a := mustMoney(t, 500, "BRL")
		got, err := a.Add(mustMoney(t, 0, "BRL"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != a {
			t.Errorf("got %+v, want %+v", got, a)
		}
	})

	t.Run("currency mismatch", func(t *testing.T) {
		_, err := mustMoney(t, 100, "BRL").Add(mustMoney(t, 100, "USD"))
		if !errors.Is(err, ErrCurrencyMismatch) {
			t.Fatalf("expected ErrCurrencyMismatch, got %v", err)
		}
	})

	t.Run("overflow", func(t *testing.T) {
		_, err := mustMoney(t, math.MaxInt64, "BRL").Add(mustMoney(t, 1, "BRL"))
		if err == nil {
			t.Fatal("expected overflow error")
		}
	})

	t.Run("exactly max int64 does not overflow", func(t *testing.T) {
		got, err := mustMoney(t, math.MaxInt64-1, "BRL").Add(mustMoney(t, 1, "BRL"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Cents() != math.MaxInt64 {
			t.Errorf("got %d, want %d", got.Cents(), int64(math.MaxInt64))
		}
	})
}

func TestMoney_Sub(t *testing.T) {
	t.Run("same currency", func(t *testing.T) {
		got, err := mustMoney(t, 1000, "BRL").Sub(mustMoney(t, 400, "BRL"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Cents() != 600 || got.Currency() != "BRL" {
			t.Errorf("got %d %s, want 600 BRL", got.Cents(), got.Currency())
		}
	})

	t.Run("exact balance results in zero", func(t *testing.T) {
		got, err := mustMoney(t, 1000, "BRL").Sub(mustMoney(t, 1000, "BRL"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.IsZero() {
			t.Errorf("expected zero, got %d", got.Cents())
		}
	})

	t.Run("insufficient funds", func(t *testing.T) {
		got, err := mustMoney(t, 500, "BRL").Sub(mustMoney(t, 501, "BRL"))
		if err == nil {
			t.Fatal("expected insufficient funds error")
		}
		if got != (Money{}) {
			t.Errorf("expected zero Money on error, got %+v", got)
		}
	})

	t.Run("currency mismatch", func(t *testing.T) {
		_, err := mustMoney(t, 1000, "BRL").Sub(mustMoney(t, 100, "USD"))
		if err == nil {
			t.Fatal("expected currency mismatch error")
		}
	})

	t.Run("does not mutate operands", func(t *testing.T) {
		a := mustMoney(t, 1000, "BRL")
		b := mustMoney(t, 400, "BRL")

		if _, err := a.Sub(b); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if a.Cents() != 1000 || b.Cents() != 400 {
			t.Errorf("operands changed: a=%d b=%d", a.Cents(), b.Cents())
		}
	})
}

func TestMoney_LessThan(t *testing.T) {
	small := mustMoney(t, 100, "BRL")
	big := mustMoney(t, 200, "BRL")

	if !small.LessThan(big) {
		t.Error("expected 100 < 200")
	}
	if big.LessThan(small) {
		t.Error("expected 200 not < 100")
	}
	if small.LessThan(small) {
		t.Error("expected equal values not to be less than")
	}
}

func TestMoney_Equality(t *testing.T) {
	// Money é comparável com ==; a normalização garante que "brl" == "BRL".
	a := mustMoney(t, 100, "brl")
	b := mustMoney(t, 100, "BRL")

	if a != b {
		t.Errorf("expected %+v == %+v", a, b)
	}
}

func TestNewMoneyFromDecimal(t *testing.T) {
	tests := []struct {
		name         string
		amount       string
		currency     string
		wantCents    int64
		wantCurrency string
		wantErr      error
	}{
		{name: "zero", amount: "0.00", currency: "BRL", wantCents: 0, wantCurrency: "BRL"},
		{name: "positive", amount: "25.00", currency: "BRL", wantCents: 2500, wantCurrency: "BRL"},
		{name: "with cents", amount: "123456.78", currency: "BRL", wantCents: 12345678, wantCurrency: "BRL"},
		{name: "one cent", amount: "0.01", currency: "BRL", wantCents: 1, wantCurrency: "BRL"},
		{name: "lowercase currency is normalized", amount: "25.00", currency: "brl", wantCents: 2500, wantCurrency: "BRL"},

		{name: "empty string", amount: "", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "negative", amount: "-25.00", currency: "BRL", wantErr: ErrNegativeAmount},
		{name: "negative zero", amount: "-0.00", currency: "BRL", wantErr: ErrNegativeAmount},
		{name: "scale greater than two", amount: "25.000", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "scale less than two", amount: "25.0", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "no decimal point", amount: "25", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "scientific notation", amount: "1e2", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "uppercase scientific notation", amount: "1E2", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "nan", amount: "NaN", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "infinity", amount: "Infinity", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "non numeric string", amount: "abc", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "multiple points", amount: "1.2.3", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "leading whitespace", amount: " 25.00", currency: "BRL", wantErr: ErrInvalidAmountFormat},
		{name: "overflow units", amount: "99999999999999999999.00", currency: "BRL", wantErr: ErrOverflow},
		{name: "overflow cents", amount: "92233720368547758.08", currency: "BRL", wantErr: ErrOverflow},
		{name: "invalid currency", amount: "25.00", currency: "BR", wantErr: ErrInvalidCurrency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMoneyFromDecimal(tt.amount, tt.currency)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				if got != (Money{}) {
					t.Errorf("expected zero Money on error, got %+v", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Cents() != tt.wantCents {
				t.Errorf("Cents() = %d, want %d", got.Cents(), tt.wantCents)
			}
			if got.Currency() != tt.wantCurrency {
				t.Errorf("Currency() = %s, want %s", got.Currency(), tt.wantCurrency)
			}
		})
	}
}

func TestMoney_AmountString(t *testing.T) {
	tests := []struct {
		cents int64
		want  string
	}{
		{cents: 0, want: "0.00"},
		{cents: 1, want: "0.01"},
		{cents: 2500, want: "25.00"},
		{cents: 123456, want: "1234.56"},
		{cents: 12345678, want: "123456.78"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			m := mustMoney(t, tt.cents, "BRL")
			if got := m.AmountString(); got != tt.want {
				t.Errorf("AmountString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMoney_DecimalRoundTrip(t *testing.T) {
	values := []string{
		"0.00",
		"0.01",
		"25.00",
		"1234.56",
		"123456.78",
	}

	for _, v := range values {
		t.Run(v, func(t *testing.T) {
			m, err := NewMoneyFromDecimal(v, "BRL")
			if err != nil {
				t.Fatalf("NewMoneyFromDecimal: unexpected error: %v", err)
			}
			if got := m.AmountString(); got != v {
				t.Errorf("round-trip = %q, want %q", got, v)
			}
		})
	}
}
