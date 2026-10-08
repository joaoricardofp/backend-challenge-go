package domain

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

type Money struct {
	cents    int64
	currency string
}

var (
	ErrNegativeAmount      = errors.New("amount cannot be negative")
	ErrInvalidCurrency     = errors.New("invalid currency")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrOverflow            = errors.New("amount overflow")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrInvalidAmount       = errors.New("amount must be greater than zero")
	ErrInvalidAmountFormat = errors.New("invalid amount format")
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

// NewMoneyFromDecimal cria Money a partir do formato decimal externo oficial
// (por exemplo "25.00"), exigindo exatamente duas casas decimais.
// Valores vazios, negativos, com escala diferente de duas casas, notação
// científica, NaN/Infinity e qualquer forma não decimal são rejeitados sem
// arredondamento silencioso. A conversão é feita dígito a dígito, sem
// float32/float64 em nenhum ponto.
func NewMoneyFromDecimal(amount string, currency string) (Money, error) {
	if amount == "" {
		return Money{}, ErrInvalidAmountFormat
	}
	if amount[0] == '-' {
		return Money{}, ErrNegativeAmount
	}

	intPart, fracPart, ok := strings.Cut(amount, ".")
	if !ok {
		return Money{}, ErrInvalidAmountFormat
	}
	if len(fracPart) != 2 {
		return Money{}, ErrInvalidAmountFormat
	}
	if !isDigits(intPart) || !isDigits(fracPart) {
		return Money{}, ErrInvalidAmountFormat
	}

	var units int64
	for i := 0; i < len(intPart); i++ {
		d := int64(intPart[i] - '0')
		if units > (math.MaxInt64-d)/10 {
			return Money{}, ErrOverflow
		}
		units = units*10 + d
	}

	frac := int64(fracPart[0]-'0')*10 + int64(fracPart[1]-'0')
	if units > (math.MaxInt64-frac)/100 {
		return Money{}, ErrOverflow
	}

	return NewMoney(units*100+frac, currency)
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
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

// AmountString serializa o valor no formato decimal externo oficial,
// sempre com exatamente duas casas (por exemplo "25.00").
func (m Money) AmountString() string {
	units := m.cents / 100
	frac := m.cents % 100

	var b strings.Builder
	b.WriteString(strconv.FormatInt(units, 10))
	b.WriteByte('.')
	if frac < 10 {
		b.WriteByte('0')
	}
	b.WriteString(strconv.FormatInt(frac, 10))
	return b.String()
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

// Negate retorna o valor com sinal trocado, para diferenças e cálculos
// internos (README §6.1). O resultado pode ser negativo e, portanto, não é
// válido como saldo de carteira nem como amount de movimentação.
func (m Money) Negate() (Money, error) {
	if m.cents == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{cents: -m.cents, currency: m.currency}, nil
}
