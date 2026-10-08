package sqs

import (
	"errors"
	"strings"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

func validRaw() string {
	return `{"messageId":"msg-123","type":"WagerTransactionRequested",` +
		`"occurredAt":"2026-09-08T12:00:00.000Z",` +
		`"data":{"providerId":"provider-a","externalTransactionId":"transaction-123",` +
		`"idempotencyKey":"provider-a:transaction-123",` +
		`"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",` +
		`"walletId":"0192f291-27dd-7d3f-8071-5f8685deef37",` +
		`"roundId":"round-987","gameId":"fortune-chimp","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"}}}`
}

func TestParseWagerMessage_Valid(t *testing.T) {
	msg, err := ParseWagerMessage([]byte(validRaw()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.MessageID != "msg-123" || msg.Type != WagerTransactionRequested {
		t.Errorf("envelope = %+v", msg)
	}
	if msg.Data.Kind != "BET" || msg.Data.Money.Amount != "25.00" {
		t.Errorf("data = %+v", msg.Data)
	}
}

func TestParseWagerMessage_Envelope(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(string) string
		wantErr error
	}{
		{"invalid json", func(s string) string { return s[:len(s)-5] }, ErrInvalidMessage},
		{"unknown field", func(s string) string {
			return strings.Replace(s, `"kind":"BET"`, `"kind":"BET","traceId":"x"`, 1)
		}, ErrInvalidMessage},
		{"missing messageId", func(s string) string {
			return strings.Replace(s, `"messageId":"msg-123",`, ``, 1)
		}, ErrInvalidMessage},
		{"wrong type", func(s string) string {
			return strings.Replace(s, `"type":"WagerTransactionRequested"`, `"type":"Other"`, 1)
		}, ErrInvalidMessage},
		{"bad occurredAt", func(s string) string {
			return strings.Replace(s, `"2026-09-08T12:00:00.000Z"`, `"yesterday"`, 1)
		}, ErrInvalidMessage},
		{"missing provider", func(s string) string {
			return strings.Replace(s, `"providerId":"provider-a",`, ``, 1)
		}, ErrInvalidMessage},
		{"missing idempotencyKey", func(s string) string {
			return strings.Replace(s, `"idempotencyKey":"provider-a:transaction-123",`, ``, 1)
		}, ErrInvalidMessage},
		{"unknown kind", func(s string) string {
			return strings.Replace(s, `"kind":"BET"`, `"kind":"DEBIT"`, 1)
		}, ErrInvalidMessage},
		{"opening rejected", func(s string) string {
			return strings.Replace(s, `"kind":"BET"`, `"kind":"OPENING"`, 1)
		}, ErrOpeningNotAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseWagerMessage([]byte(tc.mutate(validRaw())))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestParseWagerMessage_Money(t *testing.T) {
	for _, amount := range []string{`"25"`, `"25.0"`, `"25.000"`, `"1e2"`, `"-25.00"`, `"NaN"`, `"Infinity"`, `""`, `25.00`} {
		t.Run("amount "+amount, func(t *testing.T) {
			raw := strings.Replace(validRaw(), `"amount":"25.00"`, `"amount":`+amount, 1)
			_, err := ParseWagerMessage([]byte(raw))
			if !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("error = %v, want ErrInvalidMessage", err)
			}
		})
	}

	t.Run("invalid currency", func(t *testing.T) {
		raw := strings.Replace(validRaw(), `"currency":"BRL"`, `"currency":"BR"`, 1)
		_, err := ParseWagerMessage([]byte(raw))
		if !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("error = %v, want ErrInvalidMessage", err)
		}
	})
}

func TestParseWagerMessage_Kinds(t *testing.T) {
	for _, kind := range []string{"BET", "WIN", "LOSS", "REFUND", "ROLLBACK"} {
		t.Run(kind, func(t *testing.T) {
			raw := validRaw()
			if kind == "LOSS" {
				raw = strings.Replace(raw, `"amount":"25.00"`, `"amount":"0.00"`, 1)
			}
			raw = strings.Replace(raw, `"kind":"BET"`, `"kind":"`+kind+`"`, 1)
			msg, err := ParseWagerMessage([]byte(raw))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if msg.Data.Kind != kind {
				t.Errorf("kind = %q", msg.Data.Kind)
			}
		})
	}
}

func TestWagerMessage_ToTransaction(t *testing.T) {
	msg, err := ParseWagerMessage([]byte(validRaw()))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	tx, err := msg.ToTransaction("internal-id-1")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if tx.ID != "internal-id-1" {
		t.Errorf("ID = %q", tx.ID)
	}
	if tx.ProviderID != "provider-a" || tx.ExternalTransactionID != "transaction-123" {
		t.Errorf("identity = %q/%q", tx.ProviderID, tx.ExternalTransactionID)
	}
	if tx.IdempotencyKey != "provider-a:transaction-123" {
		t.Errorf("key = %q", tx.IdempotencyKey)
	}
	if tx.Kind != domain.TransactionBet {
		t.Errorf("kind = %q", tx.Kind)
	}
	if tx.Amount.Cents() != 2500 || tx.Amount.Currency() != "BRL" {
		t.Errorf("amount = %+v", tx.Amount)
	}
	if !tx.IsPending() {
		t.Errorf("status = %q, want PENDING", tx.Status)
	}
}

func TestWagerMessage_ToTransactionOpeningRejected(t *testing.T) {
	msg := WagerMessage{
		MessageID:  "m",
		Type:       WagerTransactionRequested,
		OccurredAt: "2026-09-08T12:00:00.000Z",
		Data:       WagerData{Kind: "OPENING"},
	}
	_, err := msg.ToTransaction("id")
	if !errors.Is(err, ErrOpeningNotAllowed) {
		t.Fatalf("error = %v, want ErrOpeningNotAllowed", err)
	}
}

func TestWagerMessage_ToTransactionReference(t *testing.T) {
	raw := strings.Replace(validRaw(), `"kind":"BET"`, `"kind":"REFUND","referenceExternalTransactionId":"transaction-1"`, 1)
	msg, err := ParseWagerMessage([]byte(raw))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	tx, err := msg.ToTransaction("internal-id-2")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if tx.Kind != domain.TransactionRefund {
		t.Errorf("kind = %q", tx.Kind)
	}
	if tx.ReferenceExternalTransactionID != "transaction-1" {
		t.Errorf("reference = %q", tx.ReferenceExternalTransactionID)
	}
}

func TestMessageHash(t *testing.T) {
	a := MessageHash([]byte(validRaw()))
	b := MessageHash([]byte(validRaw()))
	if a != b || len(a) != 64 {
		t.Fatalf("hash not deterministic hex64: %q", a)
	}
	if MessageHash([]byte(validRaw()+" ")) == a {
		t.Error("different bytes produced same hash")
	}
}
