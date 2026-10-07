package application_test

import (
	"errors"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

func storedTransaction(status domain.WagerTransactionStatus, hash string) *domain.WagerTransaction {
	amount, err := domain.NewMoney(3000, "BRL")
	if err != nil {
		panic(err)
	}
	balance, err := domain.NewMoney(7000, "BRL")
	if err != nil {
		panic(err)
	}
	tx := &domain.WagerTransaction{
		ID:                    "tx-stored",
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-1",
		IdempotencyKey:        "key-1",
		PayloadHash:           hash,
		PlayerID:              "player-1",
		WalletID:              "wallet-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  domain.TransactionBet,
		Status:                status,
		Amount:                amount,
	}
	if status == domain.TransactionProcessed {
		tx.ResultingBalance = &balance
	}
	if status == domain.TransactionRejected || status == domain.TransactionFailed {
		tx.FailureCode = "SOME_CODE"
	}
	return tx
}

func TestDecideIdempotency(t *testing.T) {
	const hash = "abc123"

	t.Run("missing is new", func(t *testing.T) {
		res, err := application.DecideIdempotency(nil, hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Outcome != application.IdempotencyNew {
			t.Errorf("Outcome = %v, want NEW", res.Outcome)
		}
		if res.Existing != nil {
			t.Errorf("Existing = %+v, want nil", res.Existing)
		}
	})

	for _, status := range []domain.WagerTransactionStatus{
		domain.TransactionProcessed,
		domain.TransactionRejected,
		domain.TransactionFailed,
	} {
		t.Run(string(status)+" same hash is replay", func(t *testing.T) {
			existing := storedTransaction(status, hash)

			res, err := application.DecideIdempotency(existing, hash)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Outcome != application.IdempotencyReplay {
				t.Fatalf("Outcome = %v, want REPLAY", res.Outcome)
			}
			if res.Existing != existing {
				t.Error("Existing is not the stored transaction")
			}
			if res.Existing.Status != status {
				t.Errorf("Status = %q, want %q", res.Existing.Status, status)
			}
			if res.Existing.PayloadHash != hash {
				t.Errorf("PayloadHash = %q, want %q", res.Existing.PayloadHash, hash)
			}
		})
	}

	for _, status := range []domain.WagerTransactionStatus{
		domain.TransactionPending,
		domain.TransactionPendingReference,
	} {
		t.Run(string(status)+" same hash is in progress", func(t *testing.T) {
			existing := storedTransaction(status, hash)

			res, err := application.DecideIdempotency(existing, hash)
			if !errors.Is(err, application.ErrIdempotencyInProgress) {
				t.Fatalf("error = %v, want ErrIdempotencyInProgress", err)
			}
			if res.Outcome != application.IdempotencyInProgress {
				t.Errorf("Outcome = %v, want IN_PROGRESS", res.Outcome)
			}
			if res.Existing != existing {
				t.Error("Existing is not the stored transaction")
			}
		})
	}

	t.Run("different hash is conflict", func(t *testing.T) {
		for _, status := range []domain.WagerTransactionStatus{
			domain.TransactionPending,
			domain.TransactionProcessed,
		} {
			res, err := application.DecideIdempotency(storedTransaction(status, hash), "other-hash")
			if !errors.Is(err, domain.ErrIdempotencyConflict) {
				t.Fatalf("status %q: error = %v, want ErrIdempotencyConflict", status, err)
			}
			if res.Existing == nil {
				t.Errorf("status %q: Existing is nil, want stored row", status)
			}
		}
	})

	t.Run("empty stored hash never matches", func(t *testing.T) {
		existing := storedTransaction(domain.TransactionProcessed, "")

		_, err := application.DecideIdempotency(existing, hash)
		if !errors.Is(err, domain.ErrIdempotencyConflict) {
			t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
		}
	})
}
