package application_test

import (
	"errors"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

func storedExternalTransaction(status domain.WagerTransactionStatus, hash string) *domain.WagerTransaction {
	amount, err := domain.NewMoney(3000, "BRL")
	if err != nil {
		panic(err)
	}
	return &domain.WagerTransaction{
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
}

func TestDecideExternalTransaction(t *testing.T) {
	const hash = "abc123"

	t.Run("missing is new", func(t *testing.T) {
		res, err := application.DecideExternalTransaction(nil, hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Outcome != application.ExternalTransactionNew {
			t.Errorf("Outcome = %v, want NEW", res.Outcome)
		}
		if res.Existing != nil {
			t.Errorf("Existing = %+v, want nil", res.Existing)
		}
	})

	for _, status := range []domain.WagerTransactionStatus{
		domain.TransactionPending,
		domain.TransactionPendingReference,
		domain.TransactionProcessed,
		domain.TransactionRejected,
		domain.TransactionFailed,
	} {
		t.Run(string(status)+" same hash is known", func(t *testing.T) {
			existing := storedExternalTransaction(status, hash)

			res, err := application.DecideExternalTransaction(existing, hash)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Outcome != application.ExternalTransactionKnown {
				t.Fatalf("Outcome = %v, want KNOWN", res.Outcome)
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
			res, err := application.DecideExternalTransaction(storedExternalTransaction(status, hash), "other-hash")
			if !errors.Is(err, domain.ErrExternalTransactionConflict) {
				t.Fatalf("status %q: error = %v, want ErrExternalTransactionConflict", status, err)
			}
			if res.Existing == nil {
				t.Errorf("status %q: Existing is nil, want stored row", status)
			}
		}
	})

	t.Run("different idempotency key same external is known", func(t *testing.T) {
		// A identidade externa independe da chave: a linha foi gravada com
		// K1 e a requisição entrante usa K2, mas (P, E, H) são iguais.
		existing := storedExternalTransaction(domain.TransactionProcessed, hash)
		existing.IdempotencyKey = "key-1"

		res, err := application.DecideExternalTransaction(existing, hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Outcome != application.ExternalTransactionKnown {
			t.Errorf("Outcome = %v, want KNOWN (not NEW)", res.Outcome)
		}
	})

	t.Run("different idempotency key different hash is external conflict", func(t *testing.T) {
		existing := storedExternalTransaction(domain.TransactionProcessed, hash)

		_, err := application.DecideExternalTransaction(existing, "other-hash")
		if !errors.Is(err, domain.ErrExternalTransactionConflict) {
			t.Fatalf("error = %v, want ErrExternalTransactionConflict (not ErrIdempotencyConflict)", err)
		}
	})

	t.Run("empty stored hash never matches", func(t *testing.T) {
		existing := storedExternalTransaction(domain.TransactionProcessed, "")

		_, err := application.DecideExternalTransaction(existing, hash)
		if !errors.Is(err, domain.ErrExternalTransactionConflict) {
			t.Fatalf("error = %v, want ErrExternalTransactionConflict", err)
		}
	})
}

func TestExternalFingerprintDependsOnExternalID(t *testing.T) {
	// externalTransactionId faz parte do fingerprint (B2.1): E1 e E2 com
	// todo o resto igual geram hashes diferentes.
	amount, err := domain.NewMoney(3000, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	build := func(external string) domain.WagerTransaction {
		return domain.WagerTransaction{
			ProviderID:            "provider-a",
			ExternalTransactionID: external,
			PlayerID:              "player-1",
			WalletID:              "wallet-1",
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.TransactionBet,
			Status:                domain.TransactionPending,
			Amount:                amount,
		}
	}

	fp1, err := domain.CanonicalWagerFingerprint(build("ext-1"))
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	fp2, err := domain.CanonicalWagerFingerprint(build("ext-2"))
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if fp1 == fp2 {
		t.Error("different external IDs produced the same fingerprint")
	}
}
