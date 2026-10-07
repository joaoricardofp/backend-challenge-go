package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

func lookupByExternal(t *testing.T, f *serviceFixture, providerID, externalID string) (*domain.WagerTransaction, error) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()

	return f.transactions.GetByProviderExternalID(ctx, dbTx, providerID, externalID)
}

func countByExternal(t *testing.T, f *serviceFixture, providerID, externalID string) int {
	t.Helper()

	var n int
	err := f.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func assertNoFinancialEffects(t *testing.T, f *serviceFixture, wallet *domain.Wallet, providerID, externalID string, wantRows int) {
	t.Helper()

	if n := countByExternal(t, f, providerID, externalID); n != wantRows {
		t.Errorf("rows = %d, want %d", n, wantRows)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestExternalIdentityPersistence_Known(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, fp := seedProcessed(t, f, wallet)

	existing, err := lookupByExternal(t, f, seeded.ProviderID, seeded.ExternalTransactionID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	res, err := application.DecideExternalTransaction(existing, fp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Outcome != application.ExternalTransactionKnown {
		t.Fatalf("Outcome = %v, want KNOWN", res.Outcome)
	}
	if res.Existing.ID != seeded.ID {
		t.Errorf("ID = %q, want %q", res.Existing.ID, seeded.ID)
	}
	if res.Existing.Status != domain.TransactionProcessed {
		t.Errorf("Status = %q, want PROCESSED", res.Existing.Status)
	}
	if res.Existing.ResultingBalance == nil || res.Existing.ResultingBalance.Cents() != 7000 {
		t.Errorf("ResultingBalance = %+v, want 7000", res.Existing.ResultingBalance)
	}
	if res.Existing.PayloadHash != fp {
		t.Errorf("PayloadHash = %q, want %q", res.Existing.PayloadHash, fp)
	}

	assertNoFinancialEffects(t, f, wallet, seeded.ProviderID, seeded.ExternalTransactionID, 1)
}

func TestExternalIdentityPersistence_DifferentKeyIsKnown(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, fp := seedProcessed(t, f, wallet)

	// A linha foi gravada com K1; a lookup externa encontra por (P, E)
	// independentemente da chave da requisição entrante.
	existing, err := lookupByExternal(t, f, seeded.ProviderID, seeded.ExternalTransactionID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if existing.IdempotencyKey != seeded.IdempotencyKey {
		t.Fatalf("IdempotencyKey = %q, want stored %q", existing.IdempotencyKey, seeded.IdempotencyKey)
	}

	res, err := application.DecideExternalTransaction(existing, fp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Outcome != application.ExternalTransactionKnown {
		t.Errorf("Outcome = %v, want KNOWN (not NEW)", res.Outcome)
	}

	assertNoFinancialEffects(t, f, wallet, seeded.ProviderID, seeded.ExternalTransactionID, 1)
}

func TestExternalIdentityPersistence_Conflict(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, _ := seedProcessed(t, f, wallet)

	existing, err := lookupByExternal(t, f, seeded.ProviderID, seeded.ExternalTransactionID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	_, err = application.DecideExternalTransaction(existing, "different-hash")
	if !errors.Is(err, domain.ErrExternalTransactionConflict) {
		t.Fatalf("error = %v, want ErrExternalTransactionConflict", err)
	}

	assertNoFinancialEffects(t, f, wallet, seeded.ProviderID, seeded.ExternalTransactionID, 1)
	stillStored := f.readTransaction(t, seeded.ProviderID, seeded.ExternalTransactionID)
	if stillStored.Status != domain.TransactionProcessed {
		t.Errorf("stored status = %q, want PROCESSED (unchanged)", stillStored.Status)
	}
}

func TestExternalIdentityPersistence_Isolation(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, _ := seedProcessed(t, f, wallet)

	t.Run("different provider is new", func(t *testing.T) {
		_, err := lookupByExternal(t, f, testUUID(t, f.pool), seeded.ExternalTransactionID)
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
		res, err := application.DecideExternalTransaction(nil, "any-hash")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Outcome != application.ExternalTransactionNew {
			t.Errorf("Outcome = %v, want NEW", res.Outcome)
		}
	})

	t.Run("different external id is new", func(t *testing.T) {
		_, err := lookupByExternal(t, f, seeded.ProviderID, "other-ext")
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})
}

func TestExternalIdentityPersistence_PendingIsKnown(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	amount, err := domain.NewMoney(3000, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), testUUID(t, f.pool), "ext-"+testUUID(t, f.pool),
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, amount, "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	fp, err := domain.CanonicalWagerFingerprint(*wt)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	wt.PayloadHash = fp

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := f.transactions.Create(ctx, dbTx, wt); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("create: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Status não altera a identidade: PENDING + mesmo hash é KNOWN
	// (o significado de continuação será decidido em B3).
	existing, err := lookupByExternal(t, f, wt.ProviderID, wt.ExternalTransactionID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	res, err := application.DecideExternalTransaction(existing, fp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Outcome != application.ExternalTransactionKnown {
		t.Errorf("Outcome = %v, want KNOWN", res.Outcome)
	}
}
