package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// seedProcessed armazena uma BET PROCESSED com fingerprint real como
// payload_hash e a retorna junto do hash.
func seedProcessed(t *testing.T, f *serviceFixture, wallet *domain.Wallet) (*domain.WagerTransaction, string) {
	t.Helper()

	amount, err := domain.NewMoney(3000, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), testUUID(t, f.pool), "ext-"+testUUID(t, f.pool),
		"key-"+testUUID(t, f.pool), "hash-seed",
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

	balance, err := domain.NewMoney(7000, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	if err := wt.Complete(balance); err != nil {
		t.Fatalf("complete: %v", err)
	}

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

	return wt, fp
}

func lookupByKey(t *testing.T, f *serviceFixture, providerID, key string) (*domain.WagerTransaction, error) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()

	return f.transactions.GetByIdempotencyKey(ctx, dbTx, providerID, key)
}

func countByKey(t *testing.T, f *serviceFixture, providerID, key string) int {
	t.Helper()

	var n int
	err := f.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`,
		providerID, key,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestIdempotencyPersistence_Replay(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, fp := seedProcessed(t, f, wallet)

	existing, err := lookupByKey(t, f, seeded.ProviderID, seeded.IdempotencyKey)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	res, err := application.DecideIdempotency(existing, fp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Outcome != application.IdempotencyReplay {
		t.Fatalf("Outcome = %v, want REPLAY", res.Outcome)
	}
	if res.Existing.ID != seeded.ID {
		t.Errorf("ID = %q, want %q", res.Existing.ID, seeded.ID)
	}
	if res.Existing.Status != domain.TransactionProcessed {
		t.Errorf("Status = %q, want PROCESSED", res.Existing.Status)
	}
	if res.Existing.Amount.Cents() != 3000 || res.Existing.Amount.Currency() != "BRL" {
		t.Errorf("Amount = %+v, want 3000 BRL", res.Existing.Amount)
	}
	if res.Existing.ResultingBalance == nil || res.Existing.ResultingBalance.Cents() != 7000 {
		t.Errorf("ResultingBalance = %+v, want 7000", res.Existing.ResultingBalance)
	}
	if res.Existing.PayloadHash != fp {
		t.Errorf("PayloadHash = %q, want %q", res.Existing.PayloadHash, fp)
	}

	// Replay não produz efeito financeiro: mesma única linha, wallet e
	// ledger intactos.
	if n := countByKey(t, f, seeded.ProviderID, seeded.IdempotencyKey); n != 1 {
		t.Errorf("rows = %d, want 1", n)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestIdempotencyPersistence_Conflict(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, _ := seedProcessed(t, f, wallet)

	existing, err := lookupByKey(t, f, seeded.ProviderID, seeded.IdempotencyKey)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	_, err = application.DecideIdempotency(existing, "different-hash")
	if !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
	}

	// Conflito não produz efeito financeiro.
	if n := countByKey(t, f, seeded.ProviderID, seeded.IdempotencyKey); n != 1 {
		t.Errorf("rows = %d, want 1", n)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
	stillPending := f.readTransaction(t, seeded.ProviderID, seeded.ExternalTransactionID)
	if stillPending.Status != domain.TransactionProcessed {
		t.Errorf("stored status = %q, want PROCESSED (unchanged)", stillPending.Status)
	}
}

func TestIdempotencyPersistence_IsolationIsNew(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, _ := seedProcessed(t, f, wallet)

	t.Run("different provider is new", func(t *testing.T) {
		_, err := lookupByKey(t, f, testUUID(t, f.pool), seeded.IdempotencyKey)
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
		res, err := application.DecideIdempotency(nil, "any-hash")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Outcome != application.IdempotencyNew {
			t.Errorf("Outcome = %v, want NEW", res.Outcome)
		}
	})

	t.Run("different key is new", func(t *testing.T) {
		_, err := lookupByKey(t, f, seeded.ProviderID, "other-key")
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})
}

func TestIdempotencyPersistence_PendingIsInProgress(t *testing.T) {
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

	existing, err := lookupByKey(t, f, wt.ProviderID, wt.IdempotencyKey)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	res, err := application.DecideIdempotency(existing, fp)
	if !errors.Is(err, application.ErrIdempotencyInProgress) {
		t.Fatalf("error = %v, want ErrIdempotencyInProgress", err)
	}
	if res.Outcome != application.IdempotencyInProgress {
		t.Errorf("Outcome = %v, want IN_PROGRESS", res.Outcome)
	}
}
