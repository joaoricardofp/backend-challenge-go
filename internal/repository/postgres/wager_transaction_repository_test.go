package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// --- helpers ---

// newTransaction creates a domain WagerTransaction (not yet persisted) and
// registers a cleanup to delete it from the database.
func newTransaction(
	t *testing.T,
	pool *pgxpool.Pool,
	providerID string,
	externalTxID string,
	idempotencyKey string,
	walletID string,
	kind domain.WagerTransactionKind,
	amountCents int64,
	reference string,
) *domain.WagerTransaction {
	t.Helper()

	amount, err := domain.NewMoney(amountCents, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}

	txID := newUUID(t, pool)

	wt, err := domain.NewWagerTransaction(
		txID,
		providerID,
		externalTxID,
		idempotencyKey,
		"sha256-test-hash",
		newUUID(t, pool), // playerID
		walletID,
		"round-1",
		"game-1",
		kind,
		amount,
		reference,
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(),
			"DELETE FROM wager_transactions WHERE id = $1", wt.ID)
		if err != nil {
			t.Errorf("cleanup transaction %s: %v", wt.ID, err)
		}
	})

	return wt
}

// mustCreateTransaction persists a transaction inside a committed tx and
// returns it. Requires a pre-existing wallet.
func mustCreateTransaction(
	t *testing.T,
	pool *pgxpool.Pool,
	repo *postgres.WagerTransactionRepository,
	wt *domain.WagerTransaction,
) {
	t.Helper()

	tx := beginTx(t, pool)
	if err := repo.Create(context.Background(), tx, wt); err != nil {
		t.Fatalf("create wager transaction: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// --- tests ---

func TestWagerTransactionRepository_CreateAndGetByProviderExternalID(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	// Need a wallet to satisfy the schema (wallet_id is present in the table).
	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-tx-" + newUUID(t, pool)

	wt := newTransaction(t, pool, providerID, extTxID, "idem-"+extTxID, wallet.ID, domain.TransactionBet, 5000, "")
	mustCreateTransaction(t, pool, repo, wt)

	// Retrieve by provider + external ID
	tx := beginTx(t, pool)
	got, err := repo.GetByProviderExternalID(ctx, tx, providerID, extTxID)
	if err != nil {
		t.Fatalf("GetByProviderExternalID: %v", err)
	}

	if got.ID != wt.ID {
		t.Errorf("ID = %q, want %q", got.ID, wt.ID)
	}
	if got.ProviderID != providerID {
		t.Errorf("ProviderID = %q, want %q", got.ProviderID, providerID)
	}
	if got.ExternalTransactionID != extTxID {
		t.Errorf("ExternalTransactionID = %q, want %q", got.ExternalTransactionID, extTxID)
	}
	if got.Kind != domain.TransactionBet {
		t.Errorf("Kind = %q, want %q", got.Kind, domain.TransactionBet)
	}
	if got.Status != domain.TransactionPending {
		t.Errorf("Status = %q, want %q", got.Status, domain.TransactionPending)
	}
	if got.Amount.Cents() != 5000 {
		t.Errorf("Amount = %d, want 5000", got.Amount.Cents())
	}
	if got.Amount.Currency() != "BRL" {
		t.Errorf("Currency = %q, want BRL", got.Amount.Currency())
	}
}

func TestWagerTransactionRepository_GetByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	idemKey := "idem-" + newUUID(t, pool)

	wt := newTransaction(t, pool, providerID, "ext-"+idemKey, idemKey, wallet.ID, domain.TransactionWin, 3000, "")
	mustCreateTransaction(t, pool, repo, wt)

	tx := beginTx(t, pool)
	got, err := repo.GetByIdempotencyKey(ctx, tx, providerID, idemKey)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}

	if got.ID != wt.ID {
		t.Errorf("ID = %q, want %q", got.ID, wt.ID)
	}
	if got.IdempotencyKey != idemKey {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, idemKey)
	}
	if got.Kind != domain.TransactionWin {
		t.Errorf("Kind = %q, want %q", got.Kind, domain.TransactionWin)
	}
}

func TestWagerTransactionRepository_GetByIdempotencyKey_PayloadHash(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	idemKey := "idem-" + newUUID(t, pool)

	wt := newTransaction(t, pool, providerID, "ext-"+idemKey, idemKey, wallet.ID, domain.TransactionBet, 5000, "")

	fp, err := domain.CanonicalWagerFingerprint(*wt)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	wt.PayloadHash = fp
	mustCreateTransaction(t, pool, repo, wt)

	tx := beginTx(t, pool)
	got, err := repo.GetByIdempotencyKey(ctx, tx, providerID, idemKey)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if got.PayloadHash != fp {
		t.Errorf("PayloadHash = %q, want fingerprint %q", got.PayloadHash, fp)
	}

	t.Run("different provider not found", func(t *testing.T) {
		_, err := repo.GetByIdempotencyKey(ctx, tx, newUUID(t, pool), idemKey)
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})

	t.Run("different key not found", func(t *testing.T) {
		_, err := repo.GetByIdempotencyKey(ctx, tx, providerID, "other-"+idemKey)
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})
}

func TestWagerTransactionRepository_GetByProviderExternalID_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWagerTransactionRepository(pool)

	tx := beginTx(t, pool)
	_, err := repo.GetByProviderExternalID(ctx, tx, newUUID(t, pool), "nonexistent")
	if !errors.Is(err, domain.ErrTransactionNotFound) {
		t.Fatalf("error = %v, want ErrTransactionNotFound", err)
	}
}

func TestWagerTransactionRepository_GetByIdempotencyKey_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWagerTransactionRepository(pool)

	tx := beginTx(t, pool)
	_, err := repo.GetByIdempotencyKey(ctx, tx, newUUID(t, pool), "nonexistent")
	if !errors.Is(err, domain.ErrTransactionNotFound) {
		t.Fatalf("error = %v, want ErrTransactionNotFound", err)
	}
}

func TestWagerTransactionRepository_Create_DuplicateExternalID(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-dup-" + newUUID(t, pool)

	// First transaction
	wt1 := newTransaction(t, pool, providerID, extTxID, "idem-1-"+newUUID(t, pool), wallet.ID, domain.TransactionBet, 1000, "")
	mustCreateTransaction(t, pool, repo, wt1)

	// Second transaction with same provider_id + external_transaction_id but different idempotency_key
	wt2 := newTransaction(t, pool, providerID, extTxID, "idem-2-"+newUUID(t, pool), wallet.ID, domain.TransactionBet, 2000, "")

	tx := beginTx(t, pool)
	err := repo.Create(ctx, tx, wt2)
	if !errors.Is(err, domain.ErrDuplicateExternalID) {
		t.Fatalf("error = %v, want ErrDuplicateExternalID", err)
	}
}

func TestWagerTransactionRepository_Create_DuplicateIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	idemKey := "idem-dup-" + newUUID(t, pool)

	// First transaction
	wt1 := newTransaction(t, pool, providerID, "ext-1-"+newUUID(t, pool), idemKey, wallet.ID, domain.TransactionWin, 1000, "")
	mustCreateTransaction(t, pool, repo, wt1)

	// Second transaction with same provider_id + idempotency_key but different external_transaction_id
	wt2 := newTransaction(t, pool, providerID, "ext-2-"+newUUID(t, pool), idemKey, wallet.ID, domain.TransactionWin, 2000, "")

	tx := beginTx(t, pool)
	err := repo.Create(ctx, tx, wt2)
	if !errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
		t.Fatalf("error = %v, want ErrDuplicateIdempotencyKey", err)
	}
}

func TestWagerTransactionRepository_UpdateStatus_Complete(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-upd-" + newUUID(t, pool)

	wt := newTransaction(t, pool, providerID, extTxID, "idem-"+extTxID, wallet.ID, domain.TransactionBet, 4000, "")
	mustCreateTransaction(t, pool, repo, wt)

	// Mark as processed
	balance, err := domain.NewMoney(8345, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	if err := wt.Complete(balance); err != nil {
		t.Fatalf("complete: %v", err)
	}

	tx := beginTx(t, pool)
	if err := repo.UpdateStatus(ctx, tx, wt); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Re-read and verify
	tx2 := beginTx(t, pool)
	got, err := repo.GetByProviderExternalID(ctx, tx2, providerID, extTxID)
	if err != nil {
		t.Fatalf("GetByProviderExternalID: %v", err)
	}

	if got.Status != domain.TransactionProcessed {
		t.Errorf("Status = %q, want PROCESSED", got.Status)
	}
	if got.ResultingBalance == nil {
		t.Fatal("ResultingBalance is nil after update")
	}
	if got.ResultingBalance.Cents() != 8345 {
		t.Errorf("ResultingBalance = %d, want 8345", got.ResultingBalance.Cents())
	}
	if got.FailureCode != "" {
		t.Errorf("FailureCode = %q, want empty", got.FailureCode)
	}
}

func TestWagerTransactionRepository_UpdateStatus_Fail(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-fail-" + newUUID(t, pool)

	wt := newTransaction(t, pool, providerID, extTxID, "idem-"+extTxID, wallet.ID, domain.TransactionBet, 99999, "")
	mustCreateTransaction(t, pool, repo, wt)

	// Mark as failed
	if err := wt.Fail("INSUFFICIENT_FUNDS"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	tx := beginTx(t, pool)
	if err := repo.UpdateStatus(ctx, tx, wt); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Re-read and verify
	tx2 := beginTx(t, pool)
	got, err := repo.GetByProviderExternalID(ctx, tx2, providerID, extTxID)
	if err != nil {
		t.Fatalf("GetByProviderExternalID: %v", err)
	}

	if got.Status != domain.TransactionFailed {
		t.Errorf("Status = %q, want FAILED", got.Status)
	}
	if got.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Errorf("FailureCode = %q, want INSUFFICIENT_FUNDS", got.FailureCode)
	}
	if got.ResultingBalance != nil {
		t.Errorf("ResultingBalance = %+v, want nil", got.ResultingBalance)
	}
}

func TestWagerTransactionRepository_UpdateStatus_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWagerTransactionRepository(pool)

	amount, _ := domain.NewMoney(1000, "BRL")
	phantom := &domain.WagerTransaction{
		ID:     newUUID(t, pool),
		Status: domain.TransactionProcessed,
		Amount: amount,
	}

	tx := beginTx(t, pool)
	err := repo.UpdateStatus(ctx, tx, phantom)
	if !errors.Is(err, domain.ErrTransactionNotFound) {
		t.Fatalf("error = %v, want ErrTransactionNotFound", err)
	}
}

func TestWagerTransactionRepository_RoundTrip_WithReferenceTransaction(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)

	// Create original bet
	extTxID := "ext-orig-" + newUUID(t, pool)
	original := newTransaction(t, pool, providerID, extTxID, "idem-"+extTxID, wallet.ID, domain.TransactionBet, 3000, "")
	mustCreateTransaction(t, pool, repo, original)

	// Create refund referencing the original
	refExtTxID := "ext-ref-" + newUUID(t, pool)
	refund := newTransaction(t, pool, providerID, refExtTxID, "idem-"+refExtTxID, wallet.ID, domain.TransactionRefund, 3000, extTxID)

	mustCreateTransaction(t, pool, repo, refund)

	tx := beginTx(t, pool)
	got, err := repo.GetByProviderExternalID(ctx, tx, providerID, refExtTxID)
	if err != nil {
		t.Fatalf("GetByProviderExternalID: %v", err)
	}

	if got.ReferenceExternalTransactionID != extTxID {
		t.Errorf("ReferenceExternalTransactionID = %q, want %q", got.ReferenceExternalTransactionID, extTxID)
	}
	if got.Kind != domain.TransactionRefund {
		t.Errorf("Kind = %q, want REFUND", got.Kind)
	}
}

func TestWagerTransactionRepository_GetByProviderExternalIDForUpdate(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-lock-" + newUUID(t, pool)

	wt := newTransaction(t, pool, providerID, extTxID, "idem-"+extTxID, wallet.ID, domain.TransactionBet, 3000, "")
	mustCreateTransaction(t, pool, repo, wt)

	tx := beginTx(t, pool)
	got, err := repo.GetByProviderExternalIDForUpdate(ctx, tx, providerID, extTxID)
	if err != nil {
		t.Fatalf("GetByProviderExternalIDForUpdate: %v", err)
	}
	if got.ID != wt.ID {
		t.Errorf("ID = %q, want %q", got.ID, wt.ID)
	}
	if got.Kind != domain.TransactionBet {
		t.Errorf("Kind = %q, want BET", got.Kind)
	}

	t.Run("missing returns not found", func(t *testing.T) {
		_, err := repo.GetByProviderExternalIDForUpdate(ctx, tx, providerID, "nonexistent")
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})
}

func TestWagerTransactionRepository_FindProcessedReversal(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-orig-" + newUUID(t, pool)

	original := newTransaction(t, pool, providerID, extTxID, "idem-"+extTxID, wallet.ID, domain.TransactionBet, 3000, "")
	mustCreateTransaction(t, pool, repo, original)

	t.Run("missing returns not found", func(t *testing.T) {
		tx := beginTx(t, pool)
		_, err := repo.FindProcessedReversal(ctx, tx, providerID, extTxID)
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})

	t.Run("pending reversal does not count", func(t *testing.T) {
		refExtTxID := "ext-pending-" + newUUID(t, pool)
		pending := newTransaction(t, pool, providerID, refExtTxID, "idem-"+refExtTxID, wallet.ID, domain.TransactionRefund, 3000, extTxID)
		mustCreateTransaction(t, pool, repo, pending)

		tx := beginTx(t, pool)
		_, err := repo.FindProcessedReversal(ctx, tx, providerID, extTxID)
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})

	t.Run("processed reversal is found", func(t *testing.T) {
		refExtTxID := "ext-done-" + newUUID(t, pool)
		reversal := newTransaction(t, pool, providerID, refExtTxID, "idem-"+refExtTxID, wallet.ID, domain.TransactionRefund, 3000, extTxID)
		balance, err := domain.NewMoney(10000, "BRL")
		if err != nil {
			t.Fatalf("new money: %v", err)
		}
		if err := reversal.Complete(balance); err != nil {
			t.Fatalf("complete: %v", err)
		}
		mustCreateTransaction(t, pool, repo, reversal)

		tx := beginTx(t, pool)
		got, err := repo.FindProcessedReversal(ctx, tx, providerID, extTxID)
		if err != nil {
			t.Fatalf("FindProcessedReversal: %v", err)
		}
		if got.ID != reversal.ID {
			t.Errorf("ID = %q, want %q", got.ID, reversal.ID)
		}
		if got.ReferenceExternalTransactionID != extTxID {
			t.Errorf("ReferenceExternalTransactionID = %q, want %q", got.ReferenceExternalTransactionID, extTxID)
		}
	})

	t.Run("other provider is isolated", func(t *testing.T) {
		tx := beginTx(t, pool)
		_, err := repo.FindProcessedReversal(ctx, tx, newUUID(t, pool), extTxID)
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			t.Fatalf("error = %v, want ErrTransactionNotFound", err)
		}
	})
}
