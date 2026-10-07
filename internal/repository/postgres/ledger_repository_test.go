package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// mustInsertWagerTx inserts a minimal wager_transactions row to satisfy the FK
// on wallet_ledger_entries.transaction_id. Returns the transaction UUID string.
// The ledger entries and the transaction row are cleaned up after the test.
func mustInsertWagerTx(t *testing.T, pool *pgxpool.Pool, walletID, playerID string) string {
	t.Helper()

	ctx := context.Background()

	var txID string
	err := pool.QueryRow(ctx,
		`INSERT INTO wager_transactions (
			provider_id, external_transaction_id, idempotency_key, payload_hash,
			player_id, wallet_id, kind, status, amount, currency
		)
		VALUES (
			gen_random_uuid(), gen_random_uuid()::text, gen_random_uuid()::text, 'hash',
			$1, $2, 'BET', 'COMPLETED', 5000, 'BRL'
		)
		RETURNING id::text`,
		playerID, walletID,
	).Scan(&txID)
	if err != nil {
		t.Fatalf("insert wager transaction: %v", err)
	}

	t.Cleanup(func() {
		// Delete ledger entries first (FK child), then the transaction itself.
		_, _ = pool.Exec(ctx,
			"DELETE FROM wallet_ledger_entries WHERE transaction_id = $1", txID)
		_, _ = pool.Exec(ctx,
			"DELETE FROM wager_transactions WHERE id = $1", txID)
	})

	return txID
}

func TestLedgerRepository_Create(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	ledgerRepo := postgres.NewLedgerRepository(pool)
	walletRepo := postgres.NewWalletRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)

	// Insert a wager transaction to satisfy the FK constraint.
	txnID := mustInsertWagerTx(t, pool, wallet.ID, wallet.PlayerID)

	tx := beginTx(t, pool)

	amount, err := domain.NewMoney(5000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}

	balanceBefore, err := domain.NewMoney(12345, "BRL")
	if err != nil {
		t.Fatalf("NewMoney(balanceBefore) error = %v", err)
	}

	balanceAfter, err := domain.NewMoney(12345+5000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney(balanceAfter) error = %v", err)
	}

	entry, err := domain.NewLedgerEntry(
		newUUID(t, pool),
		wallet.ID,
		txnID,
		domain.LedgerCredit,
		amount,
		balanceBefore,
		balanceAfter,
	)
	if err != nil {
		t.Fatalf("NewLedgerEntry() error = %v", err)
	}

	if err := ledgerRepo.Create(ctx, tx, entry); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestLedgerRepository_Create_DuplicateWalletTransaction(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	ledgerRepo := postgres.NewLedgerRepository(pool)
	walletRepo := postgres.NewWalletRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	txnID := mustInsertWagerTx(t, pool, wallet.ID, wallet.PlayerID)

	amount, err := domain.NewMoney(5000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	balance, err := domain.NewMoney(12345, "BRL")
	if err != nil {
		t.Fatalf("NewMoney(balance) error = %v", err)
	}

	// First insert: should succeed.
	tx1 := beginTx(t, pool)
	entry1, err := domain.NewLedgerEntry(
		newUUID(t, pool),
		wallet.ID,
		txnID,
		domain.LedgerCredit,
		amount,
		balance,
		balance,
	)
	if err != nil {
		t.Fatalf("NewLedgerEntry() error = %v", err)
	}
	if err := ledgerRepo.Create(ctx, tx1, entry1); err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}

	// Second insert: same wallet_id + transaction_id must violate the UNIQUE constraint.
	tx2 := beginTx(t, pool)
	entry2, err := domain.NewLedgerEntry(
		newUUID(t, pool), // different ledger entry ID
		wallet.ID,        // same wallet
		txnID,            // same transaction
		domain.LedgerCredit,
		amount,
		balance,
		balance,
	)
	if err != nil {
		t.Fatalf("NewLedgerEntry() error = %v", err)
	}

	err = ledgerRepo.Create(ctx, tx2, entry2)
	if err == nil {
		t.Fatal("second Create(): expected unique violation, got nil")
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("error = %v, want *pgconn.PgError", err)
	}
	if pgErr.Code != "23505" { // unique_violation
		t.Errorf("sqlstate = %s, want 23505", pgErr.Code)
	}
}
