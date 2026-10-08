package postgres_test

import (
	"context"
	"errors"
	"strings"
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
			$1, $2, 'BET', 'PROCESSED', 5000, 'BRL'
		)
		RETURNING id::text`,
		playerID, walletID,
	).Scan(&txID)
	if err != nil {
		t.Fatalf("insert wager transaction: %v", err)
	}

	t.Cleanup(func() {
		// Ledger é append-only no banco (trigger bloqueia UPDATE/DELETE):
		// TRUNCATE antes de remover a transação (FK child primeiro).
		_, _ = pool.Exec(ctx, "TRUNCATE wallet_ledger_entries")
		_, _ = pool.Exec(ctx,
			"DELETE FROM wager_transactions WHERE id = $1", txID)
	})

	return txID
}

func TestLedgerRepository_AppendOnly(t *testing.T) {
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
	before, err := domain.NewMoney(10000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	after, err := domain.NewMoney(15000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	entry, err := domain.NewLedgerEntry(
		newUUID(t, pool), wallet.ID, txnID, domain.LedgerCredit, amount, before, after,
	)
	if err != nil {
		t.Fatalf("NewLedgerEntry() error = %v", err)
	}

	// INSERT é permitido.
	tx := beginTx(t, pool)
	if err := ledgerRepo.Create(ctx, tx, entry); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// UPDATE é rejeitado pelo trigger (README §5.8/§6.4).
	_, err = pool.Exec(ctx,
		`UPDATE wallet_ledger_entries SET amount = 1 WHERE id = $1`, entry.ID)
	if err == nil {
		t.Fatal("UPDATE ledger: expected trigger rejection, got nil")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("UPDATE ledger error = %v, want *pgconn.PgError", err)
	}
	if !strings.Contains(pgErr.Message, "append-only") {
		t.Errorf("UPDATE ledger message = %q, want append-only rejection", pgErr.Message)
	}

	// DELETE é rejeitado pelo trigger.
	_, err = pool.Exec(ctx,
		`DELETE FROM wallet_ledger_entries WHERE id = $1`, entry.ID)
	if err == nil {
		t.Fatal("DELETE ledger: expected trigger rejection, got nil")
	}
	if !errors.As(err, &pgErr) {
		t.Fatalf("DELETE ledger error = %v, want *pgconn.PgError", err)
	}
	if !strings.Contains(pgErr.Message, "append-only") {
		t.Errorf("DELETE ledger message = %q, want append-only rejection", pgErr.Message)
	}

	// A linha continua intacta.
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE id = $1`, entry.ID,
	).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("ledger rows = %d, want 1", n)
	}
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
	balanceBefore, err := domain.NewMoney(12345, "BRL")
	if err != nil {
		t.Fatalf("NewMoney(balanceBefore) error = %v", err)
	}
	balanceAfter, err := domain.NewMoney(12345+5000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney(balanceAfter) error = %v", err)
	}

	// First insert: should succeed.
	tx1 := beginTx(t, pool)
	entry1, err := domain.NewLedgerEntry(
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
		balanceBefore,
		balanceAfter,
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

func TestLedgerRepository_Create_Rollback(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	ledgerRepo := postgres.NewLedgerRepository(pool)
	walletRepo := postgres.NewWalletRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	txnID := mustInsertWagerTx(t, pool, wallet.ID, wallet.PlayerID)

	tx := beginTx(t, pool)

	amount, err := domain.NewMoney(5000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}

	balanceBefore, err := domain.NewMoney(10000, "BRL")
	if err != nil {
		t.Fatalf("NewMoney(balanceBefore) error = %v", err)
	}

	balanceAfter, err := domain.NewMoney(15000, "BRL")
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

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	var count int
	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE id = $1`,
		entry.ID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("query ledger entry: %v", err)
	}

	if count != 0 {
		t.Fatalf("ledger entry count = %d, want 0", count)
	}
}
