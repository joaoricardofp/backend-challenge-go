package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

const defaultTestDatabaseURL = "postgres://postgres:postgres@localhost:5432/backend-challenge-go"

// newTestPool conecta no PostgreSQL real. Usa DATABASE_URL se existir,
// senão o banco local do docker-compose. Falha (não pula) se não conectar.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultTestDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}

	t.Cleanup(pool.Close)

	return pool
}

// newUUID pede um UUID ao próprio PostgreSQL, evitando dependência extra.
func newUUID(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	var id string
	err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id)
	if err != nil {
		t.Fatalf("generate uuid: %v", err)
	}

	return id
}

// newWallet cria a wallet de domínio (ainda não persistida) e registra a
// limpeza no banco. O DELETE é seguro mesmo se a wallet nunca for inserida.
func newWallet(t *testing.T, pool *pgxpool.Pool, playerID, currency string) *domain.Wallet {
	t.Helper()

	wallet, err := domain.NewWallet(newUUID(t, pool), playerID, currency)
	if err != nil {
		t.Fatalf("new wallet: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "DELETE FROM wallets WHERE id = $1", wallet.ID)
		if err != nil {
			t.Errorf("cleanup wallet %s: %v", wallet.ID, err)
		}
	})

	return wallet
}

func TestWalletRepository_CreateAndGetByID(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	wallet := newWallet(t, pool, newUUID(t, pool), "brl") // minúsculo: o domínio normaliza

	// Saldo não-zero para provar o round-trip de int64.
	deposit, err := domain.NewMoney(12345, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	if err := wallet.Credit(deposit); err != nil {
		t.Fatalf("credit: %v", err)
	}

	if err := repo.Create(ctx, wallet); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := repo.GetByID(ctx, wallet.ID)
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}

	if got.ID != wallet.ID {
		t.Errorf("ID = %q, want %q", got.ID, wallet.ID)
	}
	if got.PlayerID != wallet.PlayerID {
		t.Errorf("PlayerID = %q, want %q", got.PlayerID, wallet.PlayerID)
	}
	if got.Currency != "BRL" {
		t.Errorf("Currency = %q, want %q", got.Currency, "BRL")
	}
	if got.Balance.Cents() != 12345 {
		t.Errorf("Balance.Cents() = %d, want %d", got.Balance.Cents(), 12345)
	}
	if got.Balance.Currency() != "BRL" {
		t.Errorf("Balance.Currency() = %q, want %q", got.Balance.Currency(), "BRL")
	}
	if got.Version != 1 {
		t.Errorf("Version = %d, want %d", got.Version, 1)
	}
}

func TestWalletRepository_GetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	_, err := repo.GetByID(ctx, newUUID(t, pool)) // UUID válido que não existe no banco
	if !errors.Is(err, domain.ErrWalletNotFound) {
		t.Fatalf("error = %v, want domain.ErrWalletNotFound", err)
	}
}

func TestWalletRepository_Create_DuplicatePlayerCurrency(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	playerID := newUUID(t, pool)

	first := newWallet(t, pool, playerID, "BRL")
	if err := repo.Create(ctx, first); err != nil {
		t.Fatalf("create first: %v", err)
	}

	// Mesma combinação (player_id, currency), outro id: deve violar a UNIQUE.
	second := newWallet(t, pool, playerID, "BRL")
	err := repo.Create(ctx, second)
	if err == nil {
		t.Fatal("create second: expected unique violation, got nil")
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("error = %v, want *pgconn.PgError", err)
	}
	if pgErr.Code != "23505" { // unique_violation
		t.Errorf("sqlstate = %s, want 23505", pgErr.Code)
	}
}
