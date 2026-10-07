package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

// mustCreateWallet cria e persiste uma wallet BRL com saldo de 12345 centavos.
func mustCreateWallet(t *testing.T, pool *pgxpool.Pool, repo *postgres.WalletRepository) *domain.Wallet {
	t.Helper()

	wallet := newWallet(t, pool, newUUID(t, pool), "BRL")

	deposit, err := domain.NewMoney(12345, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	if err := wallet.Credit(deposit); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if err := repo.Create(context.Background(), wallet); err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	return wallet
}

// beginTx abre uma transação e garante rollback no fim do teste, liberando
// qualquer lock. Rollback depois de Commit retorna erro, que é ignorado.
//
// IMPORTANTE: chamar depois de newWallet/mustCreateWallet. Os cleanups rodam
// em ordem inversa, então o rollback acontece antes do DELETE da wallet
// (que ficaria bloqueado se a transação ainda segurasse o lock).
func beginTx(t *testing.T, pool *pgxpool.Pool) pgx.Tx {
	t.Helper()

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	return tx
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

func TestWalletRepository_GetByIDForUpdate(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	wallet := mustCreateWallet(t, pool, repo)
	tx := beginTx(t, pool)

	got, err := repo.GetByIDForUpdate(ctx, tx, wallet.ID)
	if err != nil {
		t.Fatalf("get by id for update: %v", err)
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
	if got.Version != 1 {
		t.Errorf("Version = %d, want %d", got.Version, 1)
	}
}

func TestWalletRepository_GetByIDForUpdate_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	tx := beginTx(t, pool)

	_, err := repo.GetByIDForUpdate(ctx, tx, newUUID(t, pool))
	if !errors.Is(err, domain.ErrWalletNotFound) {
		t.Fatalf("error = %v, want domain.ErrWalletNotFound", err)
	}
}

// Prova que o FOR UPDATE realmente trava a linha: enquanto tx1 segura o lock,
// tx2 não consegue travar a mesma wallet. Usa lock_timeout para falhar rápido
// e de forma determinística, sem depender de sleeps ou goroutines.
func TestWalletRepository_GetByIDForUpdate_LocksRow(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	wallet := mustCreateWallet(t, pool, repo)

	// tx1 pega o lock e o mantém.
	tx1 := beginTx(t, pool)
	if _, err := repo.GetByIDForUpdate(ctx, tx1, wallet.ID); err != nil {
		t.Fatalf("tx1 lock: %v", err)
	}

	// tx2 tenta travar a mesma linha e desiste depois de 200ms.
	tx2 := beginTx(t, pool)
	if _, err := tx2.Exec(ctx, "SET LOCAL lock_timeout = '200ms'"); err != nil {
		t.Fatalf("set lock_timeout: %v", err)
	}

	_, err := repo.GetByIDForUpdate(ctx, tx2, wallet.ID)
	if err == nil {
		t.Fatal("tx2 lock: expected lock timeout while tx1 holds the row, got nil")
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("tx2 error = %v, want *pgconn.PgError", err)
	}
	if pgErr.Code != "55P03" { // lock_not_available
		t.Fatalf("tx2 sqlstate = %s, want 55P03", pgErr.Code)
	}

	// tx2 ficou abortada pelo erro; encerra e libera tx1.
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("tx2 rollback: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}

	// Com o lock liberado, uma nova transação consegue travar normalmente.
	tx3 := beginTx(t, pool)
	if _, err := repo.GetByIDForUpdate(ctx, tx3, wallet.ID); err != nil {
		t.Fatalf("tx3 lock after release: %v", err)
	}
}

// Prova o cenário completo do FOR UPDATE: a TX2 BLOQUEIA enquanto a TX1 segura
// o lock e, depois do COMMIT da TX1, a MESMA TX2 continua e enxerga o dado
// que a TX1 gravou.
//
// Para o teste não passar à toa, o COMMIT só acontece depois de confirmarmos
// (via pg_blocking_pids) que a TX2 está de fato esperando pela TX1.
func TestWalletRepository_GetByIDForUpdate_WaitsForLockRelease(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	wallet := mustCreateWallet(t, pool, repo) // saldo 12345, version 1

	// TX1: trava a wallet e grava um novo saldo, ainda sem commit.
	tx1 := beginTx(t, pool)

	var tx1Pid int32
	if err := tx1.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&tx1Pid); err != nil {
		t.Fatalf("tx1 backend pid: %v", err)
	}

	if _, err := repo.GetByIDForUpdate(ctx, tx1, wallet.ID); err != nil {
		t.Fatalf("tx1 lock: %v", err)
	}

	_, err := tx1.Exec(ctx,
		"UPDATE wallets SET balance = $2, version = version + 1 WHERE id = $1",
		wallet.ID, int64(5000),
	)
	if err != nil {
		t.Fatalf("tx1 update: %v", err)
	}

	// TX2: roda em outra goroutine e deve ficar bloqueada.
	// Dentro da goroutine não se usa t.Fatalf; o resultado volta por canal.
	type lockResult struct {
		wallet *domain.Wallet
		err    error
	}
	resultCh := make(chan lockResult, 1)

	go func() {
		ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		tx2, err := pool.Begin(ctx2)
		if err != nil {
			resultCh <- lockResult{err: fmt.Errorf("begin tx2: %w", err)}
			return
		}

		got, err := repo.GetByIDForUpdate(ctx2, tx2, wallet.ID)

		_ = tx2.Rollback(context.Background())
		resultCh <- lockResult{wallet: got, err: err}
	}()

	// Espera até existir uma sessão bloqueada pela TX1.
	blocked := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := pool.QueryRow(ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE $1::int4 = ANY(pg_blocking_pids(pid))",
			tx1Pid,
		).Scan(&count)
		if err != nil {
			t.Fatalf("check blocked sessions: %v", err)
		}
		if count > 0 {
			blocked = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("tx2 never blocked on tx1's lock: FOR UPDATE is not locking the row")
	}

	// Com a TX2 comprovadamente esperando, ela ainda não pode ter retornado.
	select {
	case res := <-resultCh:
		t.Fatalf("tx2 returned before tx1 committed (err=%v)", res.err)
	default:
	}

	// COMMIT da TX1 libera o lock.
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}

	// A MESMA TX2 continua e deve ver o saldo gravado pela TX1.
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("tx2 get by id for update: %v", res.err)
		}
		if res.wallet.Balance.Cents() != 5000 {
			t.Errorf("tx2 saw Balance.Cents() = %d, want %d", res.wallet.Balance.Cents(), 5000)
		}
		if res.wallet.Version != 2 {
			t.Errorf("tx2 saw Version = %d, want %d", res.wallet.Version, 2)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tx2 did not continue after tx1 committed")
	}
}

func TestWalletRepository_Update(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	repo := postgres.NewWalletRepository(pool)

	wallet := mustCreateWallet(t, pool, repo) // BRL, saldo 12345, version 1

	// Update roda na mesma transação que adquiriu o FOR UPDATE.
	tx := beginTx(t, pool)

	lockedWallet, err := repo.GetByIDForUpdate(ctx, tx, wallet.ID)
	if err != nil {
		t.Fatalf("GetByIDForUpdate() error = %v", err)
	}

	amount, err := domain.NewMoney(5000, lockedWallet.Currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}

	if err := lockedWallet.Credit(amount); err != nil {
		t.Fatalf("Credit() error = %v", err)
	}

	// A version NÃO é incrementada aqui: é responsabilidade do Update (persistência).
	if err := repo.Update(ctx, tx, lockedWallet); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if lockedWallet.Version != 2 {
		t.Errorf("lockedWallet.Version after Update = %d, want 2", lockedWallet.Version)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, wallet.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if want := int64(12345 + 5000); updated.Balance.Cents() != want {
		t.Errorf("Balance = %d, want %d", updated.Balance.Cents(), want)
	}
	if updated.Version != 2 {
		t.Errorf("Version = %d, want 2", updated.Version)
	}
}
