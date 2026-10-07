package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// Testes da identidade persistente de idempotência: UNIQUE(provider_id,
// idempotency_key) como autoridade final contra concorrência. Validam a
// identidade, não o processamento financeiro (B2.2, sem service).

// insertTx persiste wt em transação própria e devolve o erro para o caller
// (sem t.* aqui dentro, para uso em goroutines).
func insertTx(pool *pgxpool.Pool, repo *postgres.WagerTransactionRepository, wt *domain.WagerTransaction) error {
	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.Create(ctx, dbTx, wt); err != nil {
		return err
	}
	return dbTx.Commit(ctx)
}

func countIdentity(t *testing.T, pool *pgxpool.Pool, providerID, key string) int {
	t.Helper()

	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`,
		providerID, key,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// withFingerprint monta uma BET com a identidade idempotente compartilhada
// e o fingerprint real como payload_hash.
func withFingerprint(t *testing.T, pool *pgxpool.Pool, providerID, extTxID, idemKey, walletID string) *domain.WagerTransaction {
	t.Helper()

	wt := newTransaction(t, pool, providerID, extTxID, idemKey, walletID, domain.TransactionBet, 6000, "")
	fp, err := domain.CanonicalWagerFingerprint(*wt)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	wt.PayloadHash = fp
	return wt
}

func TestWagerIdempotency_ConcurrentSameIdentity(t *testing.T) {
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	idemKey := "idem-race-" + newUUID(t, pool)

	// Mesma identidade (provider, key); external IDs distintos para isolar
	// a unique de idempotência como autoridade que decide o vencedor.
	wtA := withFingerprint(t, pool, providerID, "ext-a-"+newUUID(t, pool), idemKey, wallet.ID)
	wtB := withFingerprint(t, pool, providerID, "ext-b-"+newUUID(t, pool), idemKey, wallet.ID)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = insertTx(pool, repo, wtA)
	}()
	go func() {
		defer wg.Done()
		errs[1] = insertTx(pool, repo, wtB)
	}()
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, domain.ErrDuplicateIdempotencyKey):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("succeeded = %d, want exactly 1", succeeded)
	}
	if n := countIdentity(t, pool, providerID, idemKey); n != 1 {
		t.Fatalf("rows = %d, want exactly 1", n)
	}

	// Nenhum efeito financeiro colateral: wallet e ledger intactos.
	stored, err := walletRepo.GetByID(context.Background(), wallet.ID)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if stored.Balance.Cents() != 12345 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 12345/v1", stored.Balance.Cents(), stored.Version)
	}
	var ledgerCount int
	err = pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, wallet.ID,
	).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if ledgerCount != 0 {
		t.Errorf("ledger entries = %d, want 0", ledgerCount)
	}
}

func TestWagerExternalIdentity_ConcurrentSameIdentity(t *testing.T) {
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-race-" + newUUID(t, pool)

	// Mesma identidade externa (provider, external); chaves distintas para
	// isolar a unique externa como autoridade que decide o vencedor.
	wtA := withFingerprint(t, pool, providerID, extTxID, "idem-a-"+newUUID(t, pool), wallet.ID)
	wtB := withFingerprint(t, pool, providerID, extTxID, "idem-b-"+newUUID(t, pool), wallet.ID)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = insertTx(pool, repo, wtA)
	}()
	go func() {
		defer wg.Done()
		errs[1] = insertTx(pool, repo, wtB)
	}()
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, domain.ErrDuplicateExternalID):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("succeeded = %d, want exactly 1", succeeded)
	}

	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, extTxID,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows = %d, want exactly 1", n)
	}
}

func TestWagerIdempotency_FullDuplicateSecondFails(t *testing.T) {
	pool := newTestPool(t)
	walletRepo := postgres.NewWalletRepository(pool)
	repo := postgres.NewWagerTransactionRepository(pool)

	wallet := mustCreateWallet(t, pool, walletRepo)
	providerID := newUUID(t, pool)
	extTxID := "ext-" + newUUID(t, pool)
	idemKey := "idem-" + newUUID(t, pool)

	wtA := withFingerprint(t, pool, providerID, extTxID, idemKey, wallet.ID)
	mustCreateTransaction(t, pool, repo, wtA)

	// Mesma identidade completa (inclusive external ID): a segunda tentativa
	// barra em uma das uniques; exatamente uma linha persiste.
	wtB := withFingerprint(t, pool, providerID, extTxID, idemKey, wallet.ID)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			"DELETE FROM wager_transactions WHERE id = $1", wtB.ID)
	})

	err := insertTx(pool, repo, wtB)
	if !errors.Is(err, domain.ErrDuplicateIdempotencyKey) &&
		!errors.Is(err, domain.ErrDuplicateExternalID) {
		t.Fatalf("error = %v, want idempotency or external duplicate", err)
	}
	if n := countIdentity(t, pool, providerID, idemKey); n != 1 {
		t.Fatalf("rows = %d, want exactly 1", n)
	}
}
