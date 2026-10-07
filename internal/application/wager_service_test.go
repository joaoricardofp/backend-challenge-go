package application_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

const defaultTestDatabaseURL = "postgres://postgres:postgres@localhost:5432/backend-challenge-go"

func testPool(t *testing.T) *pgxpool.Pool {
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

func testUUID(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	var id string
	err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id)
	if err != nil {
		t.Fatalf("generate uuid: %v", err)
	}

	return id
}

type serviceFixture struct {
	pool         *pgxpool.Pool
	service      *application.WagerService
	wallets      *postgres.WalletRepository
	transactions *postgres.WagerTransactionRepository
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()

	pool := testPool(t)
	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)

	return &serviceFixture{
		pool:         pool,
		service:      application.NewWagerService(pool, wallets, transactions, ledger),
		wallets:      wallets,
		transactions: transactions,
	}
}

// createWallet persiste uma wallet BRL com o saldo informado (version 1) e
// registra a limpeza de wallet, transactions e ledger do teste.
func (f *serviceFixture) createWallet(t *testing.T, balanceCents int64) *domain.Wallet {
	t.Helper()

	wallet, err := domain.NewWallet(testUUID(t, f.pool), testUUID(t, f.pool), "BRL")
	if err != nil {
		t.Fatalf("new wallet: %v", err)
	}
	if balanceCents > 0 {
		deposit, err := domain.NewMoney(balanceCents, "BRL")
		if err != nil {
			t.Fatalf("new money: %v", err)
		}
		if err := wallet.Credit(deposit); err != nil {
			t.Fatalf("credit: %v", err)
		}
	}
	if err := f.wallets.Create(context.Background(), wallet); err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = f.pool.Exec(ctx, "DELETE FROM wallet_ledger_entries WHERE wallet_id = $1", wallet.ID)
		_, _ = f.pool.Exec(ctx, "DELETE FROM wager_transactions WHERE wallet_id = $1", wallet.ID)
		_, _ = f.pool.Exec(ctx, "DELETE FROM wallets WHERE id = $1", wallet.ID)
	})

	return wallet
}

// newInput monta uma transação válida com IDs únicos e a wallet informada.
func (f *serviceFixture) newInput(t *testing.T, wallet *domain.Wallet, kind domain.WagerTransactionKind, amountCents int64, currency string) application.ProcessWagerInput {
	t.Helper()

	amount, err := domain.NewMoney(amountCents, currency)
	if err != nil {
		t.Fatalf("new money: %v", err)
	}

	providerID := testUUID(t, f.pool)
	extTxID := "ext-" + testUUID(t, f.pool)

	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool),
		providerID,
		extTxID,
		"key-"+testUUID(t, f.pool),
		"hash-test",
		wallet.PlayerID,
		wallet.ID,
		"round-1",
		"game-1",
		kind,
		amount,
		"",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}

	return application.ProcessWagerInput{Transaction: *wt}
}

func (f *serviceFixture) readWallet(t *testing.T, id string) *domain.Wallet {
	t.Helper()

	wallet, err := f.wallets.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}

	return wallet
}

func (f *serviceFixture) readTransaction(t *testing.T, providerID, extTxID string) *domain.WagerTransaction {
	t.Helper()

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()

	got, err := f.transactions.GetByProviderExternalID(ctx, dbTx, providerID, extTxID)
	if err != nil {
		t.Fatalf("get transaction: %v", err)
	}

	return got
}

func (f *serviceFixture) transactionNotFound(t *testing.T, providerID, extTxID string) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()

	_, err = f.transactions.GetByProviderExternalID(ctx, dbTx, providerID, extTxID)
	if !errors.Is(err, domain.ErrTransactionNotFound) {
		t.Fatalf("error = %v, want ErrTransactionNotFound", err)
	}
}

type ledgerRow struct {
	direction     string
	amount        int64
	balanceBefore int64
	balanceAfter  int64
}

func (f *serviceFixture) ledgerEntries(t *testing.T, walletID string) []ledgerRow {
	t.Helper()

	rows, err := f.pool.Query(context.Background(),
		`SELECT direction, amount, balance_before, balance_after
		 FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY created_at`,
		walletID,
	)
	if err != nil {
		t.Fatalf("query ledger: %v", err)
	}
	defer rows.Close()

	var out []ledgerRow
	for rows.Next() {
		var r ledgerRow
		if err := rows.Scan(&r.direction, &r.amount, &r.balanceBefore, &r.balanceAfter); err != nil {
			t.Fatalf("scan ledger: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	return out
}

func TestProcessWager_Bet(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	result, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}

	if result.Balance.Cents() != 7000 {
		t.Errorf("Balance = %d, want 7000", result.Balance.Cents())
	}
	if !result.Transaction.IsProcessed() {
		t.Errorf("Status = %q, want PROCESSED", result.Transaction.Status)
	}
	if result.Transaction.ResultingBalance == nil || result.Transaction.ResultingBalance.Cents() != 7000 {
		t.Errorf("ResultingBalance = %+v, want 7000", result.Transaction.ResultingBalance)
	}
	if input.Transaction.IsProcessed() {
		t.Error("input transaction was mutated")
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 7000 {
		t.Errorf("stored balance = %d, want 7000", stored.Balance.Cents())
	}
	if stored.Version != 2 {
		t.Errorf("stored version = %d, want 2", stored.Version)
	}

	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	if entries[0].direction != "DEBIT" || entries[0].amount != 3000 ||
		entries[0].balanceBefore != 10000 || entries[0].balanceAfter != 7000 {
		t.Errorf("ledger entry = %+v, want DEBIT 3000 10000->7000", entries[0])
	}

	persisted := f.readTransaction(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	if !persisted.IsProcessed() {
		t.Errorf("persisted status = %q, want PROCESSED", persisted.Status)
	}
	if persisted.ResultingBalance == nil || persisted.ResultingBalance.Cents() != 7000 {
		t.Errorf("persisted ResultingBalance = %+v, want 7000", persisted.ResultingBalance)
	}
}

func TestProcessWager_Win(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionWin, 3000, "BRL")

	result, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}

	if result.Balance.Cents() != 13000 {
		t.Errorf("Balance = %d, want 13000", result.Balance.Cents())
	}
	if !result.Transaction.IsProcessed() {
		t.Errorf("Status = %q, want PROCESSED", result.Transaction.Status)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 13000 {
		t.Errorf("stored balance = %d, want 13000", stored.Balance.Cents())
	}
	if stored.Version != 2 {
		t.Errorf("stored version = %d, want 2", stored.Version)
	}

	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	if entries[0].direction != "CREDIT" || entries[0].amount != 3000 ||
		entries[0].balanceBefore != 10000 || entries[0].balanceAfter != 13000 {
		t.Errorf("ledger entry = %+v, want CREDIT 3000 10000->13000", entries[0])
	}
}

func TestProcessWager_Loss(t *testing.T) {
	// Reativado após a migration 002 (amount >= 0).
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	amount, err := domain.NewMoney(0, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), testUUID(t, f.pool), "ext-"+testUUID(t, f.pool),
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionLoss, amount, "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	input := application.ProcessWagerInput{Transaction: *wt}

	result, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}

	if result.Balance.Cents() != 10000 {
		t.Errorf("Balance = %d, want 10000", result.Balance.Cents())
	}
	if !result.Transaction.IsProcessed() {
		t.Errorf("Status = %q, want PROCESSED", result.Transaction.Status)
	}
	if result.Transaction.ResultingBalance == nil || result.Transaction.ResultingBalance.Cents() != 10000 {
		t.Errorf("ResultingBalance = %+v, want 10000", result.Transaction.ResultingBalance)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("stored balance = %d, want 10000", stored.Balance.Cents())
	}
	if stored.Version != 1 {
		t.Errorf("stored version = %d, want 1 (no movement)", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_OpeningPositive(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 0)
	input := f.newInput(t, wallet, domain.TransactionOpening, 10000, "BRL")

	result, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}

	if result.Balance.Cents() != 10000 {
		t.Errorf("Balance = %d, want 10000", result.Balance.Cents())
	}
	if !result.Transaction.IsProcessed() {
		t.Errorf("Status = %q, want PROCESSED", result.Transaction.Status)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("stored balance = %d, want 10000", stored.Balance.Cents())
	}
	if stored.Version != 2 {
		t.Errorf("stored version = %d, want 2", stored.Version)
	}

	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	if entries[0].direction != "CREDIT" || entries[0].amount != 10000 ||
		entries[0].balanceBefore != 0 || entries[0].balanceAfter != 10000 {
		t.Errorf("ledger entry = %+v, want CREDIT 10000 0->10000", entries[0])
	}
}

func TestProcessWager_OpeningZero(t *testing.T) {
	// Reativado após a migration 002 (amount >= 0).
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 0)

	amount, err := domain.NewMoney(0, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), testUUID(t, f.pool), "ext-"+testUUID(t, f.pool),
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "", "",
		domain.TransactionOpening, amount, "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	input := application.ProcessWagerInput{Transaction: *wt}

	result, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}

	if result.Balance.Cents() != 0 {
		t.Errorf("Balance = %d, want 0", result.Balance.Cents())
	}
	if !result.Transaction.IsProcessed() {
		t.Errorf("Status = %q, want PROCESSED", result.Transaction.Status)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 0 {
		t.Errorf("stored balance = %d, want 0", stored.Balance.Cents())
	}
	if stored.Version != 1 {
		t.Errorf("stored version = %d, want 1 (no movement)", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_InsufficientFunds(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 10001, "BRL")

	_, err := f.service.Process(context.Background(), input)
	if !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("error = %v, want ErrInsufficientFunds", err)
	}

	// B3.2: a tentativa persiste como REJECTED, sem efeito financeiro.
	rejected := f.readTransaction(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	if !rejected.IsRejected() {
		t.Errorf("status = %q, want REJECTED", rejected.Status)
	}
	if rejected.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Errorf("FailureCode = %q, want INSUFFICIENT_FUNDS", rejected.FailureCode)
	}
	if rejected.ResultingBalance == nil || rejected.ResultingBalance.Cents() != 10000 {
		t.Errorf("ResultingBalance = %+v, want 10000", rejected.ResultingBalance)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("stored balance = %d, want 10000", stored.Balance.Cents())
	}
	if stored.Version != 1 {
		t.Errorf("stored version = %d, want 1", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_CurrencyMismatch(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "USD")

	_, err := f.service.Process(context.Background(), input)
	if !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("error = %v, want ErrCurrencyMismatch", err)
	}

	// B3.3: persiste REJECTED, sem efeito financeiro.
	rejected := f.readTransaction(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	if !rejected.IsRejected() {
		t.Errorf("status = %q, want REJECTED", rejected.Status)
	}
	if rejected.FailureCode != "CURRENCY_MISMATCH" {
		t.Errorf("FailureCode = %q, want CURRENCY_MISMATCH", rejected.FailureCode)
	}
	if rejected.ResultingBalance == nil || rejected.ResultingBalance.Cents() != 10000 {
		t.Errorf("ResultingBalance = %+v, want 10000", rejected.ResultingBalance)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("stored balance = %d, want 10000", stored.Balance.Cents())
	}
	if stored.Version != 1 {
		t.Errorf("stored version = %d, want 1", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_WalletPlayerMismatch(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	input.Transaction.PlayerID = testUUID(t, f.pool) // outro player

	_, err := f.service.Process(context.Background(), input)
	if !errors.Is(err, domain.ErrWalletPlayerMismatch) {
		t.Fatalf("error = %v, want ErrWalletPlayerMismatch", err)
	}

	// B3.3: persiste REJECTED, sem efeito financeiro.
	rejected := f.readTransaction(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	if !rejected.IsRejected() {
		t.Errorf("status = %q, want REJECTED", rejected.Status)
	}
	if rejected.FailureCode != "PLAYER_WALLET_MISMATCH" {
		t.Errorf("FailureCode = %q, want PLAYER_WALLET_MISMATCH", rejected.FailureCode)
	}
	if rejected.ResultingBalance == nil || rejected.ResultingBalance.Cents() != 10000 {
		t.Errorf("ResultingBalance = %+v, want 10000", rejected.ResultingBalance)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("stored balance = %d, want 10000", stored.Balance.Cents())
	}
	if stored.Version != 1 {
		t.Errorf("stored version = %d, want 1", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_DuplicateTransactionRollsBackWallet(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	// Linha pré-existente com o mesmo (provider_id, external_transaction_id).
	providerID := testUUID(t, f.pool)
	extTxID := "ext-" + testUUID(t, f.pool)
	seed, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), providerID, extTxID,
		"key-"+testUUID(t, f.pool), "hash-seed",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 1000), "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	dbTx, err := f.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := f.transactions.Create(context.Background(), dbTx, seed); err != nil {
		_ = dbTx.Rollback(context.Background())
		t.Fatalf("seed transaction: %v", err)
	}
	if err := dbTx.Commit(context.Background()); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = f.pool.Exec(ctx, "DELETE FROM wager_transactions WHERE id = $1", seed.ID)
	})

	amount, err := domain.NewMoney(3000, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), providerID, extTxID,
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, amount, "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}

	_, err = f.service.Process(context.Background(), application.ProcessWagerInput{Transaction: *wt})
	// B3.1: a duplicata no INSERT é recuperada deterministicamente — a
	// releitura encontra a linha seed com hash diferente ("hash-seed" vs
	// fingerprint calculado), logo conflito externo, não o erro bruto.
	if !errors.Is(err, domain.ErrExternalTransactionConflict) {
		t.Fatalf("error = %v, want ErrExternalTransactionConflict", err)
	}

	// O UPDATE da wallet aconteceu antes da falha e deve ter sido revertido.
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("stored balance = %d, want 10000 (rolled back)", stored.Balance.Cents())
	}
	if stored.Version != 1 {
		t.Errorf("stored version = %d, want 1 (rolled back)", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
	seeded := f.readTransaction(t, providerID, extTxID)
	if !seeded.IsPending() {
		t.Errorf("seeded status = %q, want PENDING", seeded.Status)
	}
	if seeded.ResultingBalance != nil {
		t.Errorf("seeded ResultingBalance = %+v, want nil", seeded.ResultingBalance)
	}
}

func TestProcessWager_ConcurrentBets(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	input1 := f.newInput(t, wallet, domain.TransactionBet, 6000, "BRL")
	input2 := f.newInput(t, wallet, domain.TransactionBet, 6000, "BRL")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = f.service.Process(context.Background(), input1)
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = f.service.Process(context.Background(), input2)
	}()
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, domain.ErrInsufficientFunds):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("succeeded = %d, want exactly 1", succeeded)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 4000 {
		t.Errorf("stored balance = %d, want 4000", stored.Balance.Cents())
	}
	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	if entries[0].direction != "DEBIT" || entries[0].amount != 6000 {
		t.Errorf("ledger entry = %+v, want DEBIT 6000", entries[0])
	}
}

func mustMoney(t *testing.T, cents int64) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(cents, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	return m
}
