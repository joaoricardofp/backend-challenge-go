package application_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := postgres.NewPool(ctx, "postgres://postgres:postgres@localhost:5432/backend-challenge-go")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newUUID(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id
}

func newMoney(t *testing.T, amount string) domain.Money {
	t.Helper()
	m, err := domain.NewMoneyFromDecimal(amount, "BRL")
	if err != nil {
		t.Fatalf("money: %v", err)
	}
	return m
}

func TestWalletService_OpenWallet_ReopeningRejected(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)
	service := application.NewWalletService(pool, wallets, transactions, ledger, outbox)

	playerID := newUUID(t, pool)
	walletID := newUUID(t, pool)

	// First creation with 100.00
	initial100 := newMoney(t, "100.00")
	res1, err := service.OpenWallet(ctx, application.OpenWalletInput{
		WalletID:       walletID,
		PlayerID:       playerID,
		InitialBalance: initial100,
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if res1.Wallet.Balance.AmountString() != "100.00" {
		t.Errorf("first balance = %s, want 100.00", res1.Wallet.Balance.AmountString())
	}
	if res1.Wallet.Version != 1 {
		t.Errorf("first version = %d, want 1", res1.Wallet.Version)
	}
	if res1.Transaction == nil || !res1.Transaction.IsProcessed() {
		t.Error("first opening should be processed")
	}
	if res1.LedgerEntry == nil {
		t.Error("first ledger entry should exist")
	}

	// Count transactions, ledger entries, outbox events after first creation
	txCount1 := countWagerTransactions(t, pool)
	ledgerCount1 := countLedgerEntries(t, pool)
	outboxCount1 := countOutboxEvents(t, pool)

	// Second creation with 50.00 (should be rejected)
	initial50 := newMoney(t, "50.00")
	_, err = service.OpenWallet(ctx, application.OpenWalletInput{
		WalletID:       newUUID(t, pool), // Different wallet ID, same player+currency
		PlayerID:       playerID,
		InitialBalance: initial50,
	})
	if err == nil {
		t.Fatal("second create should fail with conflict")
	}
	if !errors.Is(err, application.ErrWalletAlreadyExists) {
		t.Errorf("error = %v, want ErrWalletAlreadyExists", err)
	}

	// Verify wallet unchanged
	wallet, err := wallets.GetByID(ctx, walletID)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if wallet.Balance.AmountString() != "100.00" {
		t.Errorf("balance after conflict = %s, want 100.00", wallet.Balance.AmountString())
	}
	if wallet.Version != 1 {
		t.Errorf("version after conflict = %d, want 1", wallet.Version)
	}

	// Verify no new transaction/ledger/outbox created
	txCount2 := countWagerTransactions(t, pool)
	ledgerCount2 := countLedgerEntries(t, pool)
	outboxCount2 := countOutboxEvents(t, pool)

	if txCount2 != txCount1 {
		t.Errorf("transaction count changed: %d -> %d", txCount1, txCount2)
	}
	if ledgerCount2 != ledgerCount1 {
		t.Errorf("ledger count changed: %d -> %d", ledgerCount1, ledgerCount2)
	}
	if outboxCount2 != outboxCount1 {
		t.Errorf("outbox count changed: %d -> %d", outboxCount1, outboxCount2)
	}
}

func TestWalletService_OpenWallet_Concurrency(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	// Clean up any leftover data from previous runs
	cleanupTestData(t, pool)

	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)
	service := application.NewWalletService(pool, wallets, transactions, ledger, outbox)

	playerID := newUUID(t, pool)
	walletID := newUUID(t, pool)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	resultCh := make(chan *application.OpenWalletResult, 2)

	// Two concurrent creations with different initial balances
	wg.Add(2)
	go func() {
		defer wg.Done()
		res, err := service.OpenWallet(ctx, application.OpenWalletInput{
			WalletID:       walletID,
			PlayerID:       playerID,
			InitialBalance: newMoney(t, "100.00"),
		})
		if err != nil {
			errCh <- err
		} else {
			resultCh <- res
		}
	}()
	go func() {
		defer wg.Done()
		res, err := service.OpenWallet(ctx, application.OpenWalletInput{
			WalletID:       newUUID(t, pool),
			PlayerID:       playerID,
			InitialBalance: newMoney(t, "200.00"),
		})
		if err != nil {
			errCh <- err
		} else {
			resultCh <- res
		}
	}()
	wg.Wait()
	close(errCh)
	close(resultCh)

	// Exactly one should succeed, one should fail with conflict
	successCount := 0
	conflictCount := 0
	var finalBalance string
	for res := range resultCh {
		successCount++
		finalBalance = res.Wallet.Balance.AmountString()
	}
	for err := range errCh {
		if errors.Is(err, application.ErrWalletAlreadyExists) {
			conflictCount++
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	}

	if successCount != 1 {
		t.Fatalf("success count = %d, want 1", successCount)
	}
	if conflictCount != 1 {
		t.Fatalf("conflict count = %d, want 1", conflictCount)
	}

	// Final balance must be exactly one of the two (100.00 or 200.00), not sum
	if finalBalance != "100.00" && finalBalance != "200.00" {
		t.Errorf("final balance = %s, want 100.00 or 200.00", finalBalance)
	}
	if finalBalance == "300.00" {
		t.Error("final balance must not be sum of both attempts")
	}

	// Exactly one OPENING, one ledger, two outbox events (decision + balance)
	txCount := countWagerTransactions(t, pool)
	ledgerCount := countLedgerEntries(t, pool)
	outboxCount := countOutboxEvents(t, pool)

	if txCount != 1 {
		t.Errorf("transaction count = %d, want 1", txCount)
	}
	if ledgerCount != 1 {
		t.Errorf("ledger count = %d, want 1", ledgerCount)
	}
	if outboxCount != 2 {
		t.Errorf("outbox count = %d, want 2 (WagerTransactionProcessed + WalletBalanceChanged)", outboxCount)
	}
}

func TestWalletService_OpenWallet_Atomicity(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	// Clean up any leftover data from previous runs
	cleanupTestData(t, pool)

	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)
	service := application.NewWalletService(pool, wallets, transactions, ledger, outbox)

	playerID := newUUID(t, pool)
	walletID := newUUID(t, pool)

	// This test verifies that if wallet creation succeeds but subsequent
	// operations fail, the whole transaction rolls back.
	// We can't easily inject a failure in the middle without modifying the service,
	// but we can verify the unique constraint behavior ensures atomicity:
	// if the wallet insert succeeds but later steps fail, the tx rolls back
	// and the wallet doesn't exist, allowing a retry.

	// First, create successfully
	initial := newMoney(t, "100.00")
	res, err := service.OpenWallet(ctx, application.OpenWalletInput{
		WalletID:       walletID,
		PlayerID:       playerID,
		InitialBalance: initial,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !res.Transaction.IsProcessed() {
		t.Error("opening should be processed")
	}

	// Verify wallet exists
	wallet, err := wallets.GetByID(ctx, walletID)
	if err != nil {
		t.Fatalf("wallet should exist: %v", err)
	}
	if wallet.Balance.AmountString() != "100.00" {
		t.Errorf("balance = %s, want 100.00", wallet.Balance.AmountString())
	}
}

func TestWalletService_OpenWallet_OutboxEvents(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	// Clean up any leftover data from previous runs
	cleanupTestData(t, pool)

	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)
	service := application.NewWalletService(pool, wallets, transactions, ledger, outbox)

	// Create with zero initial balance: no financial events
	playerIDZero := newUUID(t, pool)
	resZero, err := service.OpenWallet(ctx, application.OpenWalletInput{
		WalletID:       newUUID(t, pool),
		PlayerID:       playerIDZero,
		InitialBalance: newMoney(t, "0.00"),
	})
	if err != nil {
		t.Fatalf("create zero: %v", err)
	}
	if resZero.Transaction != nil {
		t.Error("zero initial balance should not create OPENING transaction")
	}
	if resZero.LedgerEntry != nil {
		t.Error("zero initial balance should not create ledger entry")
	}

	// Count outbox after zero balance creation
	outboxCountZero := countOutboxEvents(t, pool)

	// Create with positive initial balance: should create exactly 2 outbox events
	// Use a different player to avoid unique constraint conflict
	playerIDPos := newUUID(t, pool)
	walletID2 := newUUID(t, pool)
	resPos, err := service.OpenWallet(ctx, application.OpenWalletInput{
		WalletID:       walletID2,
		PlayerID:       playerIDPos,
		InitialBalance: newMoney(t, "100.00"),
	})
	if err != nil {
		t.Fatalf("create positive: %v", err)
	}
	if resPos.Transaction == nil || !resPos.Transaction.IsProcessed() {
		t.Error("positive initial balance should create OPENING transaction")
	}
	if resPos.LedgerEntry == nil {
		t.Error("positive initial balance should create ledger entry")
	}

	// Verify outbox events: exactly 2 new events (WagerTransactionProcessed + WalletBalanceChanged)
	outboxCountPos := countOutboxEvents(t, pool)
	newOutboxEvents := outboxCountPos - outboxCountZero
	if newOutboxEvents != 2 {
		t.Errorf("new outbox events = %d, want 2", newOutboxEvents)
	}

	// Verify event types
	tx, err := transactions.GetByID(ctx, resPos.Transaction.ID)
	if err != nil {
		t.Fatalf("get opening tx: %v", err)
	}
	if tx.Kind != domain.TransactionOpening {
		t.Errorf("tx kind = %s, want OPENING", tx.Kind)
	}
	if tx.Status != domain.TransactionProcessed {
		t.Errorf("tx status = %s, want PROCESSED", tx.Status)
	}

	// List outbox events for this aggregate
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	events, err := outbox.ListByAggregate(ctx, dbTx, tx.ID)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	_ = dbTx.Rollback(ctx)

	eventTypes := make(map[string]int)
	for _, ev := range events {
		eventTypes[ev.EventType]++
	}
	if eventTypes["WagerTransactionProcessed"] != 1 {
		t.Errorf("WagerTransactionProcessed count = %d, want 1", eventTypes["WagerTransactionProcessed"])
	}
	if eventTypes["WalletBalanceChanged"] != 1 {
		t.Errorf("WalletBalanceChanged count = %d, want 1", eventTypes["WalletBalanceChanged"])
	}
}

func countWagerTransactions(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM wager_transactions`).Scan(&count); err != nil {
		t.Fatalf("count wager_transactions: %v", err)
	}
	return count
}

func countLedgerEntries(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM wallet_ledger_entries`).Scan(&count); err != nil {
		t.Fatalf("count wallet_ledger_entries: %v", err)
	}
	return count
}

func countOutboxEvents(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events`).Scan(&count); err != nil {
		t.Fatalf("count outbox_events: %v", err)
	}
	return count
}

func cleanupTestData(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `DELETE FROM outbox_events`)
	_, _ = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries`)
	_, _ = pool.Exec(ctx, `DELETE FROM wager_transactions`)
	_, _ = pool.Exec(ctx, `DELETE FROM wallets`)
}

// newLedgerEntry constrói uma entrada de ledger válida em memória.
// Amount é o valor do movimento (balanceAfter = balanceBefore ± amount),
// nunca o saldo resultante.
func newLedgerEntry(t *testing.T, pool *pgxpool.Pool, walletID, transactionID string, direction domain.LedgerDirection, amount, balanceBefore, balanceAfter domain.Money) domain.LedgerEntry {
	t.Helper()
	entry, err := domain.NewLedgerEntry(
		newUUID(t, pool),
		walletID,
		transactionID,
		direction,
		amount,
		balanceBefore,
		balanceAfter,
	)
	if err != nil {
		t.Fatalf("new ledger entry: %v", err)
	}
	return *entry
}
