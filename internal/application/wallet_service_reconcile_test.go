package application_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// Fixtures de reconciliação são inseridas via SQL direto, de propósito: os
// cenários de corrupção (cadeia quebrada, matemática inválida, primeiro
// balanceBefore != 0) são rejeitados por domain.NewLedgerEntry e precisam
// existir no banco para a reconciliação detectá-los.

type reconFixture struct {
	pool    *pgxpool.Pool
	service *application.WalletService
	wallets *postgres.WalletRepository
}

func newReconFixture(t *testing.T) *reconFixture {
	t.Helper()
	pool := newTestPool(t)
	cleanupTestData(t, pool)
	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)
	return &reconFixture{
		pool:    pool,
		service: application.NewWalletService(pool, wallets, transactions, ledger, outbox),
		wallets: wallets,
	}
}

func (f *reconFixture) seedWallet(t *testing.T, balanceCents int64) (walletID, playerID string) {
	t.Helper()
	walletID = newUUID(t, f.pool)
	playerID = newUUID(t, f.pool)
	_, err := f.pool.Exec(context.Background(),
		`INSERT INTO wallets (id, player_id, currency, balance, version) VALUES ($1, $2, 'BRL', $3, 1)`,
		walletID, playerID, balanceCents)
	if err != nil {
		t.Fatalf("seed wallet: %v", err)
	}
	return walletID, playerID
}

func (f *reconFixture) seedTransaction(t *testing.T, walletID, playerID string, seq int, amountCents int64) string {
	t.Helper()
	txID := newUUID(t, f.pool)
	uniq := newUUID(t, f.pool)
	_, err := f.pool.Exec(context.Background(),
		`INSERT INTO wager_transactions
			(id, provider_id, external_transaction_id, idempotency_key, payload_hash,
			 player_id, wallet_id, kind, status, amount, currency)
		 VALUES ($1, $2, $3, $4, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', $5, $6, 'BET', 'PROCESSED', $7, 'BRL')`,
		txID, newUUID(t, f.pool), "recon-ext-"+uniq, "recon-key-"+uniq, playerID, walletID, amountCents)
	if err != nil {
		t.Fatalf("seed transaction: %v", err)
	}
	return txID
}

func (f *reconFixture) seedEntry(t *testing.T, walletID, txID string, direction domain.LedgerDirection, amountCents, beforeCents, afterCents int64, seq int) string {
	t.Helper()
	entryID := newUUID(t, f.pool)
	createdAt := time.Now().Add(-time.Hour).Add(time.Duration(seq) * time.Second)
	_, err := f.pool.Exec(context.Background(),
		`INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		entryID, walletID, txID, string(direction), amountCents, beforeCents, afterCents, createdAt)
	if err != nil {
		t.Fatalf("seed ledger entry: %v", err)
	}
	return entryID
}

func (f *reconFixture) setWalletBalance(t *testing.T, walletID string, balanceCents int64) {
	t.Helper()
	_, err := f.pool.Exec(context.Background(),
		`UPDATE wallets SET balance = $2 WHERE id = $1`, walletID, balanceCents)
	if err != nil {
		t.Fatalf("set wallet balance: %v", err)
	}
}

func hasIssueType(res *application.ReconciliationResult, want application.ReconciliationIssueType) bool {
	for _, issue := range res.Issues {
		if issue.Type == want {
			return true
		}
	}
	return false
}

func TestReconcileWallet_EmptyLedgerZeroBalance(t *testing.T) {
	f := newReconFixture(t)
	walletID, _ := f.seedWallet(t, 0)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Consistent {
		t.Errorf("consistent = false, want true (issues: %+v)", res.Issues)
	}
	if res.CheckedEntries != 0 {
		t.Errorf("checkedEntries = %d, want 0", res.CheckedEntries)
	}
	if res.StoredBalance.Cents() != 0 || res.CalculatedBalance.Cents() != 0 {
		t.Errorf("stored/calculated = %d/%d, want 0/0", res.StoredBalance.Cents(), res.CalculatedBalance.Cents())
	}
	if len(res.Issues) != 0 {
		t.Errorf("issues = %+v, want none", res.Issues)
	}
}

func TestReconcileWallet_EmptyLedgerPositiveBalance(t *testing.T) {
	f := newReconFixture(t)
	walletID, _ := f.seedWallet(t, 5000)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Consistent {
		t.Error("consistent = true, want false")
	}
	if !hasIssueType(res, application.IssueBalanceMismatch) {
		t.Errorf("want BALANCE_MISMATCH, got %+v", res.Issues)
	}
	if res.Difference.Cents() != 5000 || res.Difference.Currency() != "BRL" {
		t.Errorf("difference = %s %s, want 50.00 BRL", res.Difference.AmountString(), res.Difference.Currency())
	}
}

func TestReconcileWallet_OpeningOnly(t *testing.T) {
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 10000)
	txID := f.seedTransaction(t, walletID, playerID, 0, 10000)
	f.seedEntry(t, walletID, txID, domain.LedgerCredit, 10000, 0, 10000, 0)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Consistent {
		t.Errorf("consistent = false, want true (issues: %+v)", res.Issues)
	}
	if res.CheckedEntries != 1 {
		t.Errorf("checkedEntries = %d, want 1", res.CheckedEntries)
	}
	if res.CalculatedBalance.AmountString() != "100.00" {
		t.Errorf("calculated = %s, want 100.00", res.CalculatedBalance.AmountString())
	}
}

func TestReconcileWallet_ValidCreditDebitSequence(t *testing.T) {
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 7500)
	// +100.00, -30.00, +5.00 => 75.00
	tx0 := f.seedTransaction(t, walletID, playerID, 0, 10000)
	tx1 := f.seedTransaction(t, walletID, playerID, 1, 3000)
	tx2 := f.seedTransaction(t, walletID, playerID, 2, 500)
	f.seedEntry(t, walletID, tx0, domain.LedgerCredit, 10000, 0, 10000, 0)
	f.seedEntry(t, walletID, tx1, domain.LedgerDebit, 3000, 10000, 7000, 1)
	f.seedEntry(t, walletID, tx2, domain.LedgerCredit, 500, 7000, 7500, 2)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Consistent {
		t.Errorf("consistent = false, want true (issues: %+v)", res.Issues)
	}
	if res.CheckedEntries != 3 {
		t.Errorf("checkedEntries = %d, want 3", res.CheckedEntries)
	}
	if res.CalculatedBalance.AmountString() != "75.00" {
		t.Errorf("calculated = %s, want 75.00", res.CalculatedBalance.AmountString())
	}
	if res.Difference.Cents() != 0 {
		t.Errorf("difference = %s, want 0.00", res.Difference.AmountString())
	}
}

func TestReconcileWallet_DivergentBalance(t *testing.T) {
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 8000)
	tx0 := f.seedTransaction(t, walletID, playerID, 0, 10000)
	tx1 := f.seedTransaction(t, walletID, playerID, 1, 3000)
	f.seedEntry(t, walletID, tx0, domain.LedgerCredit, 10000, 0, 10000, 0)
	f.seedEntry(t, walletID, tx1, domain.LedgerDebit, 3000, 10000, 7000, 1)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Consistent {
		t.Error("consistent = true, want false")
	}
	if !hasIssueType(res, application.IssueBalanceMismatch) {
		t.Errorf("want BALANCE_MISMATCH, got %+v", res.Issues)
	}
	if res.CalculatedBalance.AmountString() != "70.00" {
		t.Errorf("calculated = %s, want 70.00", res.CalculatedBalance.AmountString())
	}
	if res.Difference.AmountString() != "10.00" || res.Difference.Currency() != "BRL" {
		t.Errorf("difference = %s %s, want 10.00 BRL", res.Difference.AmountString(), res.Difference.Currency())
	}
}

func TestReconcileWallet_DivergentBalanceStoredBelowCalculated(t *testing.T) {
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 6000)
	tx0 := f.seedTransaction(t, walletID, playerID, 0, 10000)
	tx1 := f.seedTransaction(t, walletID, playerID, 1, 3000)
	f.seedEntry(t, walletID, tx0, domain.LedgerCredit, 10000, 0, 10000, 0)
	f.seedEntry(t, walletID, tx1, domain.LedgerDebit, 3000, 10000, 7000, 1)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Consistent {
		t.Error("consistent = true, want false")
	}
	if !hasIssueType(res, application.IssueBalanceMismatch) {
		t.Errorf("want BALANCE_MISMATCH, got %+v", res.Issues)
	}
	// Magnitude |60 - 70| = 10.00 com a currency da carteira.
	if res.Difference.AmountString() != "10.00" || res.Difference.Currency() != "BRL" {
		t.Errorf("difference = %s %s, want 10.00 BRL", res.Difference.AmountString(), res.Difference.Currency())
	}
}

func TestReconcileWallet_FirstBalanceBeforeNotZero(t *testing.T) {
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 15000)
	tx0 := f.seedTransaction(t, walletID, playerID, 0, 10000)
	f.seedEntry(t, walletID, tx0, domain.LedgerCredit, 10000, 5000, 15000, 0)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Consistent {
		t.Error("consistent = true, want false")
	}
	if !hasIssueType(res, application.IssueFirstBalanceBeforeMismatch) {
		t.Errorf("want FIRST_BALANCE_BEFORE_MISMATCH, got %+v", res.Issues)
	}
}

func TestReconcileWallet_BrokenChain(t *testing.T) {
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 11000)
	tx0 := f.seedTransaction(t, walletID, playerID, 0, 10000)
	tx1 := f.seedTransaction(t, walletID, playerID, 1, 2000)
	f.seedEntry(t, walletID, tx0, domain.LedgerCredit, 10000, 0, 10000, 0)
	// previous.balanceAfter (10000) != current.balanceBefore (9000)
	f.seedEntry(t, walletID, tx1, domain.LedgerCredit, 2000, 9000, 11000, 1)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Consistent {
		t.Error("consistent = true, want false")
	}
	if !hasIssueType(res, application.IssueBalanceChainMismatch) {
		t.Errorf("want BALANCE_CHAIN_MISMATCH, got %+v", res.Issues)
	}
}

func TestReconcileWallet_InvalidEntryMath(t *testing.T) {
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 13000)
	tx0 := f.seedTransaction(t, walletID, playerID, 0, 2500)
	// 100.00 + 25.00 = 125.00, mas balanceAfter gravado é 130.00
	f.seedEntry(t, walletID, tx0, domain.LedgerCredit, 2500, 10000, 13000, 0)

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Consistent {
		t.Error("consistent = true, want false")
	}
	if !hasIssueType(res, application.IssueEntryBalanceMismatch) {
		t.Errorf("want ENTRY_BALANCE_MISMATCH, got %+v", res.Issues)
	}
}

func TestReconcileWallet_NegativeBalanceRejectedByConstraint(t *testing.T) {
	// O schema proíbe balance_after negativo; a reconciliação nunca deve
	// observar esse estado via banco. O teste fixa a invariante de defesa.
	f := newReconFixture(t)
	walletID, playerID := f.seedWallet(t, 0)
	txID := f.seedTransaction(t, walletID, playerID, 0, 100)

	_, err := f.pool.Exec(context.Background(),
		`INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
		 VALUES ($1, $2, $3, 'DEBIT', 100, 0, -100, now())`,
		newUUID(t, f.pool), walletID, txID)
	if err == nil {
		t.Fatal("expected CHECK violation for negative balance_after, got nil error")
	}
	if !strings.Contains(err.Error(), "balance_after") {
		t.Errorf("error = %v, want CHECK constraint on balance_after", err)
	}
}

func TestReconcileWallet_MoreThan100Entries(t *testing.T) {
	f := newReconFixture(t)
	const n = 101
	const amountCents = 100
	walletID, playerID := f.seedWallet(t, int64(n*amountCents))

	var running int64
	for i := 0; i < n; i++ {
		txID := f.seedTransaction(t, walletID, playerID, i, amountCents)
		f.seedEntry(t, walletID, txID, domain.LedgerCredit, amountCents, running, running+amountCents, i)
		running += amountCents
	}

	res, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.CheckedEntries != n {
		t.Errorf("checkedEntries = %d, want %d (todas as entradas, sem limite 100)", res.CheckedEntries, n)
	}
	if !res.Consistent {
		t.Errorf("consistent = false, want true (issues: %+v)", res.Issues)
	}
	if res.CalculatedBalance.Cents() != int64(n*amountCents) {
		t.Errorf("calculated = %d, want %d", res.CalculatedBalance.Cents(), n*amountCents)
	}
}

func TestReconcileWallet_WalletNotFound(t *testing.T) {
	f := newReconFixture(t)

	_, err := f.service.ReconcileWallet(context.Background(), application.ReconciliationInput{WalletID: newUUID(t, f.pool)})
	if err == nil {
		t.Fatal("expected error for unknown wallet, got nil")
	}
}

func TestReconcileWallet_IsReadOnly(t *testing.T) {
	f := newReconFixture(t)
	ctx := context.Background()
	walletID, playerID := f.seedWallet(t, 7500)
	tx0 := f.seedTransaction(t, walletID, playerID, 0, 10000)
	tx1 := f.seedTransaction(t, walletID, playerID, 1, 3000)
	f.seedEntry(t, walletID, tx0, domain.LedgerCredit, 10000, 0, 10000, 0)
	f.seedEntry(t, walletID, tx1, domain.LedgerDebit, 3000, 10000, 7000, 1)

	beforeWallet, err := f.wallets.GetByID(ctx, walletID)
	if err != nil {
		t.Fatalf("get wallet before: %v", err)
	}
	txCountBefore := countWagerTransactions(t, f.pool)
	ledgerCountBefore := countLedgerEntries(t, f.pool)
	outboxCountBefore := countOutboxEvents(t, f.pool)

	if _, err := f.service.ReconcileWallet(ctx, application.ReconciliationInput{WalletID: walletID}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	afterWallet, err := f.wallets.GetByID(ctx, walletID)
	if err != nil {
		t.Fatalf("get wallet after: %v", err)
	}
	if afterWallet.Balance.Cents() != beforeWallet.Balance.Cents() {
		t.Errorf("balance changed: %d -> %d", beforeWallet.Balance.Cents(), afterWallet.Balance.Cents())
	}
	if afterWallet.Version != beforeWallet.Version {
		t.Errorf("version changed: %d -> %d", beforeWallet.Version, afterWallet.Version)
	}
	if got := countWagerTransactions(t, f.pool); got != txCountBefore {
		t.Errorf("transactions changed: %d -> %d", txCountBefore, got)
	}
	if got := countLedgerEntries(t, f.pool); got != ledgerCountBefore {
		t.Errorf("ledger entries changed: %d -> %d", ledgerCountBefore, got)
	}
	if got := countOutboxEvents(t, f.pool); got != outboxCountBefore {
		t.Errorf("outbox events changed: %d -> %d", outboxCountBefore, got)
	}
}
