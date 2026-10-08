package application_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// testResolver monta um resolvedor determinístico: sem espera entre
// tentativas na prática (o reagendamento usa next_attempt futuro, e os
// testes chamam ResolveDue diretamente), com MaxAttempts configurável.
func testResolver(f *serviceFixture, maxAttempts int32) *application.ReferenceResolver {
	return application.NewReferenceResolver(f.service, application.ReferenceResolverConfig{
		Interval:    time.Millisecond,
		BatchSize:   10,
		MaxAttempts: maxAttempts,
		MaxBackoff:  time.Second,
	})
}

func pendingProgress(t *testing.T, f *serviceFixture, txID string) (int32, time.Time) {
	t.Helper()
	ctx := context.Background()
	var attempts int32
	var next time.Time
	err := f.pool.QueryRow(ctx,
		`SELECT pending_attempts, pending_next_attempt_at FROM wager_transactions WHERE id = $1`, txID,
	).Scan(&attempts, &next)
	if err != nil {
		t.Fatalf("pending progress: %v", err)
	}
	return attempts, next
}

func TestResolver_ReferenceArrivesLate(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	wallet := f.createWallet(t, 10000)

	// Reversão antes da referência: estaciona PENDING_REFERENCE.
	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-late-bet")
	res, err := f.service.Process(ctx, revInput)
	if err != nil {
		t.Fatalf("Process reversal: %v", err)
	}
	if !res.Transaction.IsPendingReference() {
		t.Fatalf("status = %q, want PENDING_REFERENCE", res.Transaction.Status)
	}

	// A referência chega depois (mesmo provider/external da reversão).
	providerID := revInput.Transaction.ProviderID
	betTx, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), providerID, "ext-late-bet",
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 3000), "",
	)
	if err != nil {
		t.Fatalf("new bet: %v", err)
	}
	if _, err := f.service.Process(ctx, application.ProcessWagerInput{Transaction: *betTx}); err != nil {
		t.Fatalf("Process bet: %v", err)
	}
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 7000 {
		t.Fatalf("balance after bet = %d, want 7000", got.Balance.Cents())
	}

	// O worker conclui a reversão: crédito de volta, com ledger e outbox.
	settled, err := testResolver(f, 10).ResolveDue(ctx)
	if err != nil {
		t.Fatalf("ResolveDue: %v", err)
	}
	if settled != 1 {
		t.Fatalf("settled = %d, want 1", settled)
	}

	done := f.readTransaction(t, providerID, revInput.Transaction.ExternalTransactionID)
	if !done.IsProcessed() {
		t.Errorf("status = %q, want PROCESSED", done.Status)
	}
	if done.ResultingBalance == nil || done.ResultingBalance.Cents() != 10000 {
		t.Errorf("resulting balance = %+v, want 10000", done.ResultingBalance)
	}
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want 10000", got.Balance.Cents())
	}
	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 2 {
		t.Fatalf("ledger entries = %d, want 2 (BET debit + REFUND credit)", len(entries))
	}
	if entries[1].direction != "CREDIT" || entries[1].amount != 3000 ||
		entries[1].balanceBefore != 7000 || entries[1].balanceAfter != 10000 {
		t.Errorf("refund ledger = %+v, want CREDIT 3000 7000->10000", entries[1])
	}
	outboxByType(t, f, done.ID, string(domain.EventWagerTransactionProcessed))
	outboxByType(t, f, done.ID, string(domain.EventWalletBalanceChanged))
}

func TestResolver_ReferenceStillMissingRetries(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	wallet := f.createWallet(t, 10000)

	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-never")
	res, err := f.service.Process(ctx, revInput)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	pendingID := res.Transaction.ID

	before := time.Now().UTC()
	settled, err := testResolver(f, 10).ResolveDue(ctx)
	if err != nil {
		t.Fatalf("ResolveDue: %v", err)
	}
	if settled != 0 {
		t.Fatalf("settled = %d, want 0", settled)
	}

	stored := f.readTransaction(t, revInput.Transaction.ProviderID, revInput.Transaction.ExternalTransactionID)
	if !stored.IsPendingReference() {
		t.Fatalf("status = %q, want still PENDING_REFERENCE", stored.Status)
	}
	attempts, next := pendingProgress(t, f, pendingID)
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
	if !next.After(before) {
		t.Errorf("next_attempt = %v, want after %v", next, before)
	}
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want untouched 10000", got.Balance.Cents())
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}

	// Segunda varredura imediata não encontra vencidos.
	settled, err = testResolver(f, 10).ResolveDue(ctx)
	if err != nil {
		t.Fatalf("ResolveDue again: %v", err)
	}
	if settled != 0 {
		t.Errorf("settled = %d, want 0 (backoff not due)", settled)
	}
	if attempts, _ := pendingProgress(t, f, pendingID); attempts != 1 {
		t.Errorf("attempts = %d, want still 1", attempts)
	}
}

func TestResolver_ExhaustionRejects(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	wallet := f.createWallet(t, 10000)

	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-never")
	res, err := f.service.Process(ctx, revInput)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Simula TTL esgotado: tentativas no limite, vencimento no passado.
	if _, err := f.pool.Exec(ctx,
		`UPDATE wager_transactions SET pending_attempts = 3, pending_next_attempt_at = now() - interval '1 minute' WHERE id = $1`,
		res.Transaction.ID); err != nil {
		t.Fatalf("seed attempts: %v", err)
	}

	settled, err := testResolver(f, 3).ResolveDue(ctx)
	if err != nil {
		t.Fatalf("ResolveDue: %v", err)
	}
	if settled != 1 {
		t.Fatalf("settled = %d, want 1", settled)
	}

	done := f.readTransaction(t, revInput.Transaction.ProviderID, revInput.Transaction.ExternalTransactionID)
	if !done.IsRejected() {
		t.Fatalf("status = %q, want REJECTED", done.Status)
	}
	if done.FailureCode != "REFERENCE_NOT_FOUND" {
		t.Errorf("failureCode = %q, want REFERENCE_NOT_FOUND", done.FailureCode)
	}
	if done.ResultingBalance == nil || done.ResultingBalance.Cents() != 10000 {
		t.Errorf("resulting balance = %+v, want 10000", done.ResultingBalance)
	}
	outboxByType(t, f, done.ID, string(domain.EventWagerTransactionRejected))
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want untouched 10000", got.Balance.Cents())
	}
}

func TestResolver_TerminalReferenceRejects(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	wallet := f.createWallet(t, 10000)
	providerID := testUUID(t, f.pool)

	// Reversão estaciona porque a referência ainda não existe.
	revInput := reversalInput(t, f, providerID, wallet, domain.TransactionRefund, 3000, "ext-doomed-bet")
	if _, err := f.service.Process(ctx, revInput); err != nil {
		t.Fatalf("Process reversal: %v", err)
	}

	// A referência chega e é REJEITADA (sem saldo para a aposta).
	betTx, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), providerID, "ext-doomed-bet",
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 20000), "",
	)
	if err != nil {
		t.Fatalf("new bet: %v", err)
	}
	if _, err := f.service.Process(ctx, application.ProcessWagerInput{Transaction: *betTx}); err == nil {
		t.Fatal("expected insufficient funds error, got nil")
	}
	ref := f.readTransaction(t, providerID, "ext-doomed-bet")
	if !ref.IsRejected() {
		t.Fatalf("reference status = %q, want REJECTED", ref.Status)
	}

	settled, err := testResolver(f, 10).ResolveDue(ctx)
	if err != nil {
		t.Fatalf("ResolveDue: %v", err)
	}
	if settled != 1 {
		t.Fatalf("settled = %d, want 1", settled)
	}
	done := f.readTransaction(t, providerID, revInput.Transaction.ExternalTransactionID)
	if !done.IsRejected() {
		t.Fatalf("status = %q, want REJECTED", done.Status)
	}
	if done.FailureCode != "REFERENCE_NOT_PROCESSED" {
		t.Errorf("failureCode = %q, want REFERENCE_NOT_PROCESSED", done.FailureCode)
	}
}

func TestResolver_SecondRunIsNoop(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	wallet := f.createWallet(t, 10000)

	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-once-bet")
	if _, err := f.service.Process(ctx, revInput); err != nil {
		t.Fatalf("Process reversal: %v", err)
	}
	betTx, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), revInput.Transaction.ProviderID, "ext-once-bet",
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 3000), "",
	)
	if err != nil {
		t.Fatalf("new bet: %v", err)
	}
	if _, err := f.service.Process(ctx, application.ProcessWagerInput{Transaction: *betTx}); err != nil {
		t.Fatalf("Process bet: %v", err)
	}

	r := testResolver(f, 10)
	if settled, err := r.ResolveDue(ctx); err != nil || settled != 1 {
		t.Fatalf("first ResolveDue = %d, %v; want 1, nil", settled, err)
	}
	ledgerBefore := f.ledgerEntries(t, wallet.ID)
	outboxBefore := outboxRows(t, f, revInput.Transaction.ID)

	if settled, err := r.ResolveDue(ctx); err != nil || settled != 0 {
		t.Fatalf("second ResolveDue = %d, %v; want 0, nil", settled, err)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != len(ledgerBefore) {
		t.Errorf("ledger grew: %d -> %d", len(ledgerBefore), len(entries))
	}
	if events := outboxRows(t, f, revInput.Transaction.ID); len(events) != len(outboxBefore) {
		t.Errorf("outbox grew: %d -> %d", len(outboxBefore), len(events))
	}
}

func TestResolver_ConcurrentResolversSettleOnce(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	wallet := f.createWallet(t, 10000)

	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-race-bet")
	if _, err := f.service.Process(ctx, revInput); err != nil {
		t.Fatalf("Process reversal: %v", err)
	}
	betTx, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), revInput.Transaction.ProviderID, "ext-race-bet",
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 3000), "",
	)
	if err != nil {
		t.Fatalf("new bet: %v", err)
	}
	if _, err := f.service.Process(ctx, application.ProcessWagerInput{Transaction: *betTx}); err != nil {
		t.Fatalf("Process bet: %v", err)
	}

	// Duas instâncias independentes disputam a mesma linha.
	var wg sync.WaitGroup
	settled := make([]int, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			settled[i], errs[i] = testResolver(f, 10).ResolveDue(ctx)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("resolver %d: %v", i, err)
		}
	}
	if settled[0]+settled[1] != 1 {
		t.Fatalf("settled = %v, want exactly one completion total", settled)
	}
	done := f.readTransaction(t, revInput.Transaction.ProviderID, revInput.Transaction.ExternalTransactionID)
	if !done.IsProcessed() {
		t.Errorf("status = %q, want PROCESSED", done.Status)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 2 {
		t.Errorf("ledger entries = %d, want 2 (single completion)", len(entries))
	}
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want 10000", got.Balance.Cents())
	}
}

func TestResolver_RedeliveryThenResolve(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	wallet := f.createWallet(t, 10000)

	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-redeliver-bet")
	if _, err := f.service.Process(ctx, revInput); err != nil {
		t.Fatalf("Process reversal: %v", err)
	}
	// Reentrega da mesma identidade não resolve nem move nada.
	if _, err := f.service.Process(ctx, revInput); err == nil {
		t.Fatal("expected in-progress error on redelivery, got nil")
	}

	betTx, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), revInput.Transaction.ProviderID, "ext-redeliver-bet",
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 3000), "",
	)
	if err != nil {
		t.Fatalf("new bet: %v", err)
	}
	if _, err := f.service.Process(ctx, application.ProcessWagerInput{Transaction: *betTx}); err != nil {
		t.Fatalf("Process bet: %v", err)
	}
	settled, err := testResolver(f, 10).ResolveDue(ctx)
	if err != nil {
		t.Fatalf("ResolveDue: %v", err)
	}
	if settled != 1 {
		t.Fatalf("settled = %d, want 1", settled)
	}
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want 10000", got.Balance.Cents())
	}
}
