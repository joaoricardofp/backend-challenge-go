package application_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// reversalInput monta uma reversão com o provider da referência, wallet
// informada e Valores próprios de identidade.
func reversalInput(t *testing.T, f *serviceFixture, providerID string, wallet *domain.Wallet, kind domain.WagerTransactionKind, cents int64, refExt string) application.ProcessWagerInput {
	t.Helper()

	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), providerID, "ext-"+testUUID(t, f.pool),
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		kind, mustMoney(t, cents), refExt,
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	return application.ProcessWagerInput{Transaction: *wt}
}

func mustProcess(t *testing.T, f *serviceFixture, input application.ProcessWagerInput) *application.ProcessWagerResult {
	t.Helper()

	res, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}
	return res
}

func reversalLedgerCount(t *testing.T, f *serviceFixture, walletID, txID string) int {
	t.Helper()

	var n int
	err := f.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND transaction_id = $2`,
		walletID, txID,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	return n
}

func TestProcessWager_RefundBet(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput)

	refInput := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	res := mustProcess(t, f, refInput)

	if !res.Transaction.IsProcessed() {
		t.Errorf("status = %q, want PROCESSED", res.Transaction.Status)
	}
	if res.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want 10000", res.Balance.Cents())
	}
	if res.Replayed {
		t.Error("fresh reversal marked as replay")
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 3 {
		t.Errorf("wallet = %d/v%d, want 10000/v3", stored.Balance.Cents(), stored.Version)
	}
	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 2 {
		t.Fatalf("ledger entries = %d, want 2", len(entries))
	}
	if entries[1].direction != "CREDIT" || entries[1].amount != 3000 ||
		entries[1].balanceBefore != 7000 || entries[1].balanceAfter != 10000 {
		t.Errorf("refund ledger = %+v, want CREDIT 3000 7000->10000", entries[1])
	}
	if n := reversalLedgerCount(t, f, wallet.ID, refInput.Transaction.ID); n != 1 {
		t.Errorf("reversal ledger entries = %d, want 1", n)
	}

	// Referência inalterada.
	ref := f.readTransaction(t, betInput.Transaction.ProviderID, betInput.Transaction.ExternalTransactionID)
	if !ref.IsProcessed() || ref.Amount.Cents() != 3000 {
		t.Errorf("reference changed: %+v", ref)
	}
	if ref.ResultingBalance == nil || ref.ResultingBalance.Cents() != 7000 {
		t.Errorf("reference balance = %+v, want 7000", ref.ResultingBalance)
	}

	// Replay da própria reversão.
	replay, err := f.service.Process(context.Background(), refInput)
	if err != nil {
		t.Fatalf("replay: unexpected error: %v", err)
	}
	if !replay.Replayed || replay.Transaction.ID != res.Transaction.ID {
		t.Errorf("replay = %+v, want same %q", replay, res.Transaction.ID)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 2 {
		t.Errorf("ledger entries after replay = %d, want 2", len(entries))
	}
}

func TestProcessWager_RollbackBet(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput)

	refInput := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRollback, 3000, betInput.Transaction.ExternalTransactionID)
	res := mustProcess(t, f, refInput)

	if !res.Transaction.IsProcessed() {
		t.Errorf("status = %q, want PROCESSED", res.Transaction.Status)
	}
	if res.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want 10000", res.Balance.Cents())
	}
	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 2 || entries[1].direction != "CREDIT" {
		t.Errorf("ledgers = %+v, want DEBIT then CREDIT", entries)
	}
}

func TestProcessWager_RollbackWin(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	winInput := f.newInput(t, wallet, domain.TransactionWin, 3000, "BRL")
	mustProcess(t, f, winInput)

	refInput := reversalInput(t, f, winInput.Transaction.ProviderID, wallet, domain.TransactionRollback, 3000, winInput.Transaction.ExternalTransactionID)
	res := mustProcess(t, f, refInput)

	if res.Balance.Cents() != 10000 {
		t.Errorf("balance = %d, want 10000", res.Balance.Cents())
	}
	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 2 {
		t.Fatalf("ledger entries = %d, want 2", len(entries))
	}
	if entries[1].direction != "DEBIT" || entries[1].balanceBefore != 13000 || entries[1].balanceAfter != 10000 {
		t.Errorf("rollback ledger = %+v, want DEBIT 3000 13000->10000", entries[1])
	}
}

func TestProcessWager_RollbackRefund(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput)
	refundInput := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	refundRes := mustProcess(t, f, refundInput)

	// ROLLBACK da REFUND (referência distinta da BET): permitido.
	rbInput := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRollback, 3000, refundInput.Transaction.ExternalTransactionID)
	res := mustProcess(t, f, rbInput)

	if !res.Transaction.IsProcessed() {
		t.Errorf("status = %q, want PROCESSED", res.Transaction.Status)
	}
	if res.Balance.Cents() != 7000 {
		t.Errorf("balance = %d, want 7000", res.Balance.Cents())
	}
	_ = refundRes

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 7000 || stored.Version != 4 {
		t.Errorf("wallet = %d/v%d, want 7000/v4", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 3 {
		t.Errorf("ledger entries = %d, want 3", len(entries))
	}
}

func openingInput(t *testing.T, f *serviceFixture, wallet *domain.Wallet, cents int64) application.ProcessWagerInput {
	t.Helper()

	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), testUUID(t, f.pool), "ext-"+testUUID(t, f.pool),
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "", "",
		domain.TransactionOpening, mustMoney(t, cents), "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	return application.ProcessWagerInput{Transaction: *wt}
}

func TestProcessWager_ReversalInvalidTargetKind(t *testing.T) {
	// Alvos processados via service; a reversão deve ser REJECTED com
	// INVALID_REFERENCE_KIND, sem movimento.
	targets := []struct {
		name      string
		kind      domain.WagerTransactionKind
		cents     int64
		reference string
		reversal  domain.WagerTransactionKind
	}{
		{name: "refund win", kind: domain.TransactionWin, cents: 3000, reversal: domain.TransactionRefund},
		{name: "refund loss", kind: domain.TransactionLoss, cents: 0, reversal: domain.TransactionRefund},
		{name: "refund opening", kind: domain.TransactionOpening, cents: 1000, reversal: domain.TransactionRefund},
		{name: "refund refund", kind: domain.TransactionRefund, cents: 3000, reversal: domain.TransactionRefund, reference: "needs-bet"},
		{name: "rollback loss", kind: domain.TransactionLoss, cents: 0, reversal: domain.TransactionRollback},
		{name: "rollback opening", kind: domain.TransactionOpening, cents: 1000, reversal: domain.TransactionRollback},
	}

	for _, tc := range targets {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceFixture(t)
			wallet := f.createWallet(t, 10000)

			var targetInput application.ProcessWagerInput
			if tc.reference != "" {
				// REFUND como alvo: precisa de uma BET antes.
				betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
				mustProcess(t, f, betInput)
				targetInput = reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
			} else if tc.kind == domain.TransactionLoss {
				targetInput = lossInput(t, f, wallet)
			} else if tc.kind == domain.TransactionOpening {
				targetInput = openingInput(t, f, wallet, 1000)
			} else {
				targetInput = f.newInput(t, wallet, tc.kind, tc.cents, "BRL")
			}
			targetRes := mustProcess(t, f, targetInput)
			before := f.readWallet(t, wallet.ID)
			ledgersBefore := len(f.ledgerEntries(t, wallet.ID))

			revInput := reversalInput(t, f, targetInput.Transaction.ProviderID, wallet, tc.reversal, 3000, targetInput.Transaction.ExternalTransactionID)
			if tc.kind == domain.TransactionLoss {
				revInput = reversalInput(t, f, targetInput.Transaction.ProviderID, wallet, tc.reversal, 1000, targetInput.Transaction.ExternalTransactionID)
			}
			_, err := f.service.Process(context.Background(), revInput)
			if !errors.Is(err, domain.ErrInvalidReferenceKind) {
				t.Fatalf("error = %v, want ErrInvalidReferenceKind", err)
			}

			rejected := f.readTransaction(t, revInput.Transaction.ProviderID, revInput.Transaction.ExternalTransactionID)
			if !rejected.IsRejected() || rejected.FailureCode != "INVALID_REFERENCE_KIND" {
				t.Errorf("row = %q/%q, want REJECTED/INVALID_REFERENCE_KIND", rejected.Status, rejected.FailureCode)
			}
			_ = targetRes
			stored := f.readWallet(t, wallet.ID)
			if stored.Balance.Cents() != before.Balance.Cents() || stored.Version != before.Version {
				t.Errorf("wallet = %d/v%d, want %d/v%d (unchanged)", stored.Balance.Cents(), stored.Version, before.Balance.Cents(), before.Version)
			}
			if entries := f.ledgerEntries(t, wallet.ID); len(entries) != ledgersBefore {
				t.Errorf("ledger entries = %d, want %d", len(entries), ledgersBefore)
			}
		})
	}
}

func lossInput(t *testing.T, f *serviceFixture, wallet *domain.Wallet) application.ProcessWagerInput {
	t.Helper()

	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), testUUID(t, f.pool), "ext-"+testUUID(t, f.pool),
		"key-"+testUUID(t, f.pool), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionLoss, mustMoney(t, 0), "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	return application.ProcessWagerInput{Transaction: *wt}
}

func TestProcessWager_ReversalMissingReference(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, "ext-never-existed")
	res, err := f.service.Process(context.Background(), revInput)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}
	if res.Transaction.IsPendingReference() != true {
		t.Errorf("status = %q, want PENDING_REFERENCE", res.Transaction.Status)
	}
	if res.Replayed {
		t.Error("fresh pending reference marked as replay")
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}

	// Mesma identidade enquanto PENDING_REFERENCE -> IN_PROGRESS.
	_, err = f.service.Process(context.Background(), revInput)
	if !errors.Is(err, application.ErrIdempotencyInProgress) {
		t.Fatalf("error = %v, want ErrIdempotencyInProgress", err)
	}

	// Outra chave, mesma external -> também IN_PROGRESS (a linha existe).
	other, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), revInput.Transaction.ProviderID, revInput.Transaction.ExternalTransactionID,
		"key-other", "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionRefund, mustMoney(t, 3000), "ext-never-existed",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	_, err = f.service.Process(context.Background(), application.ProcessWagerInput{Transaction: *other})
	if !errors.Is(err, application.ErrIdempotencyInProgress) {
		t.Fatalf("error = %v, want ErrIdempotencyInProgress", err)
	}
}

func TestProcessWager_ReversalPendingReferenceStatus(t *testing.T) {
	for _, status := range []domain.WagerTransactionStatus{
		domain.TransactionPending,
		domain.TransactionPendingReference,
	} {
		t.Run(string(status), func(t *testing.T) {
			f := newServiceFixture(t)
			wallet := f.createWallet(t, 10000)
			betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

			// Seed da referência sem processar (sem movimento).
			seed := betInput.Transaction
			if status == domain.TransactionPendingReference {
				if err := seed.MarkPendingReference(); err != nil {
					t.Fatalf("mark: %v", err)
				}
			}
			seedWithFingerprint(t, f, &seed)

			revInput := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
			res, err := f.service.Process(context.Background(), revInput)
			if err != nil {
				t.Fatalf("Process: unexpected error: %v", err)
			}
			if !res.Transaction.IsPendingReference() {
				t.Errorf("status = %q, want PENDING_REFERENCE", res.Transaction.Status)
			}

			stored := f.readWallet(t, wallet.ID)
			if stored.Balance.Cents() != 10000 || stored.Version != 1 {
				t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
			}
			if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
				t.Errorf("ledger entries = %d, want 0", len(entries))
			}
		})
	}
}

func TestProcessWager_ReversalTerminalReference(t *testing.T) {
	for _, tc := range []struct {
		name   string
		finish func(tx *domain.WagerTransaction)
	}{
		{name: "rejected", finish: func(tx *domain.WagerTransaction) {
			if err := tx.Reject("NO_FUNDS"); err != nil {
				panic(err)
			}
		}},
		{name: "failed", finish: func(tx *domain.WagerTransaction) {
			if err := tx.Fail("INFRA_TIMEOUT"); err != nil {
				panic(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceFixture(t)
			wallet := f.createWallet(t, 10000)
			betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

			seed := betInput.Transaction
			seedWithFingerprint(t, f, &seed)
			tc.finish(&seed)
			updateTxStatus(t, f, &seed)

			revInput := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
			_, err := f.service.Process(context.Background(), revInput)
			if !errors.Is(err, domain.ErrReferenceNotProcessed) {
				t.Fatalf("error = %v, want ErrReferenceNotProcessed", err)
			}

			rejected := f.readTransaction(t, revInput.Transaction.ProviderID, revInput.Transaction.ExternalTransactionID)
			if !rejected.IsRejected() || rejected.FailureCode != "REFERENCE_NOT_PROCESSED" {
				t.Errorf("row = %q/%q, want REJECTED/REFERENCE_NOT_PROCESSED", rejected.Status, rejected.FailureCode)
			}
			stored := f.readWallet(t, wallet.ID)
			if stored.Balance.Cents() != 10000 || stored.Version != 1 {
				t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
			}
			if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
				t.Errorf("ledger entries = %d, want 0", len(entries))
			}
		})
	}
}

func updateTxStatus(t *testing.T, f *serviceFixture, wt *domain.WagerTransaction) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := f.transactions.UpdateStatus(ctx, dbTx, wt); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("update status: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestProcessWager_DuplicateReversal(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput)

	first := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	mustProcess(t, f, first)

	// Segunda REFUND da mesma referência.
	second := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	_, err := f.service.Process(context.Background(), second)
	if !errors.Is(err, domain.ErrDuplicateReversal) {
		t.Fatalf("error = %v, want ErrDuplicateReversal", err)
	}
	dup := f.readTransaction(t, second.Transaction.ProviderID, second.Transaction.ExternalTransactionID)
	if !dup.IsRejected() || dup.FailureCode != "DUPLICATE_REVERSAL" {
		t.Errorf("row = %q/%q, want REJECTED/DUPLICATE_REVERSAL", dup.Status, dup.FailureCode)
	}

	// ROLLBACK depois de REFUND também bloqueia.
	rb := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRollback, 3000, betInput.Transaction.ExternalTransactionID)
	_, err = f.service.Process(context.Background(), rb)
	if !errors.Is(err, domain.ErrDuplicateReversal) {
		t.Fatalf("error = %v, want ErrDuplicateReversal", err)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("wallet balance = %d, want 10000", stored.Balance.Cents())
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 2 {
		t.Errorf("ledger entries = %d, want 2 (bet + first refund)", len(entries))
	}
}

func TestProcessWager_ReversalIdentity(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput)
	refInput := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	mustProcess(t, f, refInput)

	t.Run("same identity replays", func(t *testing.T) {
		res, err := f.service.Process(context.Background(), refInput)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Replayed {
			t.Error("not marked as replay")
		}
	})

	t.Run("other key is known", func(t *testing.T) {
		other := cloneContent(t, f, &refInput.Transaction, "key-other", refInput.Transaction.ExternalTransactionID)
		res, err := f.service.Process(context.Background(), other)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Replayed {
			t.Error("known external not marked as replay")
		}
	})

	t.Run("same key different hash conflicts", func(t *testing.T) {
		other, err := domain.NewWagerTransaction(
			testUUID(t, f.pool), refInput.Transaction.ProviderID, "ext-other",
			refInput.Transaction.IdempotencyKey, "hash-test",
			wallet.PlayerID, wallet.ID, "round-1", "game-1",
			domain.TransactionRefund, mustMoney(t, 5000), betInput.Transaction.ExternalTransactionID,
		)
		if err != nil {
			t.Fatalf("new wager transaction: %v", err)
		}
		_, err = f.service.Process(context.Background(), application.ProcessWagerInput{Transaction: *other})
		if !errors.Is(err, domain.ErrIdempotencyConflict) {
			t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
		}
	})

	t.Run("same external different hash conflicts", func(t *testing.T) {
		other, err := domain.NewWagerTransaction(
			testUUID(t, f.pool), refInput.Transaction.ProviderID, refInput.Transaction.ExternalTransactionID,
			"key-other", "hash-test",
			wallet.PlayerID, wallet.ID, "round-1", "game-1",
			domain.TransactionRefund, mustMoney(t, 5000), betInput.Transaction.ExternalTransactionID,
		)
		if err != nil {
			t.Fatalf("new wager transaction: %v", err)
		}
		_, err = f.service.Process(context.Background(), application.ProcessWagerInput{Transaction: *other})
		if !errors.Is(err, domain.ErrExternalTransactionConflict) {
			t.Fatalf("error = %v, want ErrExternalTransactionConflict", err)
		}
	})

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("wallet balance = %d, want 10000", stored.Balance.Cents())
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 2 {
		t.Errorf("ledger entries = %d, want 2", len(entries))
	}
}

func TestProcessWager_RollbackInsufficientFunds(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	winInput := f.newInput(t, wallet, domain.TransactionWin, 5000, "BRL")
	mustProcess(t, f, winInput) // 15000, v2
	betInput := f.newInput(t, wallet, domain.TransactionBet, 14500, "BRL")
	mustProcess(t, f, betInput) // 500, v3

	rbInput := reversalInput(t, f, winInput.Transaction.ProviderID, wallet, domain.TransactionRollback, 5000, winInput.Transaction.ExternalTransactionID)
	_, err := f.service.Process(context.Background(), rbInput)
	if !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("error = %v, want ErrInsufficientFunds", err)
	}

	failed := f.readTransaction(t, rbInput.Transaction.ProviderID, rbInput.Transaction.ExternalTransactionID)
	if !failed.IsRejected() || failed.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Errorf("row = %q/%q, want REJECTED/INSUFFICIENT_FUNDS", failed.Status, failed.FailureCode)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 500 || stored.Version != 3 {
		t.Errorf("wallet = %d/v%d, want 500/v3", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 2 {
		t.Errorf("ledger entries = %d, want 2", len(entries))
	}
}

func TestProcessWager_RefundOverflow(t *testing.T) {
	// Crédito além de MaxInt64: a wallet A está cheia (OPENING MaxInt64 via
	// domínio) e a referência BET vive na wallet B. Igualdade
	// wallet/referência ainda não é exigida nesta etapa, então o caminho de
	// overflow do crédito é exercitável e deve virar FAILED.
	f := newServiceFixture(t)
	walletA := f.createWallet(t, math.MaxInt64)
	walletB := f.createWallet(t, 10000)
	betInput := f.newInput(t, walletB, domain.TransactionBet, 1, "BRL")
	mustProcess(t, f, betInput) // walletB 9999

	refInput := reversalInput(t, f, betInput.Transaction.ProviderID, walletA, domain.TransactionRefund, 1, betInput.Transaction.ExternalTransactionID)
	_, err := f.service.Process(context.Background(), refInput)
	if !errors.Is(err, domain.ErrOverflow) {
		t.Fatalf("error = %v, want ErrOverflow", err)
	}

	failed := f.readTransaction(t, refInput.Transaction.ProviderID, refInput.Transaction.ExternalTransactionID)
	if !failed.IsFailed() || failed.FailureCode != "OVERFLOW" {
		t.Errorf("row = %q/%q, want FAILED/OVERFLOW", failed.Status, failed.FailureCode)
	}
	stored := f.readWallet(t, walletA.ID)
	if stored.Balance.Cents() != math.MaxInt64 || stored.Version != 1 {
		t.Errorf("walletA = %d/v%d, want MaxInt64/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, walletA.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
	// Referência intacta.
	ref := f.readTransaction(t, betInput.Transaction.ProviderID, betInput.Transaction.ExternalTransactionID)
	if !ref.IsProcessed() {
		t.Errorf("reference status = %q, want PROCESSED", ref.Status)
	}
}

func TestProcessWager_ReversalProviderIsolation(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput)

	// Mesmo reference_external_transaction_id, provider diferente: a lookup
	// é escopada por provider, logo não encontra -> PENDING_REFERENCE.
	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	res, err := f.service.Process(context.Background(), revInput)
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}
	if !res.Transaction.IsPendingReference() {
		t.Errorf("status = %q, want PENDING_REFERENCE", res.Transaction.Status)
	}
	if res.Transaction.ProviderID == betInput.Transaction.ProviderID {
		t.Error("expected different provider stored")
	}
}

func TestProcessWager_ConcurrentRefundsSameReference(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput) // 7000, v2

	refA := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	refB := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)

	var wg sync.WaitGroup
	results := make([]*application.ProcessWagerResult, 2)
	errs := make([]error, 2)
	inputs := []application.ProcessWagerInput{refA, refB}
	wg.Add(2)
	for i := range errs {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.service.Process(context.Background(), inputs[i])
		}(i)
	}
	wg.Wait()

	processed, duplicate := 0, 0
	for i, err := range errs {
		switch {
		case err == nil && results[i] != nil && results[i].Transaction.IsProcessed():
			processed++
		case errors.Is(err, domain.ErrDuplicateReversal):
			duplicate++
			if results[i] != nil {
				t.Errorf("result with error should be nil, got %+v", results[i])
			}
		default:
			assertNoDeadlock(t, err)
			t.Fatalf("unexpected outcome: err=%v result=%+v", err, results[i])
		}
	}
	if processed != 1 || duplicate != 1 {
		t.Fatalf("processed = %d, duplicate = %d, want 1 and 1", processed, duplicate)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 {
		t.Errorf("wallet balance = %d, want 10000", stored.Balance.Cents())
	}
	var reversalLedgers int
	for _, e := range f.ledgerEntries(t, wallet.ID) {
		if e.direction == "CREDIT" && e.amount == 3000 && e.balanceBefore == 7000 {
			reversalLedgers++
		}
	}
	if reversalLedgers != 1 {
		t.Errorf("reversal ledgers = %d, want 1", reversalLedgers)
	}
}

func TestProcessWager_ConcurrentRefundAndRollback(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	mustProcess(t, f, betInput) // 7000, v2

	refA := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 3000, betInput.Transaction.ExternalTransactionID)
	refB := reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRollback, 3000, betInput.Transaction.ExternalTransactionID)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	inputs := []application.ProcessWagerInput{refA, refB}
	wg.Add(2)
	for i := range errs {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.service.Process(context.Background(), inputs[i])
		}(i)
	}
	wg.Wait()

	processed, duplicate := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			processed++
		case errors.Is(err, domain.ErrDuplicateReversal):
			duplicate++
		default:
			assertNoDeadlock(t, err)
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if processed != 1 || duplicate != 1 {
		t.Fatalf("processed = %d, duplicate = %d, want 1 and 1", processed, duplicate)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 2 {
		t.Errorf("ledger entries = %d, want 2 (bet + winner)", len(entries))
	}
}

func TestProcessWager_MixedConcurrencyNoDeadlock(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 100000)
	betInput := f.newInput(t, wallet, domain.TransactionBet, 1000, "BRL")
	mustProcess(t, f, betInput) // 99000, v2

	inputs := []application.ProcessWagerInput{
		reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 1000, betInput.Transaction.ExternalTransactionID),
		reversalInput(t, f, betInput.Transaction.ProviderID, wallet, domain.TransactionRefund, 1000, betInput.Transaction.ExternalTransactionID),
		f.newInput(t, wallet, domain.TransactionBet, 500, "BRL"),
		f.newInput(t, wallet, domain.TransactionBet, 500, "BRL"),
	}

	var wg sync.WaitGroup
	errs := make([]error, len(inputs))
	wg.Add(len(inputs))
	for i := range inputs {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.service.Process(context.Background(), inputs[i])
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		assertNoDeadlock(t, err)
		if err != nil && !errors.Is(err, domain.ErrDuplicateReversal) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// 99000 -500 -500 +1000 = 99000, v5 (BET0 + 2 BET + 1 REFUND).
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 99000 || stored.Version != 5 {
		t.Errorf("wallet = %d/v%d, want 99000/v5", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 4 {
		t.Errorf("ledger entries = %d, want 4", len(entries))
	}
}

func assertNoDeadlock(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), "40P01") {
		t.Fatalf("deadlock detected: %v", err)
	}
}
