package application_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

func TestProcessWager_RejectedReplay(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 10001, "BRL")

	if _, err := f.service.Process(context.Background(), input); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("first error = %v, want ErrInsufficientFunds", err)
	}

	second, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("replay Process: unexpected error: %v", err)
	}
	if !second.Replayed {
		t.Error("not marked as replay")
	}
	if !second.Transaction.IsRejected() {
		t.Errorf("status = %q, want REJECTED", second.Transaction.Status)
	}
	if second.Transaction.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Errorf("FailureCode = %q, want INSUFFICIENT_FUNDS", second.Transaction.FailureCode)
	}
	if second.Transaction.ID != input.Transaction.ID {
		t.Errorf("ID = %q, want %q", second.Transaction.ID, input.Transaction.ID)
	}

	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_RejectedExternalKnown(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 10001, "BRL")

	if _, err := f.service.Process(context.Background(), input); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("first error = %v, want ErrInsufficientFunds", err)
	}

	res, err := f.service.Process(context.Background(),
		cloneContent(t, f, &input.Transaction, "key-other", input.Transaction.ExternalTransactionID))
	if err != nil {
		t.Fatalf("Process: unexpected error: %v", err)
	}
	if !res.Replayed {
		t.Error("known external not marked as replay")
	}
	if res.Transaction.ID != input.Transaction.ID {
		t.Errorf("ID = %q, want %q", res.Transaction.ID, input.Transaction.ID)
	}
	if !res.Transaction.IsRejected() {
		t.Errorf("status = %q, want REJECTED", res.Transaction.Status)
	}
	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
}

func TestProcessWager_OverflowBecomesFailed(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, math.MaxInt64)
	input := f.newInput(t, wallet, domain.TransactionWin, 1, "BRL")

	_, err := f.service.Process(context.Background(), input)
	if !errors.Is(err, domain.ErrOverflow) {
		t.Fatalf("error = %v, want ErrOverflow", err)
	}

	failed := f.readTransaction(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	if !failed.IsFailed() {
		t.Errorf("status = %q, want FAILED", failed.Status)
	}
	if failed.FailureCode != "OVERFLOW" {
		t.Errorf("FailureCode = %q, want OVERFLOW", failed.FailureCode)
	}
	if failed.ResultingBalance != nil {
		t.Errorf("ResultingBalance = %+v, want nil (domain Fail semantics)", failed.ResultingBalance)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != math.MaxInt64 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want MaxInt64/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_FailedReplay(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, math.MaxInt64)
	input := f.newInput(t, wallet, domain.TransactionWin, 1, "BRL")

	if _, err := f.service.Process(context.Background(), input); !errors.Is(err, domain.ErrOverflow) {
		t.Fatalf("first error = %v, want ErrOverflow", err)
	}

	res, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("replay Process: unexpected error: %v", err)
	}
	if !res.Replayed {
		t.Error("not marked as replay")
	}
	if !res.Transaction.IsFailed() {
		t.Errorf("status = %q, want FAILED", res.Transaction.Status)
	}
	if res.Transaction.ID != input.Transaction.ID {
		t.Errorf("ID = %q, want %q", res.Transaction.ID, input.Transaction.ID)
	}

	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_ConcurrentInsufficientSameIdentity(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 5000)
	input := f.newInput(t, wallet, domain.TransactionBet, 6000, "BRL")

	var wg sync.WaitGroup
	results := make([]*application.ProcessWagerResult, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i := range errs {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.service.Process(context.Background(), input)
		}(i)
	}
	wg.Wait()

	rejectedErr, replayed := 0, 0
	for i, err := range errs {
		switch {
		case errors.Is(err, domain.ErrInsufficientFunds):
			rejectedErr++
			if results[i] != nil {
				t.Errorf("result with error should be nil, got %+v", results[i])
			}
		case err == nil && results[i] != nil && results[i].Replayed && results[i].Transaction.IsRejected():
			replayed++
		default:
			t.Fatalf("unexpected outcome: err=%v result=%+v", err, results[i])
		}
	}
	if rejectedErr != 1 || replayed != 1 {
		t.Fatalf("rejected = %d, replayed = %d, want 1 and 1", rejectedErr, replayed)
	}

	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Fatalf("transactions = %d, want exactly 1 REJECTED", n)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 5000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 5000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Fatalf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_ContextCanceled(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.service.Process(ctx, input)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled (propagated, never FAILED)", err)
	}

	// Nada foi persistido: sem linha terminal, sem efeito.
	f.transactionNotFound(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_MismatchReplay(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input       func(t *testing.T, f *serviceFixture, wallet *domain.Wallet) application.ProcessWagerInput
		wantErr     error
		failureCode string
	}{
		{name: "player", input: func(t *testing.T, f *serviceFixture, wallet *domain.Wallet) application.ProcessWagerInput {
			in := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
			in.Transaction.PlayerID = testUUID(t, f.pool)
			return in
		}, wantErr: domain.ErrWalletPlayerMismatch, failureCode: "PLAYER_WALLET_MISMATCH"},
		{name: "currency", input: func(t *testing.T, f *serviceFixture, wallet *domain.Wallet) application.ProcessWagerInput {
			return f.newInput(t, wallet, domain.TransactionBet, 3000, "USD")
		}, wantErr: domain.ErrCurrencyMismatch, failureCode: "CURRENCY_MISMATCH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceFixture(t)
			wallet := f.createWallet(t, 10000)
			input := tc.input(t, f, wallet)

			if _, err := f.service.Process(context.Background(), input); !errors.Is(err, tc.wantErr) {
				t.Fatalf("first error = %v, want %v", err, tc.wantErr)
			}

			res, err := f.service.Process(context.Background(), input)
			if err != nil {
				t.Fatalf("replay Process: unexpected error: %v", err)
			}
			if !res.Replayed {
				t.Error("not marked as replay")
			}
			if !res.Transaction.IsRejected() {
				t.Errorf("status = %q, want REJECTED", res.Transaction.Status)
			}
			if res.Transaction.FailureCode != tc.failureCode {
				t.Errorf("FailureCode = %q, want %q", res.Transaction.FailureCode, tc.failureCode)
			}
			if res.Transaction.ID != input.Transaction.ID {
				t.Errorf("ID = %q, want %q", res.Transaction.ID, input.Transaction.ID)
			}

			other, err := f.service.Process(context.Background(),
				cloneContent(t, f, &input.Transaction, "key-other", input.Transaction.ExternalTransactionID))
			if err != nil {
				t.Fatalf("external known Process: unexpected error: %v", err)
			}
			if !other.Replayed || other.Transaction.ID != input.Transaction.ID {
				t.Errorf("external known = %+v, want replay of %q", other, input.Transaction.ID)
			}

			if n := countRows(t, f, wallet.ID); n != 1 {
				t.Errorf("transactions = %d, want 1", n)
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

func TestProcessWager_WalletNotFoundStaysPureError(t *testing.T) {
	// Decisão B3.3 §7: wallet inexistente continua erro puro, sem linha
	// terminal. Sem wallet não há saldo a registrar e a condição é
	// transitória (a wallet pode ser aberta depois): um REJECTED terminal
	// congelaria a resposta errada para a mesma identidade.
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	input.Transaction.WalletID = testUUID(t, f.pool) // wallet inexistente

	_, err := f.service.Process(context.Background(), input)
	if !errors.Is(err, domain.ErrWalletNotFound) {
		t.Fatalf("error = %v, want ErrWalletNotFound (pure, never REJECTED)", err)
	}

	f.transactionNotFound(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
}

func TestProcessWager_StructuralInputCreatesNothing(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	input.Transaction.Status = domain.TransactionProcessed // não é uma criação nova

	_, err := f.service.Process(context.Background(), input)
	if !errors.Is(err, domain.ErrTransactionNotPending) {
		t.Fatalf("error = %v, want ErrTransactionNotPending", err)
	}

	f.transactionNotFound(t, input.Transaction.ProviderID, input.Transaction.ExternalTransactionID)
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
}

func TestProcessWager_ConcurrentMismatchSameIdentity(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "USD")

	var wg sync.WaitGroup
	results := make([]*application.ProcessWagerResult, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i := range errs {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.service.Process(context.Background(), input)
		}(i)
	}
	wg.Wait()

	mismatch, replayed := 0, 0
	for i, err := range errs {
		switch {
		case errors.Is(err, domain.ErrCurrencyMismatch):
			mismatch++
			if results[i] != nil {
				t.Errorf("result with error should be nil, got %+v", results[i])
			}
		case err == nil && results[i] != nil && results[i].Replayed && results[i].Transaction.IsRejected():
			replayed++
		default:
			t.Fatalf("unexpected outcome: err=%v result=%+v", err, results[i])
		}
	}
	if mismatch != 1 || replayed != 1 {
		t.Fatalf("mismatch = %d, replayed = %d, want 1 and 1", mismatch, replayed)
	}

	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Fatalf("transactions = %d, want exactly 1 REJECTED", n)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Fatalf("ledger entries = %d, want 0", len(entries))
	}
}
