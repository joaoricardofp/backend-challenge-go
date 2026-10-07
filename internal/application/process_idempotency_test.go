package application_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// cloneContent monta um input com o mesmo conteúdo lógico do seed (os 10
// campos do fingerprint), novo ID interno e chave/external informados.
// O PayloadHash é sempre recalculado pelo service.
func cloneContent(t *testing.T, f *serviceFixture, seed *domain.WagerTransaction, key, ext string) application.ProcessWagerInput {
	t.Helper()

	wt, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), seed.ProviderID, ext, key, "hash-clone",
		seed.PlayerID, seed.WalletID, seed.RoundID, seed.GameID,
		seed.Kind, seed.Amount, seed.ReferenceExternalTransactionID,
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	return application.ProcessWagerInput{Transaction: *wt}
}

// seedRow persiste a transaction como está (com o PayloadHash informado).
func seedRow(t *testing.T, f *serviceFixture, wt *domain.WagerTransaction) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := f.transactions.Create(ctx, dbTx, wt); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("create: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// seedWithFingerprint persiste PENDING com o fingerprint real do conteúdo.
func seedWithFingerprint(t *testing.T, f *serviceFixture, wt *domain.WagerTransaction) string {
	t.Helper()

	fp, err := domain.CanonicalWagerFingerprint(*wt)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	wt.PayloadHash = fp
	seedRow(t, f, wt)
	return fp
}

func countRows(t *testing.T, f *serviceFixture, walletID string) int {
	t.Helper()

	var n int
	err := f.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1`, walletID,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestProcessWager_IdempotentReplay(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	first, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("first Process: unexpected error: %v", err)
	}
	if first.Replayed {
		t.Error("first processing marked as replay")
	}

	second, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("second Process: unexpected error: %v", err)
	}
	if !second.Replayed {
		t.Error("replay not marked as replay")
	}
	if second.Transaction.ID != first.Transaction.ID {
		t.Errorf("replay ID = %q, want %q", second.Transaction.ID, first.Transaction.ID)
	}
	if !second.Transaction.IsProcessed() {
		t.Errorf("replay status = %q, want PROCESSED", second.Transaction.Status)
	}
	if second.Balance.Cents() != 7000 {
		t.Errorf("replay balance = %d, want 7000", second.Balance.Cents())
	}
	if second.Transaction.ResultingBalance == nil || second.Transaction.ResultingBalance.Cents() != 7000 {
		t.Errorf("replay ResultingBalance = %+v, want 7000", second.Transaction.ResultingBalance)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 7000 || stored.Version != 2 {
		t.Errorf("wallet = %d/v%d, want 7000/v2", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Errorf("ledger entries = %d, want 1", len(entries))
	}
	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
}

func TestProcessWager_ReplayTerminalStates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		finish func(tx *domain.WagerTransaction)
		status domain.WagerTransactionStatus
	}{
		{name: "rejected", finish: func(tx *domain.WagerTransaction) {
			if err := tx.Reject("NO_FUNDS"); err != nil {
				panic(err)
			}
		}, status: domain.TransactionRejected},
		{name: "failed", finish: func(tx *domain.WagerTransaction) {
			if err := tx.Fail("INFRA_TIMEOUT"); err != nil {
				panic(err)
			}
		}, status: domain.TransactionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServiceFixture(t)
			wallet := f.createWallet(t, 10000)
			input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

			seed := input.Transaction
			seedWithFingerprint(t, f, &seed)
			tc.finish(&seed)
			// Regrava o status terminal (update direto: semântica de seed).
			ctx := context.Background()
			dbTx, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin tx: %v", err)
			}
			if err := f.transactions.UpdateStatus(ctx, dbTx, &seed); err != nil {
				_ = dbTx.Rollback(ctx)
				t.Fatalf("update status: %v", err)
			}
			if err := dbTx.Commit(ctx); err != nil {
				t.Fatalf("commit: %v", err)
			}

			res, err := f.service.Process(context.Background(), cloneContent(t, f, &seed, seed.IdempotencyKey, seed.ExternalTransactionID))
			if err != nil {
				t.Fatalf("Process: unexpected error: %v", err)
			}
			if !res.Replayed {
				t.Error("not marked as replay")
			}
			if res.Transaction.Status != tc.status {
				t.Errorf("status = %q, want %q", res.Transaction.Status, tc.status)
			}
			if res.Transaction.FailureCode == "" {
				t.Error("FailureCode empty, want preserved")
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

func TestProcessWager_IdempotencyConflict(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	seeded, _ := seedProcessed(t, f, wallet)

	// Mesma (provider, key), conteúdo diferente (outro external + amount).
	incoming, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), seeded.ProviderID, "ext-other",
		seeded.IdempotencyKey, "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 5000), "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}

	_, err = f.service.Process(context.Background(), application.ProcessWagerInput{Transaction: *incoming})
	if !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
}

func TestProcessWager_ExternalKnownOtherKey(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	first, err := f.service.Process(context.Background(), input)
	if err != nil {
		t.Fatalf("first Process: unexpected error: %v", err)
	}

	// Mesma operação externa (P, E, H) com outra chave K2.
	second, err := f.service.Process(context.Background(),
		cloneContent(t, f, &input.Transaction, "key-other", input.Transaction.ExternalTransactionID))
	if err != nil {
		t.Fatalf("second Process: unexpected error: %v", err)
	}
	if !second.Replayed {
		t.Error("known external not marked as replay")
	}
	if second.Transaction.ID != first.Transaction.ID {
		t.Errorf("ID = %q, want %q", second.Transaction.ID, first.Transaction.ID)
	}
	if second.Balance.Cents() != 7000 {
		t.Errorf("balance = %d, want 7000", second.Balance.Cents())
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 7000 || stored.Version != 2 {
		t.Errorf("wallet = %d/v%d, want 7000/v2", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Errorf("ledger entries = %d, want 1", len(entries))
	}
	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
}

func TestProcessWager_ExternalConflict(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	if _, err := f.service.Process(context.Background(), input); err != nil {
		t.Fatalf("first Process: unexpected error: %v", err)
	}

	// Mesma (P, E), conteúdo diferente (amount 5000).
	other, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), input.Transaction.ProviderID, input.Transaction.ExternalTransactionID,
		"key-other", "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 5000), "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}

	_, err = f.service.Process(context.Background(), application.ProcessWagerInput{Transaction: *other})
	if !errors.Is(err, domain.ErrExternalTransactionConflict) {
		t.Fatalf("error = %v, want ErrExternalTransactionConflict", err)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 7000 || stored.Version != 2 {
		t.Errorf("wallet = %d/v%d, want 7000/v2 (first processing kept)", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Errorf("ledger entries = %d, want 1", len(entries))
	}
	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Errorf("transactions = %d, want 1", n)
	}
}

func TestProcessWager_IdentityInconsistency(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	const forged = "forged-same-hash"
	keyIn := "key-in-" + testUUID(t, f.pool)
	extIn := "ext-in-" + testUUID(t, f.pool)

	// Linha A por (P, K_in) com external diferente; linha B por (P, E_in)
	// com chave diferente; mesmo hash forjado nas duas.
	providerID := testUUID(t, f.pool)
	buildForged := func(key, ext string) {
		wt, err := domain.NewWagerTransaction(
			testUUID(t, f.pool), providerID, ext, key, "hash-forge",
			wallet.PlayerID, wallet.ID, "round-1", "game-1",
			domain.TransactionBet, mustMoney(t, 3000), "",
		)
		if err != nil {
			t.Fatalf("new wager transaction: %v", err)
		}
		wt.PayloadHash = forged
		seedRow(t, f, wt)
	}
	buildForged(keyIn, "ext-other")
	buildForged("key-other", extIn)

	incoming, err := domain.NewWagerTransaction(
		testUUID(t, f.pool), providerID, extIn, keyIn, "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 3000), "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}

	_, err = f.service.Process(context.Background(), application.ProcessWagerInput{Transaction: *incoming})
	if !errors.Is(err, application.ErrIdentityConflict) {
		t.Fatalf("error = %v, want ErrIdentityConflict", err)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 0 {
		t.Errorf("ledger entries = %d, want 0", len(entries))
	}
	if n := countRows(t, f, wallet.ID); n != 2 {
		t.Errorf("transactions = %d, want 2 (no third created)", n)
	}
}

func TestProcessWager_InProgress(t *testing.T) {
	for _, status := range []domain.WagerTransactionStatus{
		domain.TransactionPending,
		domain.TransactionPendingReference,
	} {
		t.Run(string(status), func(t *testing.T) {
			f := newServiceFixture(t)
			wallet := f.createWallet(t, 10000)
			input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

			seed := input.Transaction
			if status == domain.TransactionPendingReference {
				if err := seed.MarkPendingReference(); err != nil {
					t.Fatalf("mark pending reference: %v", err)
				}
			}
			seedWithFingerprint(t, f, &seed)

			_, err := f.service.Process(context.Background(),
				cloneContent(t, f, &seed, seed.IdempotencyKey, seed.ExternalTransactionID))
			if !errors.Is(err, application.ErrIdempotencyInProgress) {
				t.Fatalf("error = %v, want ErrIdempotencyInProgress", err)
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

func TestProcessWager_ConcurrentSameIdentity(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
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

	fresh, replayed := 0, 0
	for _, err := range errs {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	for _, res := range results {
		if res.Replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != 1 {
		t.Fatalf("fresh = %d, replayed = %d, want 1 and 1", fresh, replayed)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 4000 {
		t.Errorf("stored balance = %d, want 4000", stored.Balance.Cents())
	}
	if stored.Version != 2 {
		t.Errorf("stored version = %d, want 2 (single increment)", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Fatalf("transactions = %d, want 1", n)
	}
}

func TestProcessWager_ConcurrentSameExternalDifferentKeys(t *testing.T) {
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	base := f.newInput(t, wallet, domain.TransactionBet, 6000, "BRL")
	input1 := cloneContent(t, f, &base.Transaction, "key-1-"+testUUID(t, f.pool), base.Transaction.ExternalTransactionID)
	input2 := cloneContent(t, f, &base.Transaction, "key-2-"+testUUID(t, f.pool), base.Transaction.ExternalTransactionID)

	var wg sync.WaitGroup
	results := make([]*application.ProcessWagerResult, 2)
	errs := make([]error, 2)
	inputs := []application.ProcessWagerInput{input1, input2}
	wg.Add(2)
	for i := range errs {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.service.Process(context.Background(), inputs[i])
		}(i)
	}
	wg.Wait()

	fresh, replayed := 0, 0
	for _, err := range errs {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	for _, res := range results {
		if res == nil {
			t.Fatal("nil result without error")
		}
		if res.Replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != 1 {
		t.Fatalf("fresh = %d, replayed = %d, want 1 and 1", fresh, replayed)
	}

	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 4000 {
		t.Errorf("stored balance = %d, want 4000", stored.Balance.Cents())
	}
	if stored.Version != 2 {
		t.Errorf("stored version = %d, want 2 (single increment)", stored.Version)
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	if n := countRows(t, f, wallet.ID); n != 1 {
		t.Fatalf("transactions = %d, want 1", n)
	}
}
