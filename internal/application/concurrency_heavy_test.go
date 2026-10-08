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

// instância independente: pool, repositórios e service próprios (conexões e
// memória separadas), mesmo banco. Três delas aproximam três processos.
type instance struct {
	service *application.WagerService
	wallets *postgres.WalletRepository
}

func newInstance(t *testing.T, pool *pgxpool.Pool) *instance {
	t.Helper()
	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)
	return &instance{
		service: application.NewWagerService(pool, wallets, transactions, ledger, outbox),
		wallets: wallets,
	}
}

// TestHeavy_SameBetFiftyTimes: §13.1 — a mesma aposta 50 vezes em paralelo,
// distribuídas em 3 instâncias independentes: um único débito.
func TestHeavy_SameBetFiftyTimes(t *testing.T) {
	ctx := context.Background()
	pools := []*pgxpool.Pool{testPool(t), testPool(t), testPool(t)}
	insts := []*instance{newInstance(t, pools[0]), newInstance(t, pools[1]), newInstance(t, pools[2])}

	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)
	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")

	const n = 50
	results := make([]*application.ProcessWagerResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = insts[i%3].service.Process(ctx, input)
		}(i)
	}
	wg.Wait()

	fresh, replayed := 0, 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("op %d: unexpected error: %v", i, err)
		}
		if results[i].Replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != n-1 {
		t.Fatalf("fresh = %d, replayed = %d, want 1 and %d", fresh, replayed, n-1)
	}
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 7000 {
		t.Errorf("balance = %d, want 7000 (single debit)", got.Balance.Cents())
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Errorf("ledger entries = %d, want 1", len(entries))
	}
}

// TestHeavy_EightyEightyOnHundred: §13.2 — cenário obrigatório literal:
// 100.00 recebe duas apostas distintas de 80.00: uma processada, uma
// rejeitada por saldo insuficiente, final 20.00, um único débito.
// Reenvios não alteram o resultado.
func TestHeavy_EightyEightyOnHundred(t *testing.T) {
	ctx := context.Background()
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	mkBet := func(ext string) application.ProcessWagerInput {
		providerID := testUUID(t, f.pool)
		wt, err := domain.NewWagerTransaction(
			testUUID(t, f.pool), providerID, ext,
			"key-"+testUUID(t, f.pool), "hash-test",
			wallet.PlayerID, wallet.ID, "round-1", "game-1",
			domain.TransactionBet, mustMoney(t, 8000), "",
		)
		if err != nil {
			t.Fatalf("new wager transaction: %v", err)
		}
		return application.ProcessWagerInput{Transaction: *wt}
	}
	input1, input2 := mkBet("ext-80-a"), mkBet("ext-80-b")

	type outcome struct {
		res *application.ProcessWagerResult
		err error
	}
	out := make([]outcome, 2)
	var wg sync.WaitGroup
	for i, in := range []application.ProcessWagerInput{input1, input2} {
		wg.Add(1)
		go func(i int, in application.ProcessWagerInput) {
			defer wg.Done()
			out[i].res, out[i].err = f.service.Process(ctx, in)
		}(i, in)
	}
	wg.Wait()

	processed, rejected := 0, 0
	for _, o := range out {
		switch {
		case o.err == nil && o.res.Transaction.IsProcessed():
			processed++
		case errors.Is(o.err, domain.ErrInsufficientFunds):
			rejected++
		default:
			t.Fatalf("outcome = (%+v, %v), want processed or INSUFFICIENT_FUNDS", o.res, o.err)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed = %d, rejected = %d, want 1 and 1", processed, rejected)
	}
	stored := f.readWallet(t, wallet.ID)
	if stored.Balance.Cents() != 2000 {
		t.Errorf("balance = %d, want 2000", stored.Balance.Cents())
	}
	entries := f.ledgerEntries(t, wallet.ID)
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	if entries[0].direction != "DEBIT" || entries[0].amount != 8000 {
		t.Errorf("ledger entry = %+v, want DEBIT 8000", entries[0])
	}

	// Reenvios não alteram o resultado: a vencedora replaya, a perdedora
	// continua rejeitada com o mesmo erro.
	winner, loser := input1, input2
	if got := f.readTransaction(t, input1.Transaction.ProviderID, input1.Transaction.ExternalTransactionID); !got.IsProcessed() {
		winner, loser = input2, input1
	}
	replay, err := f.service.Process(ctx, winner)
	if err != nil {
		t.Fatalf("replay winner: %v", err)
	}
	if !replay.Replayed || !replay.Transaction.IsProcessed() {
		t.Errorf("winner replay = %+v, want replayed PROCESSED", replay)
	}
	// A perdedora persiste REJECTED: o reenvio replaya o terminal sem erro.
	loserReplay, err := f.service.Process(ctx, loser)
	if err != nil {
		t.Fatalf("replay loser: %v", err)
	}
	if !loserReplay.Replayed || !loserReplay.Transaction.IsRejected() {
		t.Errorf("loser replay = %+v, want replayed REJECTED", loserReplay)
	}
	if got := f.readWallet(t, wallet.ID); got.Balance.Cents() != 2000 {
		t.Errorf("balance after replays = %d, want still 2000", got.Balance.Cents())
	}
	if entries := f.ledgerEntries(t, wallet.ID); len(entries) != 1 {
		t.Errorf("ledger entries after replays = %d, want still 1", len(entries))
	}
}

// TestHeavy_DistinctWalletsInParallel: §13.3 — carteiras distintas avançam
// em paralelo, sem interferência.
func TestHeavy_DistinctWalletsInParallel(t *testing.T) {
	ctx := context.Background()
	f := newServiceFixture(t)
	w1, w2 := f.createWallet(t, 10000), f.createWallet(t, 10000)
	in1, in2 := f.newInput(t, w1, domain.TransactionBet, 3000, "BRL"),
		f.newInput(t, w2, domain.TransactionBet, 4000, "BRL")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); _, errs[0] = f.service.Process(ctx, in1) }()
	go func() { defer wg.Done(); _, errs[1] = f.service.Process(ctx, in2) }()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("wallet %d: %v", i, err)
		}
	}
	if got := f.readWallet(t, w1.ID); got.Balance.Cents() != 7000 || got.Version != 2 {
		t.Errorf("w1 = %d/v%d, want 7000/v2", got.Balance.Cents(), got.Version)
	}
	if got := f.readWallet(t, w2.ID); got.Balance.Cents() != 6000 || got.Version != 2 {
		t.Errorf("w2 = %d/v%d, want 6000/v2", got.Balance.Cents(), got.Version)
	}
}

// TestHeavy_RestartPreservesDurability: §13.8 (nível aplicação) — após
// descartar pool e services ("reinício"), replay devolve o persistido,
// pendências continuam resolvíveis e o financeiro está intacto.
func TestHeavy_RestartPreservesDurability(t *testing.T) {
	ctx := context.Background()
	f := newServiceFixture(t)
	wallet := f.createWallet(t, 10000)

	input := f.newInput(t, wallet, domain.TransactionBet, 3000, "BRL")
	if _, err := f.service.Process(ctx, input); err != nil {
		t.Fatalf("Process: %v", err)
	}
	revInput := reversalInput(t, f, testUUID(t, f.pool), wallet, domain.TransactionRefund, 1000, "ext-restart-bet")
	if _, err := f.service.Process(ctx, revInput); err != nil {
		t.Fatalf("Process reversal: %v", err)
	}

	// Reinício: novos pool, repositórios e services sobre o mesmo banco.
	pool2 := testPool(t)
	wallets2 := postgres.NewWalletRepository(pool2)
	transactions2 := postgres.NewWagerTransactionRepository(pool2)
	ledger2 := postgres.NewLedgerRepository(pool2)
	outbox2 := postgres.NewOutboxRepository(pool2)
	service2 := application.NewWagerService(pool2, wallets2, transactions2, ledger2, outbox2)
	resolver2 := application.NewReferenceResolver(service2, application.ReferenceResolverConfig{
		Interval: time.Millisecond, BatchSize: 10, MaxAttempts: 10, MaxBackoff: time.Second,
	})

	// Replay após reinício devolve o original.
	replay, err := service2.Process(ctx, input)
	if err != nil {
		t.Fatalf("replay after restart: %v", err)
	}
	if !replay.Replayed || replay.Balance.Cents() != 7000 {
		t.Errorf("replay = %+v, want replayed with balance 7000", replay)
	}

	// A referência chega após o reinício; o worker resolve a pendência.
	betTx, err := domain.NewWagerTransaction(
		testUUID(t, pool2), revInput.Transaction.ProviderID, "ext-restart-bet",
		"key-"+testUUID(t, pool2), "hash-test",
		wallet.PlayerID, wallet.ID, "round-1", "game-1",
		domain.TransactionBet, mustMoney(t, 1000), "",
	)
	if err != nil {
		t.Fatalf("new bet: %v", err)
	}
	if _, err := service2.Process(ctx, application.ProcessWagerInput{Transaction: *betTx}); err != nil {
		t.Fatalf("Process bet after restart: %v", err)
	}
	settled, err := resolver2.ResolveDue(ctx)
	if err != nil {
		t.Fatalf("ResolveDue after restart: %v", err)
	}
	if settled != 1 {
		t.Fatalf("settled = %d, want 1", settled)
	}

	// Consistência financeira final: 10000 - 3000 - 1000 + 1000.
	stored, err := wallets2.GetByID(ctx, wallet.ID)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if stored.Balance.Cents() != 7000 {
		t.Errorf("balance = %d, want 7000", stored.Balance.Cents())
	}
}
