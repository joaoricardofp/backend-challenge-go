package sqs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

const testConsumerName = "wager-consumer-test"

func consumerTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:postgres@localhost:5432/backend-challenge-go"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// fakeProcessor é o WagerService de mentira: comportamento programado e
// contagem de chamadas para provar se o consumer acionou o negócio.
type fakeProcessor struct {
	mu    sync.Mutex
	calls int
	fn    func(ctx context.Context, input application.ProcessWagerInput) (*application.ProcessWagerResult, error)
}

func (f *fakeProcessor) Process(ctx context.Context, input application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.fn(ctx, input)
}

func (f *fakeProcessor) numCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func processedResult() *application.ProcessWagerResult {
	bal, _ := domain.NewMoney(7000, "BRL")
	amt, _ := domain.NewMoney(3000, "BRL")
	return &application.ProcessWagerResult{
		Transaction: domain.WagerTransaction{ID: "tx-1", Status: domain.TransactionProcessed, Amount: amt},
		Balance:     bal,
	}
}

func terminalResult(status domain.WagerTransactionStatus) *application.ProcessWagerResult {
	amt, _ := domain.NewMoney(3000, "BRL")
	bal, _ := domain.NewMoney(7000, "BRL")
	return &application.ProcessWagerResult{
		Transaction: domain.WagerTransaction{ID: "tx-1", Status: status, Amount: amt},
		Balance:     bal,
		Replayed:    true,
	}
}

func pendingRefResult() *application.ProcessWagerResult {
	amt, _ := domain.NewMoney(3000, "BRL")
	bal, _ := domain.NewMoney(10000, "BRL")
	return &application.ProcessWagerResult{
		Transaction: domain.WagerTransaction{ID: "tx-1", Status: domain.TransactionPendingReference, Amount: amt},
		Balance:     bal,
	}
}

// fakeReceiver implementa Receiver sem AWS: fila programável e registro de
// deletes para provar o ack. failDeletes faz os primeiros N deletes falharem
// (simula crash entre Complete e Delete).
type fakeReceiver struct {
	mu          sync.Mutex
	msgs        []ReceivedMessage
	receiveErr  error
	deletes     []string
	failDeletes int
}

func (f *fakeReceiver) ReceiveMessage(_ context.Context, _, _, _ int32) ([]ReceivedMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.receiveErr != nil {
		return nil, f.receiveErr
	}
	out := f.msgs
	f.msgs = nil
	return out, nil
}

func (f *fakeReceiver) DeleteMessage(_ context.Context, receiptHandle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDeletes > 0 {
		f.failDeletes--
		return errors.New("sqs delete failed")
	}
	f.deletes = append(f.deletes, receiptHandle)
	return nil
}

func (f *fakeReceiver) numDeletes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deletes)
}

func newTestConsumer(
	t *testing.T,
	pool *pgxpool.Pool,
	proc Processor,
	recv Receiver,
) (*Consumer, *postgres.InboxRepository) {
	t.Helper()

	inbox := postgres.NewInboxRepository(pool)
	c, err := NewConsumer(pool, inbox, proc, recv, testConsumerName, 10, 0, 30, true, nil)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	return c, inbox
}

func rawFor(msgID, kind, amount string) []byte {
	raw := `{"messageId":"MSG","type":"WagerTransactionRequested",` +
		`"occurredAt":"2026-09-08T12:00:00.000Z",` +
		`"data":{"providerId":"provider-a","externalTransactionId":"EXT",` +
		`"idempotencyKey":"KEY",` +
		`"playerId":"player-1",` +
		`"walletId":"wallet-1",` +
		`"roundId":"round-1","gameId":"game-1","kind":"KIND",` +
		`"money":{"amount":"AMOUNT","currency":"BRL"}}}`
	out := raw
	replace := func(old, new string) {
		for i := 0; i < 1; i++ {
			idx := indexOf(out, old)
			if idx >= 0 {
				out = out[:idx] + new + out[idx+len(old):]
			}
		}
	}
	_ = replace
	// Substituições simples (sem strings.Replace para manter o helper curto).
	out = substitute(out, "MSG", msgID)
	out = substitute(out, "EXT", "ext-"+msgID)
	out = substitute(out, "KEY", "key-"+msgID)
	out = substitute(out, "KIND", kind)
	out = substitute(out, "AMOUNT", amount)
	return []byte(out)
}

func substitute(s, old, new string) string {
	for {
		i := indexOf(s, old)
		if i < 0 {
			return s
		}
		s = s[:i] + new + s[i+len(old):]
		return s
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func findInboxState(t *testing.T, pool *pgxpool.Pool, inbox *postgres.InboxRepository, msgID string) (*postgres.InboxMessage, error) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	return inbox.Find(ctx, dbTx, testConsumerName, msgID)
}

func cleanupInboxRow(t *testing.T, pool *pgxpool.Pool, msgID string) {
	t.Helper()

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
			testConsumerName, msgID)
	})
}

func completeInboxRow(t *testing.T, pool *pgxpool.Pool, inbox *postgres.InboxRepository, msgID string) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := inbox.Complete(ctx, dbTx, testConsumerName, msgID); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("complete: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func reserveInboxRow(t *testing.T, pool *pgxpool.Pool, inbox *postgres.InboxRepository, msgID string) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, _, err := inbox.Reserve(ctx, dbTx, testConsumerName, msgID, "hash-seed"); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("reserve: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestConsumer_InvalidMessageNoServiceNoAck(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, inbox := newTestConsumer(t, pool, proc, recv)

	if err := c.ProcessMessage(context.Background(), []byte(`{invalid`), "h-1"); err == nil {
		t.Fatal("expected error for malformed JSON")
	}
	if proc.numCalls() != 0 {
		t.Errorf("service calls = %d, want 0", proc.numCalls())
	}
	if recv.numDeletes() != 0 {
		t.Errorf("deletes = %d, want 0", recv.numDeletes())
	}
	_ = inbox
}

func TestConsumer_OpeningDoesNotReachService(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, _ := newTestConsumer(t, pool, proc, recv)

	raw := rawFor("msg-opening-1", "OPENING", "25.00")
	cleanupInboxRow(t, pool, "msg-opening-1")
	err := c.ProcessMessage(context.Background(), raw, "h-1")
	if !errors.Is(err, ErrOpeningNotAllowed) {
		t.Fatalf("error = %v, want ErrOpeningNotAllowed", err)
	}
	if proc.numCalls() != 0 {
		t.Errorf("service calls = %d, want 0 (OPENING barrado na borda)", proc.numCalls())
	}
	if recv.numDeletes() != 0 {
		t.Errorf("deletes = %d, want 0", recv.numDeletes())
	}
}

func TestConsumer_RefundWithoutReferenceIsStructural(t *testing.T) {
	// Parse aceita (referência opcional no envelope), mas ToTransaction/domínio
	// exige referência para REFUND: sem service, sem Complete, sem Delete.
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, _ := newTestConsumer(t, pool, proc, recv)

	raw := rawFor("msg-noref-1", "REFUND", "25.00")
	cleanupInboxRow(t, pool, "msg-noref-1")
	if err := c.ProcessMessage(context.Background(), raw, "h-1"); err == nil {
		t.Fatal("expected conversion error")
	}
	if proc.numCalls() != 0 {
		t.Errorf("service calls = %d, want 0", proc.numCalls())
	}
	if recv.numDeletes() != 0 {
		t.Errorf("deletes = %d, want 0", recv.numDeletes())
	}
}

func TestConsumer_FirstDeliveryProcessesCompletesDeletes(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, inbox := newTestConsumer(t, pool, proc, recv)

	msgID := "msg-first-1"
	cleanupInboxRow(t, pool, msgID)
	if err := c.ProcessMessage(context.Background(), rawFor(msgID, "BET", "25.00"), "h-1"); err != nil {
		t.Fatalf("process: %v", err)
	}
	if proc.numCalls() != 1 {
		t.Errorf("service calls = %d, want 1", proc.numCalls())
	}
	if recv.numDeletes() != 1 {
		t.Errorf("deletes = %d, want 1", recv.numDeletes())
	}
	got, err := findInboxState(t, pool, inbox, msgID)
	if err != nil {
		t.Fatalf("find inbox: %v", err)
	}
	if !got.IsCompleted() {
		t.Error("inbox not completed")
	}
}

func TestConsumer_AlreadyCompletedSkipsService(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, inbox := newTestConsumer(t, pool, proc, recv)

	msgID := "msg-done-1"
	cleanupInboxRow(t, pool, msgID)
	reserveInboxRow(t, pool, inbox, msgID)
	completeInboxRow(t, pool, inbox, msgID)

	if err := c.ProcessMessage(context.Background(), rawFor(msgID, "BET", "25.00"), "h-2"); err != nil {
		t.Fatalf("process: %v", err)
	}
	if proc.numCalls() != 0 {
		t.Errorf("service calls = %d, want 0 (já processada)", proc.numCalls())
	}
	if recv.numDeletes() != 1 {
		t.Errorf("deletes = %d, want 1 (ack do replay)", recv.numDeletes())
	}
}

func TestConsumer_IncompleteRedeliveryRecovers(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, inbox := newTestConsumer(t, pool, proc, recv)

	msgID := "msg-incomplete-1"
	cleanupInboxRow(t, pool, msgID)
	// Simula crash após Reserve e antes de Complete: linha existe, incompleta.
	reserveInboxRow(t, pool, inbox, msgID)

	if err := c.ProcessMessage(context.Background(), rawFor(msgID, "BET", "25.00"), "h-9"); err != nil {
		t.Fatalf("process: %v", err)
	}
	if proc.numCalls() != 1 {
		t.Errorf("service calls = %d, want 1 (recuperação permitida)", proc.numCalls())
	}
	if recv.numDeletes() != 1 {
		t.Errorf("deletes = %d, want 1", recv.numDeletes())
	}
	got, err := findInboxState(t, pool, inbox, msgID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !got.IsCompleted() {
		t.Error("inbox should be completed after recovery")
	}
}

func TestConsumer_AckMatrix(t *testing.T) {
	cases := []struct {
		name         string
		fn           func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error)
		wantCalls    int
		wantDeletes  int
		wantComplete bool
	}{
		{"processed", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return processedResult(), nil
		}, 1, 1, true},
		{"rejected replay", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return terminalResult(domain.TransactionRejected), nil
		}, 1, 1, true},
		{"failed replay", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return terminalResult(domain.TransactionFailed), nil
		}, 1, 1, true},
		{"rejected business error", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return nil, domain.ErrInsufficientFunds
		}, 1, 1, true},
		{"failed business error", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return nil, domain.ErrOverflow
		}, 1, 1, true},
		{"transient error", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return nil, errors.New("postgres unavailable")
		}, 1, 0, false},
		{"context canceled", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return nil, context.Canceled
		}, 1, 0, false},
		{"wallet not found", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return nil, domain.ErrWalletNotFound
		}, 1, 0, false},
		{"idempotency conflict", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return nil, domain.ErrIdempotencyConflict
		}, 1, 0, false},
		{"in progress", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return nil, application.ErrIdempotencyInProgress
		}, 1, 0, false},
		{"pending reference", func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
			return pendingRefResult(), nil
		}, 1, 0, false},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := consumerTestPool(t)
			proc := &fakeProcessor{fn: tc.fn}
			recv := &fakeReceiver{}
			c, inbox := newTestConsumer(t, pool, proc, recv)

			msgID := fmt.Sprintf("msg-ack-%d", i)
			cleanupInboxRow(t, pool, msgID)
			_ = c.ProcessMessage(context.Background(), rawFor(msgID, "BET", "25.00"), "h-1")

			if proc.numCalls() != tc.wantCalls {
				t.Errorf("service calls = %d, want %d", proc.numCalls(), tc.wantCalls)
			}
			if recv.numDeletes() != tc.wantDeletes {
				t.Errorf("deletes = %d, want %d", recv.numDeletes(), tc.wantDeletes)
			}
			got, err := findInboxState(t, pool, inbox, msgID)
			if err != nil {
				t.Fatalf("find inbox: %v", err)
			}
			if got.IsCompleted() != tc.wantComplete {
				t.Errorf("completed = %v, want %v", got.IsCompleted(), tc.wantComplete)
			}
		})
	}
}

func TestConsumer_CrashBetweenCompleteAndDelete(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{failDeletes: 1}
	c, inbox := newTestConsumer(t, pool, proc, recv)

	msgID := "msg-crash-1"
	cleanupInboxRow(t, pool, msgID)
	raw := rawFor(msgID, "BET", "25.00")

	// Primeira tentativa: Process + Complete OK, Delete falha (crash antes do ack).
	if err := c.ProcessMessage(context.Background(), raw, "h-crash-1"); err == nil {
		t.Fatal("expected delete error on first attempt")
	}
	if proc.numCalls() != 1 {
		t.Fatalf("service calls = %d, want 1", proc.numCalls())
	}
	got, err := findInboxState(t, pool, inbox, msgID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !got.IsCompleted() {
		t.Fatal("inbox should be completed even though delete failed")
	}
	if recv.numDeletes() != 0 {
		t.Fatalf("deletes = %d, want 0 (falhou)", recv.numDeletes())
	}

	// Redelivery: Reserve encontra concluída, NÃO reprocessa, apenas deleta.
	if err := c.ProcessMessage(context.Background(), raw, "h-crash-2"); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if proc.numCalls() != 1 {
		t.Errorf("service calls after redelivery = %d, want still 1 (sem duplo efeito)", proc.numCalls())
	}
	if recv.numDeletes() != 1 {
		t.Errorf("deletes = %d, want 1 (ack da redelivery)", recv.numDeletes())
	}
}

func TestConsumer_DoubleDeliverySingleEffect(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, _ := newTestConsumer(t, pool, proc, recv)

	msgID := "msg-double-1"
	cleanupInboxRow(t, pool, msgID)
	raw := rawFor(msgID, "BET", "25.00")

	if err := c.ProcessMessage(context.Background(), raw, "h-1"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := c.ProcessMessage(context.Background(), raw, "h-2"); err != nil {
		t.Fatalf("second: %v", err)
	}
	if proc.numCalls() != 1 {
		t.Errorf("service calls = %d, want 1 (Inbox é a autoridade)", proc.numCalls())
	}
	if recv.numDeletes() != 2 {
		t.Errorf("deletes = %d, want 2 (um ack por entrega)", recv.numDeletes())
	}
}

func TestConsumer_PollBatchContinuesAfterInvalid(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	msgID := "msg-batch-ok-1"
	cleanupInboxRow(t, pool, msgID)
	recv := &fakeReceiver{msgs: []ReceivedMessage{
		{ReceiptHandle: "h-bad", Body: []byte(`{invalid`)},
		{ReceiptHandle: "h-ok", Body: rawFor(msgID, "BET", "25.00")},
	}}
	c, _ := newTestConsumer(t, pool, proc, recv)

	if err := c.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if proc.numCalls() != 1 {
		t.Errorf("service calls = %d, want 1 (válida processada apesar da inválida)", proc.numCalls())
	}
	if recv.numDeletes() != 1 {
		t.Errorf("deletes = %d, want 1", recv.numDeletes())
	}
}

func TestConsumer_RunRespectsCancel(t *testing.T) {
	pool := consumerTestPool(t)
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, _ := newTestConsumer(t, pool, proc, recv)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}

	// Desabilitado: retorna sem polling e sem bloquear.
	off, err := NewConsumer(pool, postgres.NewInboxRepository(pool), proc, nil, testConsumerName, 10, 0, 30, false, nil)
	if err != nil {
		t.Fatalf("disabled consumer: %v", err)
	}
	if err := off.Run(context.Background()); err != nil {
		t.Fatalf("disabled Run: %v", err)
	}
}

func TestConsumer_IntegrationWithRealService(t *testing.T) {
	pool := consumerTestPool(t)
	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)
	service := application.NewWagerService(pool, wallets, transactions, ledger, outbox)
	recv := &fakeReceiver{}
	c, inbox := newTestConsumer(t, pool, service, recv)

	// Wallet real BRL 100.00.
	var walletID, playerID, providerID string
	if err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&walletID); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	if err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&playerID); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	if err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&providerID); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	wallet, err := domain.NewWallet(walletID, playerID, "BRL")
	if err != nil {
		t.Fatalf("new wallet: %v", err)
	}
	deposit, _ := domain.NewMoney(10000, "BRL")
	if err := wallet.Credit(deposit); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if err := wallets.Create(context.Background(), wallet); err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`, walletID)
		_, _ = pool.Exec(ctx, "TRUNCATE wallet_ledger_entries")
		_, _ = pool.Exec(ctx, "DELETE FROM wager_transactions WHERE wallet_id = $1", walletID)
		_, _ = pool.Exec(ctx, "DELETE FROM wallets WHERE id = $1", walletID)
	})

	msgID := "msg-integration-1"
	cleanupInboxRow(t, pool, msgID)
	raw := []byte(`{"messageId":"` + msgID + `","type":"WagerTransactionRequested",` +
		`"occurredAt":"2026-09-08T12:00:00.000Z",` +
		`"data":{"providerId":"` + providerID + `","externalTransactionId":"ext-` + msgID + `",` +
		`"idempotencyKey":"key-` + msgID + `",` +
		`"playerId":"` + playerID + `",` +
		`"walletId":"` + walletID + `",` +
		`"roundId":"round-1","gameId":"game-1","kind":"BET",` +
		`"money":{"amount":"30.00","currency":"BRL"}}}`)

	if err := c.ProcessMessage(context.Background(), raw, "h-int-1"); err != nil {
		t.Fatalf("process: %v", err)
	}

	stored, err := wallets.GetByID(context.Background(), walletID)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if stored.Balance.Cents() != 7000 {
		t.Errorf("balance = %d, want 7000", stored.Balance.Cents())
	}
	var nTx, nLedger int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1`, walletID).Scan(&nTx); err != nil {
		t.Fatalf("count tx: %v", err)
	}
	if nTx != 1 {
		t.Errorf("transactions = %d, want 1", nTx)
	}
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&nLedger); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if nLedger != 1 {
		t.Errorf("ledger entries = %d, want 1", nLedger)
	}
	got, err := findInboxState(t, pool, inbox, msgID)
	if err != nil {
		t.Fatalf("find inbox: %v", err)
	}
	if !got.IsCompleted() {
		t.Error("inbox not completed")
	}
	if recv.numDeletes() != 1 {
		t.Errorf("deletes = %d, want 1", recv.numDeletes())
	}

	// Redelivery da mesma mensagem: sem segundo efeito financeiro.
	if err := c.ProcessMessage(context.Background(), raw, "h-int-2"); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	stored, _ = wallets.GetByID(context.Background(), walletID)
	if stored.Balance.Cents() != 7000 {
		t.Errorf("balance after redelivery = %d, want still 7000", stored.Balance.Cents())
	}
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&nLedger); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if nLedger != 1 {
		t.Errorf("ledger entries after redelivery = %d, want 1", nLedger)
	}
	if recv.numDeletes() != 2 {
		t.Errorf("deletes = %d, want 2", recv.numDeletes())
	}
}

func TestConsumer_Metrics(t *testing.T) {
	pool := consumerTestPool(t)
	inbox := postgres.NewInboxRepository(pool)
	metrics := observability.NewMetrics()
	proc := &fakeProcessor{fn: func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return processedResult(), nil
	}}
	recv := &fakeReceiver{}
	c, err := NewConsumer(pool, inbox, proc, recv, testConsumerName, 10, 0, 30, true, metrics)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}

	// Duas mensagens válidas via pollOnce: received + acked.
	for _, id := range []string{"msg-met-1", "msg-met-2"} {
		cleanupInboxRow(t, pool, id)
	}
	recv.msgs = []ReceivedMessage{
		{ReceiptHandle: "h-m1", Body: rawFor("msg-met-1", "BET", "25.00")},
		{ReceiptHandle: "h-m2", Body: rawFor("msg-met-2", "BET", "25.00")},
	}
	if err := c.pollOnce(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if got := metrics.Value("sqs_messages_received_total", map[string]string{"queue": "wager"}); got != 2 {
		t.Errorf("received = %d, want 2", got)
	}
	if got := metrics.Value("sqs_messages_processed_total", map[string]string{"queue": "wager", "outcome": "acked"}); got != 2 {
		t.Errorf("acked = %d, want 2", got)
	}

	// Redelivery de mensagem concluída: duplicate, sem novo processamento.
	dupID := "msg-met-dup"
	cleanupInboxRow(t, pool, dupID)
	reserveInboxRow(t, pool, inbox, dupID)
	completeInboxRow(t, pool, inbox, dupID)
	if err := c.ProcessMessage(context.Background(), rawFor(dupID, "BET", "25.00"), "h-dup"); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if got := metrics.Value("sqs_messages_processed_total", map[string]string{"queue": "wager", "outcome": "duplicate"}); got != 1 {
		t.Errorf("duplicate = %d, want 1", got)
	}
	if proc.numCalls() != 2 {
		t.Errorf("service calls = %d, want 2 (duplicate não reprocessa)", proc.numCalls())
	}

	// Parse inválido e erro transitório: failed por estágio.
	if err := c.ProcessMessage(context.Background(), []byte(`{invalid`), "h-bad"); err == nil {
		t.Fatal("expected parse error")
	}
	proc.fn = func(context.Context, application.ProcessWagerInput) (*application.ProcessWagerResult, error) {
		return nil, errors.New("db down")
	}
	transientID := "msg-met-transient"
	cleanupInboxRow(t, pool, transientID)
	if err := c.ProcessMessage(context.Background(), rawFor(transientID, "BET", "25.00"), "h-t"); err == nil {
		t.Fatal("expected transient error")
	}
	if got := metrics.Value("sqs_messages_failed_total", map[string]string{"queue": "wager", "stage": "parse"}); got != 1 {
		t.Errorf("failed parse = %d, want 1", got)
	}
	if got := metrics.Value("sqs_messages_failed_total", map[string]string{"queue": "wager", "stage": "process"}); got != 1 {
		t.Errorf("failed process = %d, want 1", got)
	}

	// Exposição sem IDs de negócio (baixa cardinalidade por construção).
	var b strings.Builder
	metrics.WritePrometheus(&b)
	for _, leak := range []string{"msg-met-", "tx-1", "provider-a", "h-m1"} {
		if strings.Contains(b.String(), leak) {
			t.Errorf("metrics leak %q", leak)
		}
	}
}
