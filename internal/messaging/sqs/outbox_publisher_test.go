package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// fakeSender captura bodies sem AWS. failOn marca os números de chamada
// (1-indexed) que devem falhar.
type fakeSender struct {
	mu     sync.Mutex
	bodies [][]byte
	failOn map[int]bool
	err    error
}

func (f *fakeSender) SendMessage(_ context.Context, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]byte(nil), body...)
	f.bodies = append(f.bodies, cp)
	if f.failOn[len(f.bodies)] {
		if f.err != nil {
			return f.err
		}
		return errors.New("sqs send failed")
	}
	return nil
}

func (f *fakeSender) sentBodies() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.bodies...)
}

// stubOutboxStore é o OutboxStore em memória: hermético (sem banco) para os
// testes unitários do publisher. markFails faz as próximas N marcações
// falharem, simulando crash entre SendMessage e MarkPublished. Filtra
// elegibilidade como o repository real (publicado, descartado e backoff
// futuro não são listados).
type stubOutboxStore struct {
	mu        sync.Mutex
	rows      []*postgres.OutboxEvent
	published map[string]bool
	dead      map[string]bool
	attempts  map[string]int32
	next      map[string]time.Time
	markCalls []string
	markFails int
	markErr   error
	listErr   error
}

func newStubOutboxStore(rows ...*postgres.OutboxEvent) *stubOutboxStore {
	return &stubOutboxStore{rows: rows, published: map[string]bool{}, dead: map[string]bool{}, attempts: map[string]int32{}, next: map[string]time.Time{}}
}

func stubRow(id, eventType, body string) *postgres.OutboxEvent {
	return &postgres.OutboxEvent{
		ID:          id,
		EventType:   eventType,
		AggregateID: "agg-" + id,
		Payload:     []byte(body),
		OccurredAt:  time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC),
	}
}

func (s *stubOutboxStore) ListPending(_ context.Context, _ pgx.Tx, limit int) ([]*postgres.OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	now := time.Now()
	var out []*postgres.OutboxEvent
	for _, r := range s.rows {
		if len(out) >= limit {
			break
		}
		if s.published[r.ID] || s.dead[r.ID] {
			continue
		}
		if next, ok := s.next[r.ID]; ok && now.Before(next) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (s *stubOutboxStore) MarkPublished(_ context.Context, _ pgx.Tx, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markCalls = append(s.markCalls, id)
	if s.markFails > 0 {
		s.markFails--
		if s.markErr != nil {
			return s.markErr
		}
		return errors.New("outbox mark failed")
	}
	found := false
	for _, r := range s.rows {
		if r.ID == id {
			found = true
		}
	}
	if !found {
		return postgres.ErrOutboxEventNotFound
	}
	s.published[id] = true
	return nil
}

func (s *stubOutboxStore) RecordAttemptFailure(_ context.Context, _ pgx.Tx, id string, nextAttemptAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for _, r := range s.rows {
		if r.ID == id {
			found = true
		}
	}
	if !found {
		return postgres.ErrOutboxEventNotFound
	}
	s.attempts[id]++
	s.next[id] = nextAttemptAt
	return nil
}

func (s *stubOutboxStore) MarkDeadLettered(_ context.Context, _ pgx.Tx, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for _, r := range s.rows {
		if r.ID == id {
			found = true
		}
	}
	if !found {
		return postgres.ErrOutboxEventNotFound
	}
	s.dead[id] = true
	return nil
}

func (s *stubOutboxStore) isPublished(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.published[id]
}

func (s *stubOutboxStore) isDead(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead[id]
}

func (s *stubOutboxStore) getAttempts(id string) int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[id]
}

func newTestPublisher(t *testing.T, store OutboxStore, sender Sender) *Publisher {
	t.Helper()

	p, err := NewPublisher(consumerTestPool(t), store, sender, nil, 10, 50*time.Millisecond, 5, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	return p
}

func TestPublisher_Success(t *testing.T) {
	store := newStubOutboxStore(
		stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`),
		stubRow("e2", "WalletBalanceChanged", `{"eventId":"e2"}`),
	)
	sender := &fakeSender{}
	p := newTestPublisher(t, store, sender)

	n, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n != 2 {
		t.Fatalf("published = %d, want 2", n)
	}
	bodies := sender.sentBodies()
	if len(bodies) != 2 || string(bodies[0]) != `{"eventId":"e1"}` || string(bodies[1]) != `{"eventId":"e2"}` {
		t.Errorf("bodies = %q, want payloads na ordem e byte a byte", bodies)
	}
	if !store.isPublished("e1") || !store.isPublished("e2") {
		t.Error("events not marked published")
	}
	// Nada mais pendente: segunda chamada não envia.
	n, err = p.PublishOnce(context.Background())
	if err != nil || n != 0 {
		t.Errorf("second publish = (%d, %v), want (0, nil)", n, err)
	}
	if len(sender.sentBodies()) != 2 {
		t.Error("resent already-published events")
	}
}

func TestPublisher_EmptyPending(t *testing.T) {
	p := newTestPublisher(t, newStubOutboxStore(), &fakeSender{})

	n, err := p.PublishOnce(context.Background())
	if err != nil || n != 0 {
		t.Errorf("publish = (%d, %v), want (0, nil)", n, err)
	}
}

func TestPublisher_SendFailureLeavesPending(t *testing.T) {
	store := newStubOutboxStore(
		stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`),
		stubRow("e2", "WalletBalanceChanged", `{"eventId":"e2"}`),
		stubRow("e3", "WagerTransactionProcessed", `{"eventId":"e3"}`),
	)
	sender := &fakeSender{failOn: map[int]bool{2: true}, err: errors.New("sqs down")}
	p := newTestPublisher(t, store, sender)

	// Política desta etapa: interrompe o batch no primeiro erro.
	n, err := p.PublishOnce(context.Background())
	if err == nil {
		t.Fatal("expected send error")
	}
	if n != 1 {
		t.Errorf("published = %d, want 1 (só o anterior ao erro)", n)
	}
	if !store.isPublished("e1") || store.isPublished("e2") || store.isPublished("e3") {
		t.Error("mark state wrong: e1 published, e2/e3 pending")
	}
	if len(sender.sentBodies()) != 2 {
		t.Errorf("sends = %d, want 2 tentativas (a 2ª falhou)", len(sender.sentBodies()))
	}
}

func TestPublisher_MarkFailureReturnsError(t *testing.T) {
	// SendMessage OK + MarkPublished ERROR: o publisher retorna erro e o
	// evento continua pendente (janela de crash real; sem "despublicar").
	store := newStubOutboxStore(stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`))
	store.markFails = 1
	store.markErr = errors.New("db down")
	sender := &fakeSender{}
	p := newTestPublisher(t, store, sender)

	n, err := p.PublishOnce(context.Background())
	if err == nil {
		t.Fatal("expected mark error")
	}
	if n != 0 {
		t.Errorf("published = %d, want 0", n)
	}
	if len(sender.sentBodies()) != 1 {
		t.Errorf("sends = %d, want 1 (enviou, mas não marcou)", len(sender.sentBodies()))
	}
	if store.isPublished("e1") {
		t.Error("event marked published despite mark failure")
	}
}

func TestPublisher_CrashBetweenSendAndMarkResendsIdentical(t *testing.T) {
	// Tentativa 1: Send OK, Mark falha (crash). Tentativa 2: reenvia o MESMO
	// payload com a MESMA identidade — at-least-once, sem UUID novo.
	store := newStubOutboxStore(stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`))
	store.markFails = 1
	sender := &fakeSender{}
	p := newTestPublisher(t, store, sender)

	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected mark error on first attempt")
	}
	n, err := p.PublishOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("second publish = (%d, %v), want (1, nil)", n, err)
	}
	bodies := sender.sentBodies()
	if len(bodies) != 2 || string(bodies[0]) != string(bodies[1]) {
		t.Errorf("bodies = %q, want o mesmo payload duas vezes", bodies)
	}
	if !store.isPublished("e1") {
		t.Error("event not published after redelivery")
	}
}

func TestPublisher_FetchFailure(t *testing.T) {
	store := newStubOutboxStore()
	store.listErr = errors.New("db down")
	p := newTestPublisher(t, store, &fakeSender{})

	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected fetch error")
	}
}

func TestPublisher_RespectsCancellation(t *testing.T) {
	p := newTestPublisher(t, newStubOutboxStore(stubRow("e1", "T", `{}`)), &fakeSender{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.PublishOnce(ctx); err == nil {
		t.Fatal("expected context error")
	}

	// Run desligado e cancelado retornam sem vazar goroutine.
	off, err := NewPublisher(consumerTestPool(t), newStubOutboxStore(), nil, nil, 10, time.Millisecond, 5, 5*time.Minute, false, nil)
	if err != nil {
		t.Fatalf("disabled publisher: %v", err)
	}
	if err := off.Run(context.Background()); err != nil {
		t.Fatalf("disabled Run: %v", err)
	}
	if err := p.Run(ctx); err == nil {
		t.Fatal("expected context error from Run")
	}
}

func TestPublisher_RunPublishesThenStops(t *testing.T) {
	store := newStubOutboxStore(stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`))
	p := newTestPublisher(t, store, &fakeSender{})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := p.Run(ctx); err == nil {
		t.Fatal("expected timeout error from Run")
	}
	if !store.isPublished("e1") {
		t.Error("event not published by Run")
	}
}

// newPublisherUUID pede um UUID ao PostgreSQL (colunas id/aggregate_id são
// UUID no schema).
func newPublisherUUID(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	var id string
	if err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id
}

// commitOutboxRow insere uma linha pendente via repository real e registra a
// limpeza por id.
func commitOutboxRow(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, id, eventType, agg, body string, at time.Time) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	ev := &postgres.OutboxEvent{ID: id, EventType: eventType, AggregateID: agg, Payload: []byte(body), OccurredAt: at}
	if err := repo.Create(ctx, dbTx, ev); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("create: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE id = $1`, id)
	})
}

func isOutboxPublished(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, id string) bool {
	t.Helper()

	_ = repo
	var published bool
	if err := pool.QueryRow(context.Background(),
		`SELECT published_at IS NOT NULL FROM outbox_events WHERE id = $1`, id).Scan(&published); err != nil {
		t.Fatalf("check published: %v", err)
	}
	return published
}

func TestPublisher_RealStoreRoundTrip(t *testing.T) {
	// Round trip com repository real, com escopo nas linhas próprias: o SQL
	// real é exercitado sem alcançar linhas de outros pacotes em paralelo.
	pool := consumerTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	sender := &fakeSender{}

	// 2020: ordena antes de qualquer linha de outros testes (sem empates no
	// ORDER BY que poderiam cortar minhas linhas do LIMIT).
	base := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	type seed struct {
		id, eventType, body string
		at                  time.Time
	}
	seeds := []seed{
		{id: newPublisherUUID(t, pool), eventType: "WagerTransactionProcessed", body: `{"eventId":"r1"}`, at: base},
		{id: newPublisherUUID(t, pool), eventType: "WalletBalanceChanged", body: `{"eventId":"r2"}`, at: base.Add(time.Second)},
		{id: newPublisherUUID(t, pool), eventType: "WagerTransactionRejected", body: `{"eventId":"r3"}`, at: base.Add(2 * time.Second)},
	}
	for _, s := range seeds {
		// Agregado distinto por linha: a UNIQUE (aggregate, type) da 003
		// exige tipos distintos por agregado.
		commitOutboxRow(t, pool, repo, s.id, s.eventType, newPublisherUUID(t, pool), s.body, s.at)
	}
	scoped := newScopedOutboxStore(repo, seeds[0].id, seeds[1].id, seeds[2].id)
	p, err := NewPublisher(pool, scoped, sender, nil, 100, 50*time.Millisecond, 5, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	n, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n != len(seeds) {
		t.Fatalf("published = %d, want %d", n, len(seeds))
	}
	// O JSONB normaliza o texto ao persistir: o esperado é o payload LIDO
	// do banco (o publisher envia exatamente o persistido, byte a byte).
	want := make([]string, 0, len(seeds))
	for _, s := range seeds {
		want = append(want, string(readOutboxPayload(t, pool, s.id)))
	}
	bodies := sender.sentBodies()
	idx := map[string]int{}
	for i, b := range bodies {
		idx[string(b)] = i
	}
	prev := -1
	for _, w := range want {
		i, ok := idx[w]
		if !ok {
			t.Fatalf("body %q não enviado", w)
		}
		if i < prev {
			t.Fatalf("ordem violada para %q", w)
		}
		prev = i
	}
	for _, s := range seeds {
		if !isOutboxPublished(t, pool, repo, s.id) {
			t.Errorf("row %s não marcada como publicada", s.id)
		}
	}
}

func readOutboxPayload(t *testing.T, pool *pgxpool.Pool, id string) []byte {
	t.Helper()

	var payload []byte
	if err := pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox_events WHERE id = $1`, id).Scan(&payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return payload
}

// scopedOutboxStore restringe o OutboxRepository real às linhas do teste (por
// id). O banco é compartilhado entre pacotes de teste em paralelo: um
// PublishOnce global buscaria (e marcaria!) linhas alheias em voo, quebrando
// testes alheios (ex. assertUnpublished) e as próprias asserções. Com o
// escopo, o publisher nunca vê, envia ou marca linhas de outros testes, e o
// SQL real (elegibilidade, ordem, updates atômicos) continua exercitado para
// as linhas próprias.
type scopedOutboxStore struct {
	real  *postgres.OutboxRepository
	allow map[string]bool
}

func newScopedOutboxStore(real *postgres.OutboxRepository, ids ...string) *scopedOutboxStore {
	allow := map[string]bool{}
	for _, id := range ids {
		allow[id] = true
	}
	return &scopedOutboxStore{real: real, allow: allow}
}

func (s *scopedOutboxStore) ListPending(ctx context.Context, tx pgx.Tx, limit int) ([]*postgres.OutboxEvent, error) {
	ids := make([]string, 0, len(s.allow))
	for id := range s.allow {
		ids = append(ids, id)
	}
	rows, err := tx.Query(ctx, `
		SELECT id, event_type, aggregate_id, payload, occurred_at, published_at,
		       attempts, next_attempt_at, dead_lettered_at
		FROM outbox_events
		WHERE id = ANY($1::uuid[])
		  AND published_at IS NULL
		  AND dead_lettered_at IS NULL
		  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		ORDER BY next_attempt_at ASC NULLS FIRST, occurred_at, event_type
		LIMIT $2
	`, ids, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*postgres.OutboxEvent
	for rows.Next() {
		var (
			id             string
			eventType      string
			aggregateID    string
			payload        []byte
			occurredAt     time.Time
			publishedAt    *time.Time
			attempts       int32
			nextAttemptAt  *time.Time
			deadLetteredAt *time.Time
		)
		if err := rows.Scan(&id, &eventType, &aggregateID, &payload, &occurredAt, &publishedAt, &attempts, &nextAttemptAt, &deadLetteredAt); err != nil {
			return nil, err
		}
		out = append(out, &postgres.OutboxEvent{
			ID: id, EventType: eventType, AggregateID: aggregateID,
			Payload: payload, OccurredAt: occurredAt, PublishedAt: publishedAt,
			Attempts: attempts, NextAttemptAt: nextAttemptAt, DeadLetteredAt: deadLetteredAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *scopedOutboxStore) MarkPublished(ctx context.Context, tx pgx.Tx, id string) error {
	return s.real.MarkPublished(ctx, tx, id)
}

func (s *scopedOutboxStore) RecordAttemptFailure(ctx context.Context, tx pgx.Tx, id string, nextAttemptAt time.Time) error {
	return s.real.RecordAttemptFailure(ctx, tx, id, nextAttemptAt)
}

func (s *scopedOutboxStore) MarkDeadLettered(ctx context.Context, tx pgx.Tx, id string) error {
	return s.real.MarkDeadLettered(ctx, tx, id)
}

func TestPublisher_TwoPublishersRaceSameOutbox(t *testing.T) {
	// §13.6 com store real: dois publishers disputam as mesmas linhas.
	// At-least-once permite envios duplicados, mas nenhum evento pode se
	// perder e todo corpo enviado deve ser byte a byte igual ao persistido.
	pool := consumerTestPool(t)
	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	repo := postgres.NewOutboxRepository(pool)
	service := application.NewWagerService(pool, wallets, transactions, ledger, repo)

	// Hermeticidade: o store real lista toda a tabela compartilhada; restos
	// de execuções abortadas não podem vazar para esta disputa.
	if _, err := pool.Exec(context.Background(), `TRUNCATE outbox_events`); err != nil {
		t.Fatalf("truncate outbox: %v", err)
	}

	walletID, playerID := newPublisherUUID(t, pool), newPublisherUUID(t, pool)
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

	providerID := newPublisherUUID(t, pool)
	amount, _ := domain.NewMoney(3000, "BRL")
	wt, err := domain.NewWagerTransaction(
		newPublisherUUID(t, pool), providerID, "ext-"+newPublisherUUID(t, pool),
		"key-"+newPublisherUUID(t, pool), "hash-test",
		playerID, walletID, "round-1", "game-1",
		domain.TransactionBet, amount, "",
	)
	if err != nil {
		t.Fatalf("new transaction: %v", err)
	}
	if _, err := service.Process(context.Background(), application.ProcessWagerInput{Transaction: *wt}); err != nil {
		t.Fatalf("process: %v", err)
	}
	stored := readOutboxAggregate(t, pool, repo, wt.ID)
	if len(stored) != 2 {
		t.Fatalf("outbox rows = %d, want 2", len(stored))
	}
	wantBody := map[string]string{}
	for _, ev := range stored {
		wantBody[ev.ID] = string(ev.Payload)
	}
	// O store real lista TODAS as linhas pendentes da tabela compartilhada;
	// valida-se abaixo somente os eventos deste teste (por eventId).

	senderA, senderB := &fakeSender{}, &fakeSender{}
	pubA, err := NewPublisher(pool, repo, senderA, nil, 10, 50*time.Millisecond, 5, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("publisher A: %v", err)
	}
	pubB, err := NewPublisher(pool, repo, senderB, nil, 10, 50*time.Millisecond, 5, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("publisher B: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		for _, p := range []*Publisher{pubA, pubB} {
			wg.Add(1)
			go func(p *Publisher) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					n, err := p.PublishOnce(context.Background())
					if err != nil {
						return
					}
					if n == 0 {
						return
					}
				}
			}(p)
		}
	}
	wg.Wait()

	for _, ev := range readOutboxAggregate(t, pool, repo, wt.ID) {
		var publishedAt *time.Time
		err := pool.QueryRow(context.Background(),
			`SELECT published_at FROM outbox_events WHERE id = $1`, ev.ID,
		).Scan(&publishedAt)
		if err != nil {
			t.Fatalf("published_at: %v", err)
		}
		if publishedAt == nil {
			t.Errorf("event %s %s not published after race", ev.ID, ev.EventType)
		}
	}
	sawMine := map[string]int{}
	for i, bodies := range [][][]byte{senderA.sentBodies(), senderB.sentBodies()} {
		for _, body := range bodies {
			var env map[string]any
			if err := json.Unmarshal(body, &env); err != nil {
				t.Fatalf("sender %d body not JSON: %v", i, err)
			}
			id, _ := env["eventId"].(string)
			want, ok := wantBody[id]
			if !ok {
				continue // linha de outro teste na tabela compartilhada
			}
			sawMine[id]++
			if string(body) != want {
				t.Errorf("sender %d body for %q differs from stored payload", i, id)
			}
		}
	}
	for id := range wantBody {
		if sawMine[id] == 0 {
			t.Errorf("event %q never sent", id)
		}
	}
}

func TestPublisher_IntegrationWithWagerService(t *testing.T) {
	// Composição com PostgreSQL real:
	//   ProcessWager → outbox rows → publisher → fake SQS → MarkPublished.
	// O publisher roda sobre cópias em stub (hermético); as linhas reais do
	// banco são lidas para comparação byte a byte e limpas ao fim.
	pool := consumerTestPool(t)
	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	repo := postgres.NewOutboxRepository(pool)
	service := application.NewWagerService(pool, wallets, transactions, ledger, repo)

	walletID, playerID := newPublisherUUID(t, pool), newPublisherUUID(t, pool)
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

	providerID := newPublisherUUID(t, pool)
	amount, _ := domain.NewMoney(3000, "BRL")
	wt, err := domain.NewWagerTransaction(
		newPublisherUUID(t, pool), providerID, "ext-"+newPublisherUUID(t, pool),
		"key-"+newPublisherUUID(t, pool), "hash-test",
		playerID, walletID, "round-1", "game-1",
		domain.TransactionBet, amount, "",
	)
	if err != nil {
		t.Fatalf("new transaction: %v", err)
	}
	res, err := service.Process(context.Background(), application.ProcessWagerInput{Transaction: *wt})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !res.Transaction.IsProcessed() {
		t.Fatalf("status = %q, want PROCESSED", res.Transaction.Status)
	}

	// Lê as linhas reais e alimenta o stub com cópias fiéis.
	stored := readOutboxAggregate(t, pool, repo, wt.ID)
	if len(stored) != 2 {
		t.Fatalf("outbox rows = %d, want 2 (decisão + saldo)", len(stored))
	}
	copies := make([]*postgres.OutboxEvent, 0, len(stored))
	for _, ev := range stored {
		copies = append(copies, &postgres.OutboxEvent{
			ID: ev.ID, EventType: ev.EventType, AggregateID: ev.AggregateID,
			Payload: append([]byte(nil), ev.Payload...), OccurredAt: ev.OccurredAt,
		})
	}
	store := newStubOutboxStore(copies...)
	sender := &fakeSender{}
	p, err := NewPublisher(pool, store, sender, nil, 10, 50*time.Millisecond, 5, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	n, err := p.PublishOnce(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("publish = (%d, %v), want (2, nil)", n, err)
	}
	// Payload enviado é o persistido, byte a byte; envelope íntegro.
	bodies := sender.sentBodies()
	if len(bodies) != 2 {
		t.Fatalf("sends = %d, want 2", len(bodies))
	}
	for i := range bodies {
		if string(bodies[i]) != string(stored[i].Payload) {
			t.Errorf("body %d difere do payload persistido", i)
		}
	}
	var envelope map[string]any
	if err := json.Unmarshal(bodies[0], &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	// eventId é único por linha (uma transação emite vários tipos ao longo
	// da vida, ex. PENDING_REFERENCE → PROCESSED); a identidade estável é
	// (aggregate_id, event_type).
	if envelope["eventType"] != domain.EventWagerTransactionProcessed || envelope["aggregateId"] != wt.ID {
		t.Errorf("envelope = %v, want decisão da transação %s", envelope, wt.ID)
	}
	if eventID, _ := envelope["eventId"].(string); eventID == "" || eventID == wt.ID {
		t.Errorf("envelope eventId = %q, want unique non-transaction id", eventID)
	}

	// Replay não cria segundo evento: continua 2 linhas no banco.
	replay, err := service.Process(context.Background(), application.ProcessWagerInput{Transaction: *wt})
	if err != nil || !replay.Replayed {
		t.Fatalf("replay = (%+v, %v), want replay sem erro", replay, err)
	}
	if again := readOutboxAggregate(t, pool, repo, wt.ID); len(again) != 2 {
		t.Errorf("outbox rows after replay = %d, want 2", len(again))
	}
}

func readOutboxAggregate(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, aggregateID string) []*postgres.OutboxEvent {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	events, err := repo.ListByAggregate(ctx, dbTx, aggregateID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return events
}

// setupProcessedBet processa um BET 30.00 sobre wallet 100.00 via service
// real e devolve pool/service/repo + IDs, com limpeza completa (outbox,
// ledger, transactions, wallet). As 2 linhas da outbox nascem pendentes.
func setupProcessedBet(t *testing.T) (*pgxpool.Pool, *application.WagerService, *postgres.OutboxRepository, string, string) {
	t.Helper()

	pool := consumerTestPool(t)
	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	repo := postgres.NewOutboxRepository(pool)
	service := application.NewWagerService(pool, wallets, transactions, ledger, repo)

	walletID, playerID := newPublisherUUID(t, pool), newPublisherUUID(t, pool)
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

	providerID := newPublisherUUID(t, pool)
	amount, _ := domain.NewMoney(3000, "BRL")
	wt, err := domain.NewWagerTransaction(
		newPublisherUUID(t, pool), providerID, "ext-"+newPublisherUUID(t, pool),
		"key-"+newPublisherUUID(t, pool), "hash-test",
		playerID, walletID, "round-1", "game-1",
		domain.TransactionBet, amount, "",
	)
	if err != nil {
		t.Fatalf("new transaction: %v", err)
	}
	res, err := service.Process(context.Background(), application.ProcessWagerInput{Transaction: *wt})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !res.Transaction.IsProcessed() {
		t.Fatalf("status = %q, want PROCESSED", res.Transaction.Status)
	}
	return pool, service, repo, walletID, wt.ID
}

// makeOutboxDueNow simula a passagem do tempo sem sleeps: traz o backoff das
// linhas da wallet para o passado, tornando-as elegíveis de novo.
func makeOutboxDueNow(t *testing.T, pool *pgxpool.Pool, walletID string) {
	t.Helper()

	_, err := pool.Exec(context.Background(),
		`UPDATE outbox_events SET next_attempt_at = now() - interval '1 second'
		  WHERE published_at IS NULL AND dead_lettered_at IS NULL
		    AND aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`,
		walletID)
	if err != nil {
		t.Fatalf("time travel: %v", err)
	}
}

func readOutboxAttempts(t *testing.T, pool *pgxpool.Pool, txID string) map[string]int32 {
	t.Helper()

	rows, err := pool.Query(context.Background(),
		`SELECT id, attempts FROM outbox_events WHERE aggregate_id = $1`, txID)
	if err != nil {
		t.Fatalf("query attempts: %v", err)
	}
	defer rows.Close()
	out := map[string]int32{}
	for rows.Next() {
		var id string
		var attempts int32
		if err := rows.Scan(&id, &attempts); err != nil {
			t.Fatalf("scan attempts: %v", err)
		}
		out[id] = attempts
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate attempts: %v", err)
	}
	return out
}

func TestPublisher_IntegrationRetryThenPublishes(t *testing.T) {
	// ProcessWager → outbox rows → sender falha 2x → attempts/backoff →
	// (time-travel, sem sleep) → eventual published_at, payload idêntico.
	// Com escopo nas linhas próprias: hermético no banco compartilhado.
	pool, _, repo, walletID, txID := setupProcessedBet(t)
	sender := &fakeSender{failOn: map[int]bool{1: true, 2: true}, err: errors.New("sqs down")}

	ids := []string{}
	wantBodies := map[string]string{}
	for _, ev := range readOutboxAggregate(t, pool, repo, txID) {
		ids = append(ids, ev.ID)
		wantBodies[ev.ID] = string(ev.Payload)
	}
	if len(wantBodies) != 2 {
		t.Fatalf("outbox rows = %d, want 2", len(wantBodies))
	}
	scoped := newScopedOutboxStore(repo, ids...)
	p, err := NewPublisher(pool, scoped, sender, nil, 10, 50*time.Millisecond, 5, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	// Chamada 1: primeira linha falha no envio (attempts 1), batch para.
	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected send error")
	}
	// Chamada 2: primeira linha em backoff é pulada; segunda falha
	// (attempts 1), batch para.
	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected send error")
	}
	for id, attempts := range readOutboxAttempts(t, pool, txID) {
		if attempts != 1 {
			t.Errorf("attempts[%s] = %d, want 1", id, attempts)
		}
		if isOutboxPublished(t, pool, repo, id) {
			t.Errorf("row %s published after failure", id)
		}
	}
	// Chamada 3: ambas em backoff → (0, nil), sem novos envios.
	if n, err := p.PublishOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("publish = (%d, %v), want (0, nil) com backoff pendente", n, err)
	}
	if len(sender.sentBodies()) != 2 {
		t.Fatalf("sends = %d, want 2", len(sender.sentBodies()))
	}

	// Backoff vencido (time-travel): ambas publicam com payload idêntico.
	makeOutboxDueNow(t, pool, walletID)
	sentBefore := len(sender.sentBodies())
	for i := 0; i < 4; i++ {
		if n, err := p.PublishOnce(context.Background()); err != nil {
			t.Fatalf("publish after due: %v", err)
		} else if n == 2 {
			break
		}
		makeOutboxDueNow(t, pool, walletID)
		if i == 3 {
			t.Fatal("rows never published after backoff")
		}
	}
	newBodies := sender.sentBodies()[sentBefore:]
	if len(newBodies) != 2 {
		t.Fatalf("retry sends = %d, want 2", len(newBodies))
	}
	for _, b := range newBodies {
		var env map[string]any
		if err := json.Unmarshal(b, &env); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		id, _ := env["eventId"].(string)
		if wantBodies[id] != string(b) {
			t.Errorf("body for %s difere do payload persistido", id)
		}
		if !isOutboxPublished(t, pool, repo, id) {
			t.Errorf("row %s not published", id)
		}
	}
}

func TestPublisher_IntegrationExhaustedToDLQ(t *testing.T) {
	// Falha persistente até o limite: corpo idêntico na DLQ, dead_lettered_at
	// preenchido, published_at NULL. Sem sleeps (time-travel entre ciclos).
	// Com escopo nas linhas próprias: hermético no banco compartilhado.
	pool, _, repo, walletID, txID := setupProcessedBet(t)
	sender := &fakeSender{failOn: map[int]bool{1: true, 2: true, 3: true, 4: true}, err: errors.New("sqs down")}
	dlq := &fakeSender{}

	ids := []string{}
	wantBodies := map[string]string{}
	for _, ev := range readOutboxAggregate(t, pool, repo, txID) {
		ids = append(ids, ev.ID)
		wantBodies[ev.ID] = string(ev.Payload)
	}
	scoped := newScopedOutboxStore(repo, ids...)
	p, err := NewPublisher(pool, scoped, sender, dlq, 10, 50*time.Millisecond, 2, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, _ = p.PublishOnce(context.Background())
		makeOutboxDueNow(t, pool, walletID)
		done := true
		for id := range wantBodies {
			var dead, published bool
			if err := pool.QueryRow(context.Background(),
				`SELECT dead_lettered_at IS NOT NULL, published_at IS NOT NULL
				  FROM outbox_events WHERE id = $1`, id).Scan(&dead, &published); err != nil {
				t.Fatalf("check state: %v", err)
			}
			if !dead || published {
				done = false
			}
		}
		if done {
			break
		}
		if i == 9 {
			t.Fatal("rows never dead-lettered")
		}
	}

	dlqBodies := dlq.sentBodies()
	if len(dlqBodies) != 2 {
		t.Fatalf("dlq sends = %d, want 2 (uma por linha)", len(dlqBodies))
	}
	seen := map[string]bool{}
	for _, b := range dlqBodies {
		var env map[string]any
		if err := json.Unmarshal(b, &env); err != nil {
			t.Fatalf("unmarshal dlq body: %v", err)
		}
		id, _ := env["eventId"].(string)
		if wantBodies[id] != string(b) {
			t.Errorf("DLQ body for %s difere do payload persistido", id)
		}
		seen[id] = true
	}
	if len(seen) != 2 {
		t.Errorf("dlq cobre %d linhas, want 2 distintas", len(seen))
	}
}

func TestComputeOutboxBackoff(t *testing.T) {
	max := 5 * time.Minute
	for _, tc := range []struct {
		attempt int32
		want    time.Duration
	}{
		{0, time.Second}, // defensivo: trata como primeira tentativa
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{9, 256 * time.Second},
		{10, max}, // 512s satura no teto de 300s
		{100, max},
	} {
		t.Run("", func(t *testing.T) {
			if got := ComputeOutboxBackoff(tc.attempt, max); got != tc.want {
				t.Errorf("backoff(%d) = %v, want %v", tc.attempt, got, tc.want)
			}
		})
	}
	if got := ComputeOutboxBackoff(3, 3*time.Second); got != 3*time.Second {
		t.Errorf("teto baixo: got %v, want 3s", got)
	}
	if got := ComputeOutboxBackoff(1, 0); got != time.Second {
		t.Errorf("max inválido: got %v, want 1s", got)
	}
}

func newRetryTestPublisher(t *testing.T, store OutboxStore, sender, dlq Sender, maxAttempts int32) *Publisher {
	t.Helper()

	p, err := NewPublisher(consumerTestPool(t), store, sender, dlq, 10, 50*time.Millisecond, maxAttempts, 5*time.Minute, true, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	return p
}

func TestPublisher_SendFailureRecordsAttempt(t *testing.T) {
	// SendMessage falha → attempts 1, published_at NULL, próxima tentativa
	// no futuro (sem sleep: o stub registra o agendamento).
	store := newStubOutboxStore(stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`))
	sender := &fakeSender{failOn: map[int]bool{1: true}, err: errors.New("sqs down")}
	before := time.Now()
	p := newRetryTestPublisher(t, store, sender, nil, 5)

	n, err := p.PublishOnce(context.Background())
	if err == nil {
		t.Fatal("expected send error")
	}
	if n != 0 {
		t.Errorf("published = %d, want 0", n)
	}
	if store.getAttempts("e1") != 1 {
		t.Errorf("attempts = %d, want 1", store.getAttempts("e1"))
	}
	if store.isPublished("e1") {
		t.Error("failed send marked published")
	}
	next := store.next["e1"]
	if next.Before(before.Add(time.Second)) || next.After(time.Now().Add(2*time.Second)) {
		t.Errorf("next_attempt_at = %v, want ~1s de backoff", next)
	}
	// Backoff futuro: segunda chamada não reenvia (sem sleep).
	n, err = p.PublishOnce(context.Background())
	if err != nil || n != 0 {
		t.Errorf("second publish = (%d, %v), want (0, nil) com backoff pendente", n, err)
	}
	if len(sender.sentBodies()) != 1 {
		t.Errorf("sends = %d, want 1 (sem retry imediato)", len(sender.sentBodies()))
	}
}

func TestPublisher_RetryAfterDue(t *testing.T) {
	// Após o backoff vencer, o mesmo payload com o mesmo eventId é reenviado.
	store := newStubOutboxStore(stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`))
	sender := &fakeSender{failOn: map[int]bool{1: true}, err: errors.New("sqs down")}
	p := newRetryTestPublisher(t, store, sender, nil, 5)

	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected send error")
	}
	// Simula a passagem do tempo sem sleep: backoff vencido.
	store.mu.Lock()
	store.next["e1"] = time.Now().Add(-time.Second)
	store.mu.Unlock()

	n, err := p.PublishOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("retry publish = (%d, %v), want (1, nil)", n, err)
	}
	bodies := sender.sentBodies()
	if len(bodies) != 2 || string(bodies[0]) != string(bodies[1]) {
		t.Errorf("bodies = %q, want o mesmo payload duas vezes", bodies)
	}
	if !store.isPublished("e1") {
		t.Error("event not published after retry")
	}
}

func TestPublisher_MarkFailureDoesNotIncrementAttempts(t *testing.T) {
	// Envio aceito pelo SQS + confirmação no banco falhou: attempts NÃO é
	// incrementado (§7) — o evento segue elegível para reenvio idêntico.
	store := newStubOutboxStore(stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`))
	store.markFails = 1
	store.markErr = errors.New("db down")
	p := newRetryTestPublisher(t, store, &fakeSender{}, nil, 5)

	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected mark error")
	}
	if store.getAttempts("e1") != 0 {
		t.Errorf("attempts = %d, want 0 (envio foi aceito)", store.getAttempts("e1"))
	}
}

func TestPublisher_ExhaustedGoesToDLQ(t *testing.T) {
	// Tentativas esgotadas: corpo idêntico na DLQ + dead_lettered_at, e
	// published_at continua NULL (distinguível de publicado).
	row := stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`)
	row.Attempts = 5
	store := newStubOutboxStore(row)
	sender := &fakeSender{}
	dlq := &fakeSender{}
	p := newRetryTestPublisher(t, store, sender, dlq, 5)

	n, err := p.PublishOnce(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("publish = (%d, %v), want (0, nil)", n, err)
	}
	if len(sender.sentBodies()) != 0 {
		t.Errorf("events sends = %d, want 0 (esgotado não volta à fila)", len(sender.sentBodies()))
	}
	dlqBodies := dlq.sentBodies()
	if len(dlqBodies) != 1 || string(dlqBodies[0]) != `{"eventId":"e1"}` {
		t.Errorf("dlq bodies = %q, want o corpo idêntico", dlqBodies)
	}
	if !store.isDead("e1") {
		t.Error("event not marked dead-lettered")
	}
	if store.isPublished("e1") {
		t.Error("dead-letter marcou published_at: estados indistinguíveis")
	}
	// Descartado some dos elegíveis: próxima chamada não reenvia.
	n, err = p.PublishOnce(context.Background())
	if err != nil || n != 0 {
		t.Errorf("second publish = (%d, %v), want (0, nil)", n, err)
	}
	if len(sender.sentBodies()) != 0 || len(dlq.sentBodies()) != 1 {
		t.Error("dead-lettered event resent")
	}
}

func TestPublisher_ExhaustedWithoutDLQReschedules(t *testing.T) {
	// Sem DLQ configurada: esgotado é reagendado com backoff no teto em vez
	// de descartado silenciosamente (attempt alto satura nos 5min).
	row := stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`)
	row.Attempts = 50
	store := newStubOutboxStore(row)
	sender := &fakeSender{}
	p := newRetryTestPublisher(t, store, sender, nil, 5)

	before := time.Now()
	n, err := p.PublishOnce(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("publish = (%d, %v), want (0, nil)", n, err)
	}
	if len(sender.sentBodies()) != 0 {
		t.Errorf("sends = %d, want 0", len(sender.sentBodies()))
	}
	if store.isDead("e1") || store.isPublished("e1") {
		t.Error("event marcado sem DLQ configurada")
	}
	if store.getAttempts("e1") != 1 {
		t.Errorf("attempts = %d, want 1 (reagendado)", store.getAttempts("e1"))
	}
	if next := store.next["e1"]; next.Before(before.Add(5*time.Minute)) || next.After(time.Now().Add(5*time.Minute+time.Second)) {
		t.Errorf("next_attempt_at = %v, want teto de 5min", next)
	}
}

func TestPublisher_DLQSendFailureReschedules(t *testing.T) {
	// DLQ fora do ar: registra attempt, published/dead intocados, batch para.
	row := stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`)
	row.Attempts = 5
	store := newStubOutboxStore(row)
	sender := &fakeSender{}
	dlq := &fakeSender{failOn: map[int]bool{1: true}, err: errors.New("dlq down")}
	p := newRetryTestPublisher(t, store, sender, dlq, 5)

	n, err := p.PublishOnce(context.Background())
	if err == nil {
		t.Fatal("expected DLQ send error")
	}
	if n != 0 {
		t.Errorf("published = %d, want 0", n)
	}
	if store.isDead("e1") || store.isPublished("e1") {
		t.Error("event marcado apesar da falha na DLQ")
	}
	if store.getAttempts("e1") != 1 {
		t.Errorf("attempts = %d, want 1", store.getAttempts("e1"))
	}
}

func TestPublisher_Metrics(t *testing.T) {
	pool := consumerTestPool(t)
	metrics := observability.NewMetrics()

	// Publicado com sucesso.
	store := newStubOutboxStore(stubRow("e1", "WagerTransactionProcessed", `{"eventId":"e1"}`))
	p, err := NewPublisher(pool, store, &fakeSender{}, nil, 10, 50*time.Millisecond, 5, 5*time.Minute, true, metrics)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if n, err := p.PublishOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("publish = (%d, %v), want (1, nil)", n, err)
	}
	if got := metrics.Value("outbox_events_published_total", map[string]string{"queue": "events"}); got != 1 {
		t.Errorf("published = %d, want 1", got)
	}

	// Falha de envio.
	failStore := newStubOutboxStore(stubRow("e2", "WagerTransactionProcessed", `{"eventId":"e2"}`))
	failSender := &fakeSender{failOn: map[int]bool{1: true}, err: errors.New("sqs down")}
	pf, err := NewPublisher(pool, failStore, failSender, nil, 10, 50*time.Millisecond, 5, 5*time.Minute, true, metrics)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if _, err := pf.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected send error")
	}
	if got := metrics.Value("outbox_publish_failures_total", map[string]string{"queue": "events"}); got != 1 {
		t.Errorf("failures = %d, want 1", got)
	}

	// Descarte para DLQ.
	deadRow := stubRow("e3", "WagerTransactionProcessed", `{"eventId":"e3"}`)
	deadRow.Attempts = 5
	deadStore := newStubOutboxStore(deadRow)
	pd, err := NewPublisher(pool, deadStore, &fakeSender{}, &fakeSender{}, 10, 50*time.Millisecond, 5, 5*time.Minute, true, metrics)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if _, err := pd.PublishOnce(context.Background()); err != nil {
		t.Fatalf("dlq publish: %v", err)
	}
	if got := metrics.Value("outbox_events_dead_lettered_total", nil); got != 1 {
		t.Errorf("dead-lettered = %d, want 1", got)
	}
}
