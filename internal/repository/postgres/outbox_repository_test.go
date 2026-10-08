package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

func jsonUnmarshal(raw []byte, out *map[string]any) error {
	return json.Unmarshal(raw, out)
}

func newOutboxEvent(t *testing.T, pool *pgxpool.Pool, eventType, aggregateID string) *postgres.OutboxEvent {
	t.Helper()

	// id e aggregate_id são UUID no schema: sempre UUIDs válidos.
	id := newUUID(t, pool)
	return &postgres.OutboxEvent{
		ID:          id,
		EventType:   eventType,
		AggregateID: aggregateID,
		Payload:     []byte(`{"eventId":"` + id + `","eventType":"` + eventType + `"}`),
		OccurredAt:  time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC),
	}
}

func createOutboxCommitted(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, event *postgres.OutboxEvent) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := repo.Create(ctx, dbTx, event); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("create: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE id = $1`, event.ID)
	})
}

func listOutbox(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, aggregateID string) []*postgres.OutboxEvent {
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

func TestOutboxRepository_CreateAndList(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	agg := newUUID(t, pool)
	ev := newOutboxEvent(t, pool, "WagerTransactionProcessed", agg)
	createOutboxCommitted(t, pool, repo, ev)

	events := listOutbox(t, pool, repo, agg)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	got := events[0]
	if got.ID != ev.ID || got.EventType != "WagerTransactionProcessed" || got.AggregateID != agg {
		t.Errorf("row = %+v", got)
	}
	// JSONB normaliza o texto: comparar semanticamente, não byte a byte.
	var payload map[string]any
	if err := jsonUnmarshal(got.Payload, &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload["eventId"] != ev.ID || payload["eventType"] != "WagerTransactionProcessed" {
		t.Errorf("payload = %v", payload)
	}
	if got.OccurredAt.IsZero() {
		t.Error("OccurredAt zero")
	}
	if got.PublishedAt != nil {
		t.Errorf("PublishedAt = %v, want NULL (ainda não publicado)", got.PublishedAt)
	}
}

func TestOutboxRepository_DuplicateSameType(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	agg := newUUID(t, pool)
	first := newOutboxEvent(t, pool, "WagerTransactionProcessed", agg)
	createOutboxCommitted(t, pool, repo, first)

	// Mesmo (aggregate, tipo), outro id físico: rejeitado pela 003.
	second := newOutboxEvent(t, pool, "WagerTransactionProcessed", agg)
	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.Create(ctx, dbTx, second); !errors.Is(err, postgres.ErrDuplicateOutboxEvent) {
		t.Fatalf("error = %v, want ErrDuplicateOutboxEvent", err)
	}

	// Tipo distinto para o mesmo agregado: permitido (decisão + saldo).
	balance := newOutboxEvent(t, pool, "WalletBalanceChanged", agg)
	createOutboxCommitted(t, pool, repo, balance)
	if events := listOutbox(t, pool, repo, agg); len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
}

func TestOutboxRepository_Validation(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)

	for _, tc := range []struct {
		name  string
		event *postgres.OutboxEvent
	}{
		{"empty id", &postgres.OutboxEvent{EventType: "T", AggregateID: "a", Payload: []byte(`{}`)}},
		{"empty type", &postgres.OutboxEvent{ID: "x", AggregateID: "a", Payload: []byte(`{}`)}},
		{"empty aggregate", &postgres.OutboxEvent{ID: "x", EventType: "T", Payload: []byte(`{}`)}},
		{"empty payload", &postgres.OutboxEvent{ID: "x", EventType: "T", AggregateID: "a"}},
		{"invalid json", &postgres.OutboxEvent{ID: "x", EventType: "T", AggregateID: "a", Payload: []byte(`{`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbTx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer func() {
				_ = dbTx.Rollback(ctx)
			}()
			if err := repo.Create(ctx, dbTx, tc.event); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestOutboxRepository_RollbackDiscardsEvent(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	agg := newUUID(t, pool)
	ev := newOutboxEvent(t, pool, "WagerTransactionProcessed", agg)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE id = $1`, ev.ID)
	})

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := repo.Create(ctx, dbTx, ev); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("create: %v", err)
	}
	// Visível dentro da transação, descartado pelo rollback.
	if events, err := repo.ListByAggregate(ctx, dbTx, agg); err != nil || len(events) != 1 {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("events in tx = %d, err = %v, want 1", len(events), err)
	}
	if err := dbTx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if events := listOutbox(t, pool, repo, agg); len(events) != 0 {
		t.Fatalf("events after rollback = %d, want 0", len(events))
	}
}

func listPendingOutbox(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, limit int) []*postgres.OutboxEvent {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	events, err := repo.ListPending(ctx, dbTx, limit)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	return events
}

func markPublishedCommitted(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, id string) error {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.MarkPublished(ctx, dbTx, id); err != nil {
		return err
	}
	return dbTx.Commit(ctx)
}

func TestOutboxRepository_ListPendingOnlyUnpublished(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)

	// Clean up any leftover pending events from other tests
	_, _ = pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL`)

	aggPending := newUUID(t, pool)
	aggDone := newUUID(t, pool)
	pending := newOutboxEvent(t, pool, "WagerTransactionProcessed", aggPending)
	done := newOutboxEvent(t, pool, "WagerTransactionProcessed", aggDone)
	createOutboxCommitted(t, pool, repo, pending)
	createOutboxCommitted(t, pool, repo, done)
	if err := markPublishedCommitted(t, pool, repo, done.ID); err != nil {
		t.Fatalf("mark published: %v", err)
	}

	events := listPendingOutbox(t, pool, repo, 100)
	for _, ev := range events {
		if ev.PublishedAt != nil {
			t.Errorf("listed published event %s", ev.ID)
		}
		if ev.ID == done.ID {
			t.Errorf("listed already-published event %s", done.ID)
		}
	}
	found := false
	for _, ev := range events {
		if ev.ID == pending.ID {
			found = true
		}
	}
	if !found {
		t.Error("pending event not listed")
	}
}

func TestOutboxRepository_ListPendingLimit(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	for i := 0; i < 3; i++ {
		ev := newOutboxEvent(t, pool, "WagerTransactionProcessed", newUUID(t, pool))
		createOutboxCommitted(t, pool, repo, ev)
	}
	// Pelo menos 3 pendentes existem (podem existir de outros testes); o
	// limite deve ser respeitado exatamente.
	events := listPendingOutbox(t, pool, repo, 2)
	if len(events) != 2 {
		t.Fatalf("pending = %d, want exactly 2 (limit)", len(events))
	}

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if _, err := repo.ListPending(ctx, dbTx, 0); err == nil {
		t.Fatal("expected error for non-positive limit")
	}
}

func TestOutboxRepository_MarkPublished(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	agg := newUUID(t, pool)
	ev := newOutboxEvent(t, pool, "WagerTransactionProcessed", agg)
	createOutboxCommitted(t, pool, repo, ev)

	if err := markPublishedCommitted(t, pool, repo, ev.ID); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	got := listOutbox(t, pool, repo, agg)
	if len(got) != 1 || got[0].PublishedAt == nil {
		t.Fatalf("row = %+v, want published_at set", got)
	}
	if string(got[0].Payload) == "" || got[0].EventType != ev.EventType || got[0].AggregateID != agg {
		t.Errorf("mark alterou colunas auditáveis: %+v", got[0])
	}
	// Já publicada some da lista de pendentes.
	for _, pending := range listPendingOutbox(t, pool, repo, 100) {
		if pending.ID == ev.ID {
			t.Errorf("published event %s still pending", ev.ID)
		}
	}

	// Republicar é sucesso idempotente (sem erro, sem mudar nada essencial).
	before := got[0].PublishedAt
	if err := markPublishedCommitted(t, pool, repo, ev.ID); err != nil {
		t.Fatalf("second mark published: %v", err)
	}
	again := listOutbox(t, pool, repo, agg)
	if len(again) != 1 || again[0].PublishedAt == nil || !again[0].PublishedAt.Equal(*before) {
		t.Errorf("re-mark mudou published_at: %v vs %v", again[0].PublishedAt, before)
	}

	// Id inexistente: erro classificável.
	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.MarkPublished(ctx, dbTx, newUUID(t, pool)); !errors.Is(err, postgres.ErrOutboxEventNotFound) {
		t.Fatalf("error = %v, want ErrOutboxEventNotFound", err)
	}
}

func recordAttemptCommitted(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, id string, next time.Time) error {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.RecordAttemptFailure(ctx, dbTx, id, next); err != nil {
		return err
	}
	return dbTx.Commit(ctx)
}

func markDeadLetteredCommitted(t *testing.T, pool *pgxpool.Pool, repo *postgres.OutboxRepository, id string) error {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.MarkDeadLettered(ctx, dbTx, id); err != nil {
		return err
	}
	return dbTx.Commit(ctx)
}

type outboxRowState struct {
	attempts     int32
	nextAttempt  time.Time
	published    bool
	deadLettered bool
}

func readOutboxState(t *testing.T, pool *pgxpool.Pool, id string) outboxRowState {
	t.Helper()

	var (
		attempts     int32
		nextAttempt  time.Time
		published    bool
		deadLettered bool
	)
	if err := pool.QueryRow(context.Background(),
		`SELECT attempts, next_attempt_at,
		        published_at IS NOT NULL, dead_lettered_at IS NOT NULL
		 FROM outbox_events WHERE id = $1`, id,
	).Scan(&attempts, &nextAttempt, &published, &deadLettered); err != nil {
		t.Fatalf("read state: %v", err)
	}
	return outboxRowState{attempts: attempts, nextAttempt: nextAttempt, published: published, deadLettered: deadLettered}
}

// setNextAttemptAt simula a passagem do tempo sem sleeps: move o backoff
// para o passado/futuro de forma determinística.
func setNextAttemptAt(t *testing.T, pool *pgxpool.Pool, id string, ts time.Time) {
	t.Helper()

	if _, err := pool.Exec(context.Background(),
		`UPDATE outbox_events SET next_attempt_at = $2 WHERE id = $1`, id, ts); err != nil {
		t.Fatalf("set next_attempt_at: %v", err)
	}
}

func pendingIDs(events []*postgres.OutboxEvent) map[string]bool {
	ids := map[string]bool{}
	for _, ev := range events {
		ids[ev.ID] = true
	}
	return ids
}

func TestOutboxRepository_ListPendingEligibility(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)

	// Clean up any leftover pending events from other tests
	_, _ = pool.Exec(context.Background(), `DELETE FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL`)

	due := newOutboxEvent(t, pool, "WagerTransactionProcessed", newUUID(t, pool))
	future := newOutboxEvent(t, pool, "WagerTransactionProcessed", newUUID(t, pool))
	published := newOutboxEvent(t, pool, "WagerTransactionProcessed", newUUID(t, pool))
	dead := newOutboxEvent(t, pool, "WagerTransactionProcessed", newUUID(t, pool))
	createOutboxCommitted(t, pool, repo, due)
	createOutboxCommitted(t, pool, repo, future)
	createOutboxCommitted(t, pool, repo, published)
	createOutboxCommitted(t, pool, repo, dead)
	setNextAttemptAt(t, pool, future.ID, time.Now().Add(time.Hour))
	if err := markPublishedCommitted(t, pool, repo, published.ID); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if err := markDeadLetteredCommitted(t, pool, repo, dead.ID); err != nil {
		t.Fatalf("mark dead-lettered: %v", err)
	}

	ids := pendingIDs(listPendingOutbox(t, pool, repo, 100))
	if !ids[due.ID] {
		t.Error("due event not listed")
	}
	for _, tc := range []struct{ name, id string }{
		{"future backoff", future.ID},
		{"published", published.ID},
		{"dead-lettered", dead.ID},
	} {
		if ids[tc.id] {
			t.Errorf("%s event %s listed, want excluded", tc.name, tc.id)
		}
	}
}

func TestOutboxRepository_RecordAttemptFailure(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	ev := newOutboxEvent(t, pool, "WagerTransactionProcessed", newUUID(t, pool))
	createOutboxCommitted(t, pool, repo, ev)

	next := time.Now().Add(2 * time.Second).UTC().Truncate(time.Millisecond)
	if err := recordAttemptCommitted(t, pool, repo, ev.ID, next); err != nil {
		t.Fatalf("record: %v", err)
	}
	st := readOutboxState(t, pool, ev.ID)
	if st.attempts != 1 {
		t.Errorf("attempts = %d, want 1", st.attempts)
	}
	if st.nextAttempt.UTC().Truncate(time.Millisecond).Before(next.Add(-time.Second)) ||
		st.nextAttempt.UTC().After(next.Add(time.Second)) {
		t.Errorf("next_attempt_at = %v, want ~%v", st.nextAttempt, next)
	}
	if st.published {
		t.Error("record marcou published_at, want NULL preservado")
	}

	// Segunda falha acumula: attempts 2, novo agendamento.
	later := time.Now().Add(4 * time.Second).UTC()
	if err := recordAttemptCommitted(t, pool, repo, ev.ID, later); err != nil {
		t.Fatalf("second record: %v", err)
	}
	if st := readOutboxState(t, pool, ev.ID); st.attempts != 2 {
		t.Errorf("attempts = %d, want 2", st.attempts)
	}

	// Linha já publicada: sucesso idempotente, sem tocar attempts.
	if err := markPublishedCommitted(t, pool, repo, ev.ID); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if err := recordAttemptCommitted(t, pool, repo, ev.ID, later); err != nil {
		t.Fatalf("record on published: %v, want nil idempotente", err)
	}
	if st := readOutboxState(t, pool, ev.ID); st.attempts != 2 {
		t.Errorf("attempts after published record = %d, want 2 (intocado)", st.attempts)
	}

	// Id inexistente e timestamp zero: erros classificáveis.
	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.RecordAttemptFailure(ctx, dbTx, newUUID(t, pool), later); !errors.Is(err, postgres.ErrOutboxEventNotFound) {
		t.Fatalf("error = %v, want ErrOutboxEventNotFound", err)
	}
	if err := repo.RecordAttemptFailure(ctx, dbTx, ev.ID, time.Time{}); err == nil {
		t.Fatal("expected error for zero next_attempt_at")
	}
}

func TestOutboxRepository_AttemptRecordIsTransactional(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	ev := newOutboxEvent(t, pool, "WagerTransactionProcessed", newUUID(t, pool))
	createOutboxCommitted(t, pool, repo, ev)

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := repo.RecordAttemptFailure(ctx, dbTx, ev.ID, time.Now().Add(time.Minute)); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("record: %v", err)
	}
	if err := dbTx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if st := readOutboxState(t, pool, ev.ID); st.attempts != 0 {
		t.Errorf("attempts after rollback = %d, want 0", st.attempts)
	}
}

func TestOutboxRepository_MarkDeadLettered(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewOutboxRepository(pool)
	agg := newUUID(t, pool)
	ev := newOutboxEvent(t, pool, "WagerTransactionProcessed", agg)
	createOutboxCommitted(t, pool, repo, ev)

	if err := markDeadLetteredCommitted(t, pool, repo, ev.ID); err != nil {
		t.Fatalf("mark dead-lettered: %v", err)
	}
	st := readOutboxState(t, pool, ev.ID)
	if !st.deadLettered {
		t.Fatal("dead_lettered_at not set")
	}
	if st.published {
		t.Fatal("dead-letter marcou published_at: publicado e descartado indistinguíveis")
	}
	got := listOutbox(t, pool, repo, agg)
	if len(got) != 1 || got[0].EventType != ev.EventType || string(got[0].Payload) == "" {
		t.Errorf("mark alterou colunas auditáveis: %+v", got)
	}
	// Descartado some dos elegíveis.
	if ids := pendingIDs(listPendingOutbox(t, pool, repo, 100)); ids[ev.ID] {
		t.Errorf("dead-lettered event %s still eligible", ev.ID)
	}

	// Repetir é sucesso idempotente.
	if err := markDeadLetteredCommitted(t, pool, repo, ev.ID); err != nil {
		t.Fatalf("second mark dead-lettered: %v", err)
	}

	// Id inexistente: erro classificável.
	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.MarkDeadLettered(ctx, dbTx, newUUID(t, pool)); !errors.Is(err, postgres.ErrOutboxEventNotFound) {
		t.Fatalf("error = %v, want ErrOutboxEventNotFound", err)
	}
}
