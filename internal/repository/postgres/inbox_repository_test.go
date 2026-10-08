package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

const inboxConsumer = "wager-consumer"

// reserveCommitted reserva dentro de transação própria com commit.
func reserveCommitted(t *testing.T, pool *pgxpool.Pool, repo *postgres.InboxRepository, consumer, msgID, hash string) (*postgres.InboxMessage, bool) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	msg, created, err := repo.Reserve(ctx, dbTx, consumer, msgID, hash)
	if err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("reserve: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return msg, created
}

func cleanupInbox(t *testing.T, pool *pgxpool.Pool, consumer, msgID string) {
	t.Helper()

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, msgID)
	})
}

func findInbox(t *testing.T, pool *pgxpool.Pool, repo *postgres.InboxRepository, consumer, msgID string) (*postgres.InboxMessage, error) {
	t.Helper()

	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	return repo.Find(ctx, dbTx, consumer, msgID)
}

func TestInboxRepository_ReserveNew(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewInboxRepository(pool)
	msgID := "msg-" + newUUID(t, pool)
	cleanupInbox(t, pool, inboxConsumer, msgID)

	msg, created := reserveCommitted(t, pool, repo, inboxConsumer, msgID, "hash-1")
	if !created {
		t.Fatal("created = false, want true")
	}
	if msg.Consumer != inboxConsumer || msg.MessageID != msgID || msg.PayloadHash != "hash-1" {
		t.Errorf("row = %+v", msg)
	}
	if msg.ReceivedAt.IsZero() {
		t.Error("ReceivedAt zero")
	}
	if msg.IsCompleted() {
		t.Error("IsCompleted = true, want false")
	}
}

func TestInboxRepository_ReserveDuplicate(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewInboxRepository(pool)
	msgID := "msg-" + newUUID(t, pool)
	cleanupInbox(t, pool, inboxConsumer, msgID)

	first, created := reserveCommitted(t, pool, repo, inboxConsumer, msgID, "hash-1")
	if !created {
		t.Fatal("first created = false, want true")
	}

	second, created := reserveCommitted(t, pool, repo, inboxConsumer, msgID, "hash-2")
	if created {
		t.Fatal("second created = true, want false")
	}
	if second.MessageID != first.MessageID || second.PayloadHash != "hash-1" {
		t.Errorf("duplicate overwrote row: %+v", second)
	}
	if !second.ReceivedAt.Equal(first.ReceivedAt) {
		t.Errorf("ReceivedAt changed: %v vs %v", second.ReceivedAt, first.ReceivedAt)
	}
}

func TestInboxRepository_FindMissing(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewInboxRepository(pool)

	if _, err := findInbox(t, pool, repo, inboxConsumer, "missing"); !errors.Is(err, postgres.ErrInboxNotFound) {
		t.Fatalf("error = %v, want ErrInboxNotFound", err)
	}
}

func TestInboxRepository_Complete(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewInboxRepository(pool)
	msgID := "msg-" + newUUID(t, pool)
	cleanupInbox(t, pool, inboxConsumer, msgID)

	reserveCommitted(t, pool, repo, inboxConsumer, msgID, "hash-1")

	complete := func() error {
		ctx := context.Background()
		dbTx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			_ = dbTx.Rollback(ctx)
		}()
		if err := repo.Complete(ctx, dbTx, inboxConsumer, msgID); err != nil {
			return err
		}
		return dbTx.Commit(ctx)
	}
	if err := complete(); err != nil {
		t.Fatalf("complete: %v", err)
	}

	got, err := findInbox(t, pool, repo, inboxConsumer, msgID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !got.IsCompleted() || got.CompletedAt == nil {
		t.Errorf("not completed: %+v", got)
	}

	// Reconcluir é idempotente.
	if err := complete(); err != nil {
		t.Fatalf("second complete: %v", err)
	}

	// Inexistente.
	ctx := context.Background()
	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := repo.Complete(ctx, dbTx, inboxConsumer, "missing"); !errors.Is(err, postgres.ErrInboxNotFound) {
		t.Fatalf("error = %v, want ErrInboxNotFound", err)
	}
}

func TestInboxRepository_ConcurrentSameMessage(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewInboxRepository(pool)
	msgID := "msg-" + newUUID(t, pool)
	cleanupInbox(t, pool, inboxConsumer, msgID)

	// Barreira: ambas as goroutines partem juntas; o PostgreSQL serializa
	// na PK e exatamente uma reserva vence. Sem sleep.
	start := make(chan struct{})
	var wg sync.WaitGroup
	type outcome struct {
		msg     *postgres.InboxMessage
		created bool
		err     error
	}
	outcomes := make([]outcome, 2)
	wg.Add(2)
	for i := range outcomes {
		go func(i int) {
			defer wg.Done()
			<-start
			ctx := context.Background()
			dbTx, err := pool.Begin(ctx)
			if err != nil {
				outcomes[i] = outcome{err: err}
				return
			}
			defer func() {
				_ = dbTx.Rollback(ctx)
			}()
			msg, created, err := repo.Reserve(ctx, dbTx, inboxConsumer, msgID, "hash-race")
			if err != nil {
				outcomes[i] = outcome{err: err}
				return
			}
			if err := dbTx.Commit(ctx); err != nil {
				outcomes[i] = outcome{err: err}
				return
			}
			outcomes[i] = outcome{msg: msg, created: created}
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for _, o := range outcomes {
		if o.err != nil {
			t.Fatalf("unexpected error: %v", o.err)
		}
		if o.created {
			winners++
		}
		if o.msg.MessageID != msgID || o.msg.PayloadHash != "hash-race" {
			t.Errorf("row = %+v", o.msg)
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}

	got, err := findInbox(t, pool, repo, inboxConsumer, msgID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if got.PayloadHash != "hash-race" || got.IsCompleted() {
		t.Errorf("final row = %+v", got)
	}
}

func TestInboxRepository_Isolation(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.NewInboxRepository(pool)
	msgA := "msg-a-" + newUUID(t, pool)
	msgB := "msg-b-" + newUUID(t, pool)
	cleanupInbox(t, pool, inboxConsumer, msgA)
	cleanupInbox(t, pool, inboxConsumer, msgB)
	cleanupInbox(t, pool, "other-consumer", msgA)

	reserveCommitted(t, pool, repo, inboxConsumer, msgA, "h")
	reserveCommitted(t, pool, repo, inboxConsumer, msgB, "h")
	reserveCommitted(t, pool, repo, "other-consumer", msgA, "h")

	for _, tc := range []struct{ consumer, msgID string }{
		{inboxConsumer, msgA},
		{inboxConsumer, msgB},
		{"other-consumer", msgA},
	} {
		if _, err := findInbox(t, pool, repo, tc.consumer, tc.msgID); err != nil {
			t.Errorf("find %v: %v", tc, err)
		}
	}
	if _, err := findInbox(t, pool, repo, "other-consumer", msgB); !errors.Is(err, postgres.ErrInboxNotFound) {
		t.Errorf("cross lookup error = %v, want ErrInboxNotFound", err)
	}
}
