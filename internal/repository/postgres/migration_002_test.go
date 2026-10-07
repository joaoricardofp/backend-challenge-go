package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Testes da migration 002_align_transaction_schema.sql. Inserem via SQL
// direto, bypassando o domínio, para exercitar as CHECKs do banco contra
// PostgreSQL real.

// insertWagerRow insere uma linha em wager_transactions. round, game e ref
// aceitam nil (colunas nullable). hash identifica a linha para limpeza.
func insertWagerRow(t *testing.T, pool *pgxpool.Pool, hash, kind, status string, amount int64, round, game, ref any) error {
	t.Helper()

	_, err := pool.Exec(context.Background(),
		`INSERT INTO wager_transactions (
			provider_id, external_transaction_id, idempotency_key, payload_hash,
			player_id, wallet_id, round_id, game_id,
			kind, status, amount, currency, reference_external_transaction_id
		) VALUES (
			gen_random_uuid(), gen_random_uuid()::text, gen_random_uuid()::text, $1,
			gen_random_uuid(), gen_random_uuid(), $2, $3,
			$4, $5, $6, 'BRL', $7
		)`,
		hash, round, game, kind, status, amount, ref,
	)
	return err
}

func cleanupWagerHash(t *testing.T, pool *pgxpool.Pool, hash string) {
	t.Helper()

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx,
			`DELETE FROM wallet_ledger_entries WHERE transaction_id IN (
				SELECT id FROM wager_transactions WHERE payload_hash = $1
			)`, hash)
		_, _ = pool.Exec(ctx,
			`DELETE FROM wager_transactions WHERE payload_hash = $1`, hash)
	})
}

func mustInsertWagerRow(t *testing.T, pool *pgxpool.Pool, hash, kind, status string, amount int64, round, game, ref any) {
	t.Helper()
	cleanupWagerHash(t, pool, hash)

	if err := insertWagerRow(t, pool, hash, kind, status, amount, round, game, ref); err != nil {
		t.Fatalf("insert (%s/%s/%d): unexpected error: %v", kind, status, amount, err)
	}
}

func mustRejectWagerRow(t *testing.T, pool *pgxpool.Pool, hash, kind, status string, amount int64, round, game, ref any) {
	t.Helper()
	cleanupWagerHash(t, pool, hash)

	err := insertWagerRow(t, pool, hash, kind, status, amount, round, game, ref)
	if err == nil {
		t.Fatalf("insert (%s/%s/%d): expected CHECK violation, got nil", kind, status, amount)
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("error = %v, want *pgconn.PgError", err)
	}
	if pgErr.Code != "23514" { // check_violation
		t.Errorf("sqlstate = %s, want 23514", pgErr.Code)
	}
}

func TestMigration002_ZeroAmounts(t *testing.T) {
	pool := newTestPool(t)

	t.Run("loss zero is accepted", func(t *testing.T) {
		mustInsertWagerRow(t, pool, "mig2-loss-0", "LOSS", "PROCESSED", 0, "round-1", "game-1", nil)
	})

	t.Run("opening zero is accepted", func(t *testing.T) {
		mustInsertWagerRow(t, pool, "mig2-opening-0", "OPENING", "PROCESSED", 0, nil, nil, nil)
	})

	t.Run("negative amount is rejected", func(t *testing.T) {
		mustRejectWagerRow(t, pool, "mig2-negative", "BET", "PENDING", -100, "round-1", "game-1", nil)
	})
}

func TestMigration002_OfficialKinds(t *testing.T) {
	pool := newTestPool(t)

	kinds := []struct {
		kind string
		ref  any
	}{
		{kind: "OPENING"},
		{kind: "BET"},
		{kind: "WIN"},
		{kind: "LOSS", ref: nil},
		{kind: "REFUND", ref: "ext-bet-1"},
		{kind: "ROLLBACK", ref: "ext-bet-1"},
	}

	for _, k := range kinds {
		t.Run(k.kind, func(t *testing.T) {
			amount := int64(5000)
			if k.kind == "LOSS" {
				amount = 0
			}
			mustInsertWagerRow(t, pool, "mig2-kind-"+k.kind, k.kind, "PENDING", amount, "round-1", "game-1", k.ref)
		})
	}
}

func TestMigration002_OfficialStatuses(t *testing.T) {
	pool := newTestPool(t)

	for _, status := range []string{"PENDING", "PENDING_REFERENCE", "PROCESSED", "REJECTED", "FAILED"} {
		t.Run(status, func(t *testing.T) {
			mustInsertWagerRow(t, pool, "mig2-status-"+status, "BET", status, 5000, "round-1", "game-1", nil)
		})
	}
}

func TestMigration002_LegacyKindsAndStatusRejected(t *testing.T) {
	pool := newTestPool(t)

	t.Run("debit kind is rejected", func(t *testing.T) {
		mustRejectWagerRow(t, pool, "mig2-debit", "DEBIT", "PENDING", 5000, "round-1", "game-1", nil)
	})

	t.Run("credit kind is rejected", func(t *testing.T) {
		mustRejectWagerRow(t, pool, "mig2-credit", "CREDIT", "PENDING", 5000, "round-1", "game-1", nil)
	})

	t.Run("completed status is rejected", func(t *testing.T) {
		mustRejectWagerRow(t, pool, "mig2-completed", "BET", "COMPLETED", 5000, "round-1", "game-1", nil)
	})
}

func TestMigration002_ReversalReference(t *testing.T) {
	pool := newTestPool(t)

	t.Run("refund with reference is accepted", func(t *testing.T) {
		mustInsertWagerRow(t, pool, "mig2-refund-ref", "REFUND", "PENDING", 5000, "round-1", "game-1", "ext-bet-1")
	})

	t.Run("rollback with reference is accepted", func(t *testing.T) {
		mustInsertWagerRow(t, pool, "mig2-rollback-ref", "ROLLBACK", "PENDING", 5000, "round-1", "game-1", "ext-bet-1")
	})

	t.Run("refund without reference is rejected", func(t *testing.T) {
		mustRejectWagerRow(t, pool, "mig2-refund-noref", "REFUND", "PENDING", 5000, "round-1", "game-1", nil)
	})

	t.Run("rollback without reference is rejected", func(t *testing.T) {
		mustRejectWagerRow(t, pool, "mig2-rollback-noref", "ROLLBACK", "PENDING", 5000, "round-1", "game-1", nil)
	})

	t.Run("refund with empty reference is rejected", func(t *testing.T) {
		mustRejectWagerRow(t, pool, "mig2-refund-emptyref", "REFUND", "PENDING", 5000, "round-1", "game-1", "")
	})

	t.Run("win without reference is accepted", func(t *testing.T) {
		mustInsertWagerRow(t, pool, "mig2-win-noref", "WIN", "PENDING", 5000, "round-1", "game-1", nil)
	})
}

func TestMigration002_OpeningRepresentation(t *testing.T) {
	pool := newTestPool(t)

	// Representação escolhida: OPENING usa o mesmo shape de metadata
	// externa (provider/external/key/hash NOT NULL) e NULL em
	// round/game/reference. A identidade interna será definida em
	// etapa própria; nenhum provider "internal" artificial foi criado.
	t.Run("opening with external metadata and null round game is accepted", func(t *testing.T) {
		mustInsertWagerRow(t, pool, "mig2-opening-shape", "OPENING", "PROCESSED", 10000, nil, nil, nil)
	})
}

func TestMigration002_LedgerDirections(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	walletID := newUUID(t, pool)
	if _, err := pool.Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance, version)
		 VALUES ($1, gen_random_uuid(), 'BRL', 10000, 1)`,
		walletID,
	); err != nil {
		t.Fatalf("insert wallet: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx,
			`DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx,
			`DELETE FROM wager_transactions WHERE wallet_id = $1`, walletID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, walletID)
	})

	var txIDs [2]string
	for i := range txIDs {
		err := pool.QueryRow(ctx,
			`INSERT INTO wager_transactions (
				provider_id, external_transaction_id, idempotency_key, payload_hash,
				player_id, wallet_id, round_id, game_id,
				kind, status, amount, currency
			) VALUES (
				gen_random_uuid(), gen_random_uuid()::text, gen_random_uuid()::text, 'mig2-ledger',
				gen_random_uuid(), $1, 'round-1', 'game-1',
				'BET', 'PROCESSED', 5000, 'BRL'
			) RETURNING id::text`,
			walletID,
		).Scan(&txIDs[i])
		if err != nil {
			t.Fatalf("insert wager transaction: %v", err)
		}
	}

	for i, direction := range []string{"DEBIT", "CREDIT"} {
		t.Run(direction, func(t *testing.T) {
			if _, err := pool.Exec(ctx,
				`INSERT INTO wallet_ledger_entries (
					wallet_id, transaction_id, direction,
					amount, balance_before, balance_after
				) VALUES ($1, $2, $3, 5000, 10000, 5000)`,
				walletID, txIDs[i], direction,
			); err != nil {
				t.Fatalf("insert ledger %s: unexpected error: %v", direction, err)
			}
		})
	}
}
