package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

type LedgerRepository struct {
	pool *pgxpool.Pool
}

func NewLedgerRepository(pool *pgxpool.Pool) *LedgerRepository {
	return &LedgerRepository{
		pool: pool,
	}
}

func (r *LedgerRepository) Create(
	ctx context.Context,
	tx pgx.Tx,
	entry *domain.LedgerEntry,
) error {
	const query = `
		INSERT INTO wallet_ledger_entries (
			id,
			wallet_id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err := tx.Exec(
		ctx,
		query,
		entry.ID,
		entry.WalletID,
		entry.TransactionID,
		entry.Direction,
		entry.Amount.Cents(),
		entry.BalanceBefore.Cents(),
		entry.BalanceAfter.Cents(),
	)
	if err != nil {
		return fmt.Errorf("create ledger entry: %w", err)
	}

	return nil
}
