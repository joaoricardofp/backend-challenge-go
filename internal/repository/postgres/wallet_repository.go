package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

type WalletRepository struct {
	pool *pgxpool.Pool
}

func NewWalletRepository(pool *pgxpool.Pool) *WalletRepository {
	return &WalletRepository{
		pool: pool,
	}
}

func (r *WalletRepository) Create(
	ctx context.Context,
	wallet *domain.Wallet,
) error {
	const query = `
		INSERT INTO wallets (
			id,
			player_id,
			currency,
			balance,
			version
		)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := r.pool.Exec(
		ctx,
		query,
		wallet.ID,
		wallet.PlayerID,
		wallet.Currency,
		wallet.Balance.Cents(),
		wallet.Version,
	)
	if err != nil {
		return fmt.Errorf("create wallet: %w", err)
	}

	return nil
}

func (r *WalletRepository) GetByID(
	ctx context.Context,
	id string,
) (*domain.Wallet, error) {
	const query = `
		SELECT
			id,
			player_id,
			currency,
			balance,
			version
		FROM wallets
		WHERE id = $1
	`

	var (
		walletID string
		playerID string
		currency string
		balance  int64
		version  int64
	)

	err := r.pool.QueryRow(ctx, query, id).Scan(
		&walletID,
		&playerID,
		&currency,
		&balance,
		&version,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrWalletNotFound
		}

		return nil, fmt.Errorf("get wallet: %w", err)
	}

	money, err := domain.NewMoney(balance, currency)
	if err != nil {
		return nil, fmt.Errorf("create wallet money: %w", err)
	}

	return &domain.Wallet{
		ID:       walletID,
		PlayerID: playerID,
		Currency: currency,
		Balance:  money,
		Version:  version,
	}, nil
}
