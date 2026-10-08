package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

const constraintWalletPlayerCurrency = "wallets_player_currency_unique"

var ErrWalletPlayerCurrencyConflict = errors.New("wallet player currency unique conflict")

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
		return r.classifyCreateError(err)
	}

	return nil
}

func (r *WalletRepository) CreateWithTx(
	ctx context.Context,
	tx pgx.Tx,
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

	_, err := tx.Exec(
		ctx,
		query,
		wallet.ID,
		wallet.PlayerID,
		wallet.Currency,
		wallet.Balance.Cents(),
		wallet.Version,
	)
	if err != nil {
		return r.classifyCreateError(err)
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

func (r *WalletRepository) GetByIDForUpdate(
	ctx context.Context,
	tx pgx.Tx,
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
	FOR UPDATE`

	var (
		walletID string
		playerID string
		currency string
		balance  int64
		version  int64
	)

	err := tx.QueryRow(ctx, query, id).Scan(
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

func (r *WalletRepository) Update(
	ctx context.Context,
	tx pgx.Tx,
	wallet *domain.Wallet,
) error {
	const query = `
		UPDATE wallets
		SET balance = $2,
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1
		RETURNING version
	`

	var newVersion int64

	err := tx.QueryRow(
		ctx,
		query,
		wallet.ID,
		wallet.Balance.Cents(),
	).Scan(&newVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrWalletNotFound
		}

		return fmt.Errorf("update wallet: %w", err)
	}

	wallet.Version = newVersion

	return nil
}

func (r *WalletRepository) GetByPlayerAndCurrency(
	ctx context.Context,
	playerID string,
	currency string,
) (*domain.Wallet, error) {
	const query = `
		SELECT
			id,
			player_id,
			currency,
			balance,
			version
		FROM wallets
		WHERE player_id = $1
		  AND currency = $2
	`

	var (
		walletID   string
		currencyDB string
		balance    int64
		version    int64
	)

	err := r.pool.QueryRow(ctx, query, playerID, currency).Scan(
		&walletID,
		&playerID,
		&currencyDB,
		&balance,
		&version,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrWalletNotFound
		}

		return nil, fmt.Errorf("get wallet by player and currency: %w", err)
	}

	money, err := domain.NewMoney(balance, currencyDB)
	if err != nil {
		return nil, fmt.Errorf("create wallet money: %w", err)
	}

	return &domain.Wallet{
		ID:       walletID,
		PlayerID: playerID,
		Currency: currencyDB,
		Balance:  money,
		Version:  version,
	}, nil
}

func (r *WalletRepository) classifyCreateError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		if pgErr.ConstraintName == constraintWalletPlayerCurrency {
			return fmt.Errorf("%w: %v", ErrWalletPlayerCurrencyConflict, pgErr.ConstraintName)
		}
	}
	return fmt.Errorf("create wallet: %w", err)
}
