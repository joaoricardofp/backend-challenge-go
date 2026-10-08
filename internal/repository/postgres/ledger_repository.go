package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

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

type ledgerCursor struct {
	CreatedAt time.Time
	ID        string
}

func encodeCursor(createdAt time.Time, id string) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor encoding: %w", err)
	}
	parts := strings.Split(string(decoded), "|")
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("invalid cursor format")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	return createdAt, parts[1], nil
}

func (r *LedgerRepository) ListByWallet(
	ctx context.Context,
	walletID string,
	cursor string,
	limit int,
) ([]domain.LedgerEntry, string, error) {
	var createdAtFilter string
	args := []any{walletID}
	argIdx := 2

	if cursor != "" {
		createdAt, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("decode cursor: %w", err)
		}
		createdAtFilter = " AND (created_at, id) > ($2, $3)"
		args = append(args, createdAt, id)
		argIdx = 4
	}

	query := fmt.Sprintf(`
		SELECT
			id,
			wallet_id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		FROM wallet_ledger_entries
		WHERE wallet_id = $1
		%s
		ORDER BY created_at, id
		LIMIT $%d
	`, createdAtFilter, argIdx)

	args = append(args, limit+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list ledger entries: %w", err)
	}
	defer rows.Close()

	var entries []domain.LedgerEntry
	count := 0

	for rows.Next() {
		if count >= limit {
			break
		}
		var (
			id            string
			walletID      string
			transactionID string
			direction     string
			amount        int64
			balanceBefore int64
			balanceAfter  int64
			createdAt     time.Time
		)
		if err := rows.Scan(&id, &walletID, &transactionID, &direction, &amount, &balanceBefore, &balanceAfter, &createdAt); err != nil {
			return nil, "", fmt.Errorf("scan ledger entry: %w", err)
		}

		amountMoney, err := domain.NewMoney(amount, "BRL")
		if err != nil {
			return nil, "", fmt.Errorf("reconstruct amount: %w", err)
		}
		balanceBeforeMoney, err := domain.NewMoney(balanceBefore, "BRL")
		if err != nil {
			return nil, "", fmt.Errorf("reconstruct balance before: %w", err)
		}
		balanceAfterMoney, err := domain.NewMoney(balanceAfter, "BRL")
		if err != nil {
			return nil, "", fmt.Errorf("reconstruct balance after: %w", err)
		}

		entry, err := domain.NewLedgerEntry(
			id,
			walletID,
			transactionID,
			domain.LedgerDirection(direction),
			amountMoney,
			balanceBeforeMoney,
			balanceAfterMoney,
		)
		if err != nil {
			return nil, "", fmt.Errorf("reconstruct ledger entry: %w", err)
		}

		entries = append(entries, *entry)
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate ledger entries: %w", err)
	}

	var nextCursor string
	if count == limit && rows.Next() {
		var (
			id            string
			walletID      string
			transactionID string
			direction     string
			amount        int64
			balanceBefore int64
			balanceAfter  int64
			createdAt     time.Time
		)
		if err := rows.Scan(&id, &walletID, &transactionID, &direction, &amount, &balanceBefore, &balanceAfter, &createdAt); err != nil {
			return nil, "", fmt.Errorf("scan extra ledger entry: %w", err)
		}
		nextCursor = encodeCursor(createdAt, id)
	}

	return entries, nextCursor, nil
}

// ListAllByWallet retorna todas as entradas do ledger de uma carteira em ordem determinística
// (created_at, id), sem paginação. Usada para reconciliação.
func (r *LedgerRepository) ListAllByWallet(
	ctx context.Context,
	walletID string,
) ([]domain.LedgerEntry, error) {
	const query = `
		SELECT
			id,
			wallet_id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		FROM wallet_ledger_entries
		WHERE wallet_id = $1
		ORDER BY created_at, id
	`

	rows, err := r.pool.Query(ctx, query, walletID)
	if err != nil {
		return nil, fmt.Errorf("list all ledger entries: %w", err)
	}
	defer rows.Close()

	var entries []domain.LedgerEntry

	for rows.Next() {
		var (
			id            string
			walletID      string
			transactionID string
			direction     string
			amount        int64
			balanceBefore int64
			balanceAfter  int64
			createdAt     time.Time
		)
		if err := rows.Scan(&id, &walletID, &transactionID, &direction, &amount, &balanceBefore, &balanceAfter, &createdAt); err != nil {
			return nil, fmt.Errorf("scan ledger entry: %w", err)
		}

		amountMoney, err := domain.NewMoney(amount, "BRL")
		if err != nil {
			return nil, fmt.Errorf("reconstruct amount: %w", err)
		}
		balanceBeforeMoney, err := domain.NewMoney(balanceBefore, "BRL")
		if err != nil {
			return nil, fmt.Errorf("reconstruct balance before: %w", err)
		}
		balanceAfterMoney, err := domain.NewMoney(balanceAfter, "BRL")
		if err != nil {
			return nil, fmt.Errorf("reconstruct balance after: %w", err)
		}

		// Construção crua de propósito: este método alimenta a
		// reconciliação, que precisa observar linhas inválidas (matemática
		// inconsistente, direção desconhecida) em vez de falhar antes de
		// detectá-las. A validação de domínio vive em NewLedgerEntry e no
		// caminho de escrita; aqui apenas transportamos o que está no banco.
		entries = append(entries, domain.LedgerEntry{
			ID:            id,
			WalletID:      walletID,
			TransactionID: transactionID,
			Direction:     domain.LedgerDirection(direction),
			Amount:        amountMoney,
			BalanceBefore: balanceBeforeMoney,
			BalanceAfter:  balanceAfterMoney,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ledger entries: %w", err)
	}

	return entries, nil
}
