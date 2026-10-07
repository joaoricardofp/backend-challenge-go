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

// Constraint names matching the unique indexes in 001_initial_schema.sql.
const (
	constraintProviderExternalID  = "wager_transactions_provider_external_id_unique"
	constraintProviderIdempotency = "wager_transactions_provider_idempotency_key_unique"
)

type WagerTransactionRepository struct {
	pool *pgxpool.Pool
}

func NewWagerTransactionRepository(pool *pgxpool.Pool) *WagerTransactionRepository {
	return &WagerTransactionRepository{pool: pool}
}

// Create insere uma nova transação de aposta dentro da transação de banco de dados fornecida.
//
// Unique violation handling:
//   - 23505 on provider_id + external_transaction_id → domain.ErrDuplicateExternalID
//   - 23505 on provider_id + idempotency_key        → domain.ErrDuplicateIdempotencyKey
//   - any other error                                → real persistence error
func (r *WagerTransactionRepository) Create(
	ctx context.Context,
	tx pgx.Tx,
	transaction *domain.WagerTransaction,
) error {
	const query = `
		INSERT INTO wager_transactions (
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			player_id,
			wallet_id,
			round_id,
			game_id,
			kind,
			status,
			amount,
			currency,
			reference_external_transaction_id,
			resulting_balance,
			failure_code
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
	`

	var resultingBalance *int64
	if transaction.ResultingBalance != nil {
		cents := transaction.ResultingBalance.Cents()
		resultingBalance = &cents
	}

	var refExtTxID *string
	if transaction.ReferenceExternalTransactionID != "" {
		refExtTxID = &transaction.ReferenceExternalTransactionID
	}

	var failureCode *string
	if transaction.FailureCode != "" {
		failureCode = &transaction.FailureCode
	}

	_, err := tx.Exec(
		ctx,
		query,
		transaction.ID,
		transaction.ProviderID,
		transaction.ExternalTransactionID,
		transaction.IdempotencyKey,
		transaction.PayloadHash,
		transaction.PlayerID,
		transaction.WalletID,
		transaction.RoundID,
		transaction.GameID,
		string(transaction.Kind),
		string(transaction.Status),
		transaction.Amount.Cents(),
		transaction.Amount.Currency(),
		refExtTxID,
		resultingBalance,
		failureCode,
	)
	if err != nil {
		return classifyInsertError(err)
	}

	return nil
}

// GetByProviderExternalID busca uma transação pelo provider_id + external_transaction_id.
func (r *WagerTransactionRepository) GetByProviderExternalID(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	externalTransactionID string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			player_id,
			wallet_id,
			round_id,
			game_id,
			kind,
			status,
			amount,
			currency,
			reference_external_transaction_id,
			resulting_balance,
			failure_code
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
	`

	return r.scanTransaction(tx.QueryRow(ctx, query, providerID, externalTransactionID))
}

// GetByProviderExternalIDForUpdate é a leitura com lock da referência de uma
// reversão: mesma busca por (provider_id, external_transaction_id), com
// FOR UPDATE na mesma pgx.Tx. Serializa reversões concorrentes da mesma
// referência (ordem global de locks: wallet → reference).
func (r *WagerTransactionRepository) GetByProviderExternalIDForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	externalTransactionID string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			player_id,
			wallet_id,
			round_id,
			game_id,
			kind,
			status,
			amount,
			currency,
			reference_external_transaction_id,
			resulting_balance,
			failure_code
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
		FOR UPDATE
	`

	return r.scanTransaction(tx.QueryRow(ctx, query, providerID, externalTransactionID))
}

// FindProcessedReversal retorna a reversão PROCESSED (REFUND ou ROLLBACK)
// de uma referência identificada por (provider_id,
// reference_external_transaction_id), ou ErrTransactionNotFound quando não
// há nenhuma. Reversões não terminais não bloqueiam (A3.2: só PROCESSED
// conta como reversão bem-sucedida). Não faz lock: a serialização vem do
// FOR UPDATE na linha da referência, adquirido antes desta consulta.
func (r *WagerTransactionRepository) FindProcessedReversal(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	referenceExternalTransactionID string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			player_id,
			wallet_id,
			round_id,
			game_id,
			kind,
			status,
			amount,
			currency,
			reference_external_transaction_id,
			resulting_balance,
			failure_code
		FROM wager_transactions
		WHERE provider_id = $1
		  AND reference_external_transaction_id = $2
		  AND kind IN ('REFUND', 'ROLLBACK')
		  AND status = 'PROCESSED'
		LIMIT 1
	`

	return r.scanTransaction(tx.QueryRow(ctx, query, providerID, referenceExternalTransactionID))
}

// GetByIdempotencyKey busca uma transação pelo provider_id + idempotency_key.
func (r *WagerTransactionRepository) GetByIdempotencyKey(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	idempotencyKey string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			player_id,
			wallet_id,
			round_id,
			game_id,
			kind,
			status,
			amount,
			currency,
			reference_external_transaction_id,
			resulting_balance,
			failure_code
		FROM wager_transactions
		WHERE provider_id = $1
		  AND idempotency_key = $2
	`

	return r.scanTransaction(tx.QueryRow(ctx, query, providerID, idempotencyKey))
}

// UpdateStatus atualiza o status, resulting_balance e failure_code de uma transação.
func (r *WagerTransactionRepository) UpdateStatus(
	ctx context.Context,
	tx pgx.Tx,
	transaction *domain.WagerTransaction,
) error {
	const query = `
		UPDATE wager_transactions
		SET status = $2,
		    resulting_balance = $3,
		    failure_code = $4,
		    updated_at = now()
		WHERE id = $1
	`

	var resultingBalance *int64
	if transaction.ResultingBalance != nil {
		cents := transaction.ResultingBalance.Cents()
		resultingBalance = &cents
	}

	var failureCode *string
	if transaction.FailureCode != "" {
		failureCode = &transaction.FailureCode
	}

	tag, err := tx.Exec(
		ctx,
		query,
		transaction.ID,
		string(transaction.Status),
		resultingBalance,
		failureCode,
	)
	if err != nil {
		return fmt.Errorf("update wager transaction status: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrTransactionNotFound
	}

	return nil
}

// scanTransaction mapeia uma linha única em um domain.WagerTransaction.
func (r *WagerTransactionRepository) scanTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		id                    string
		providerID            string
		externalTransactionID string
		idempotencyKey        string
		payloadHash           string
		playerID              string
		walletID              string
		roundID               *string
		gameID                *string
		kind                  string
		status                string
		amount                int64
		currency              string
		refExtTxID            *string
		resultingBalance      *int64
		failureCode           *string
	)

	err := row.Scan(
		&id,
		&providerID,
		&externalTransactionID,
		&idempotencyKey,
		&payloadHash,
		&playerID,
		&walletID,
		&roundID,
		&gameID,
		&kind,
		&status,
		&amount,
		&currency,
		&refExtTxID,
		&resultingBalance,
		&failureCode,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTransactionNotFound
		}
		return nil, fmt.Errorf("scan wager transaction: %w", err)
	}

	money, err := domain.NewMoney(amount, currency)
	if err != nil {
		return nil, fmt.Errorf("reconstruct transaction amount: %w", err)
	}

	wt := &domain.WagerTransaction{
		ID:                    id,
		ProviderID:            providerID,
		ExternalTransactionID: externalTransactionID,
		IdempotencyKey:        idempotencyKey,
		PayloadHash:           payloadHash,
		PlayerID:              playerID,
		WalletID:              walletID,
		Kind:                  domain.WagerTransactionKind(kind),
		Status:                domain.WagerTransactionStatus(status),
		Amount:                money,
	}

	if roundID != nil {
		wt.RoundID = *roundID
	}
	if gameID != nil {
		wt.GameID = *gameID
	}
	if refExtTxID != nil {
		wt.ReferenceExternalTransactionID = *refExtTxID
	}
	if resultingBalance != nil {
		bal, err := domain.NewMoney(*resultingBalance, currency)
		if err != nil {
			return nil, fmt.Errorf("reconstruct resulting balance: %w", err)
		}
		wt.ResultingBalance = &bal
	}
	if failureCode != nil {
		wt.FailureCode = *failureCode
	}

	return wt, nil
}

// classifyInsertError classifica o erro de inserção com base no nome da restrição.
// Retorna o erro apropriado com base no nome da restrição, ou o erro original se não for reconhecido.
func classifyInsertError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		switch pgErr.ConstraintName {
		case constraintProviderExternalID:
			return domain.ErrDuplicateExternalID
		case constraintProviderIdempotency:
			return domain.ErrDuplicateIdempotencyKey
		}
	}

	return fmt.Errorf("create wager transaction: %w", err)
}
