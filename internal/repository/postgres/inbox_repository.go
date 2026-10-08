package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInboxNotFound indica ausência de (consumer, message_id) na inbox.
var ErrInboxNotFound = errors.New("inbox message not found")

// InboxMessage espelha inbox_messages: identidade (consumer, message_id),
// hash do payload cru, recebimento e conclusão (nil = ainda não concluída).
// Estados derivam das colunas, sem máquina extra:
// ausente = nova; presente sem completed_at = recebida/em processamento;
// com completed_at = processada.
type InboxMessage struct {
	Consumer    string
	MessageID   string
	PayloadHash string
	ReceivedAt  time.Time
	CompletedAt *time.Time
}

// IsCompleted retorna se a mensagem já foi concluída.
func (m *InboxMessage) IsCompleted() bool {
	return m.CompletedAt != nil
}

// InboxRepository persiste a inbox de mensageria. Nunca chama o
// WagerService nem conhece SQS: só reserva, consulta e conclui linhas.
type InboxRepository struct {
	pool *pgxpool.Pool
}

// NewInboxRepository monta o repository da inbox.
func NewInboxRepository(pool *pgxpool.Pool) *InboxRepository {
	return &InboxRepository{pool: pool}
}

// Reserve insere (consumer, messageID, payloadHash) e devolve created=true.
// Em corrida pelo mesmo ID, o perdedor recebe a linha vencedora com
// created=false (a PK é a autoridade final). Nunca altera linha existente.
func (r *InboxRepository) Reserve(
	ctx context.Context,
	tx pgx.Tx,
	consumer string,
	messageID string,
	payloadHash string,
) (*InboxMessage, bool, error) {
	const query = `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash)
		VALUES ($1, $2, $3)
		RETURNING consumer_name, message_id, payload_hash, received_at, completed_at
	`

	msg, err := func() (*InboxMessage, error) {
		// SAVEPOINT: se o INSERT conflitar (23505), a transação aborta no
		// PostgreSQL; o ROLLBACK TO SAVEPOINT permite o SELECT seguinte
		// na mesma transação.
		if _, err := tx.Exec(ctx, "SAVEPOINT inbox_reserve"); err != nil {
			return nil, fmt.Errorf("savepoint: %w", err)
		}
		return scanInbox(tx.QueryRow(ctx, query, consumer, messageID, payloadHash))
	}()
	if err == nil {
		return msg, true, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT inbox_reserve"); rbErr != nil {
			return nil, false, fmt.Errorf("rollback to savepoint: %w", rbErr)
		}
		existing, findErr := r.Find(ctx, tx, consumer, messageID)
		if findErr != nil {
			return nil, false, findErr
		}
		return existing, false, nil
	}
	return nil, false, fmt.Errorf("reserve inbox message: %w", err)
}

// Find busca (consumer, messageID) ou ErrInboxNotFound.
func (r *InboxRepository) Find(
	ctx context.Context,
	tx pgx.Tx,
	consumer string,
	messageID string,
) (*InboxMessage, error) {
	const query = `
		SELECT consumer_name, message_id, payload_hash, received_at, completed_at
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
	`

	return scanInbox(tx.QueryRow(ctx, query, consumer, messageID))
}

// Complete marca completed_at = now(). Idempotente: reconcluir linha já
// concluída é sucesso. Linha inexistente retorna ErrInboxNotFound.
func (r *InboxRepository) Complete(
	ctx context.Context,
	tx pgx.Tx,
	consumer string,
	messageID string,
) error {
	const query = `
		UPDATE inbox_messages
		SET completed_at = now()
		WHERE consumer_name = $1
		  AND message_id = $2
	`

	tag, err := tx.Exec(ctx, query, consumer, messageID)
	if err != nil {
		return fmt.Errorf("complete inbox message: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInboxNotFound
	}
	return nil
}

func scanInbox(row pgx.Row) (*InboxMessage, error) {
	var (
		consumer    string
		messageID   string
		payloadHash string
		receivedAt  time.Time
		completedAt *time.Time
	)

	if err := row.Scan(&consumer, &messageID, &payloadHash, &receivedAt, &completedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInboxNotFound
		}
		return nil, fmt.Errorf("scan inbox message: %w", err)
	}

	return &InboxMessage{
		Consumer:    consumer,
		MessageID:   messageID,
		PayloadHash: payloadHash,
		ReceivedAt:  receivedAt,
		CompletedAt: completedAt,
	}, nil
}
