package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// constraintOutboxAggregateEvent é o índice único da migration 003: a
// identidade estável do evento é (aggregate_id, event_type), onde
// aggregate_id é o ID da wager_transaction de origem.
const constraintOutboxAggregateEvent = "outbox_events_aggregate_event_unique"

// ErrDuplicateOutboxEvent indica que um evento do mesmo tipo já foi
// registrado para o agregado: o mesmo processamento não pode gerar dois
// eventos iguais, inclusive sob redelivery.
var ErrDuplicateOutboxEvent = errors.New("outbox event already exists")

// ErrOutboxEventNotFound indica ausência do evento (id inexistente) ao
// marcar publicação.
var ErrOutboxEventNotFound = errors.New("outbox event not found")

// OutboxEvent espelha outbox_events (001+004): identidade física (id),
// identidade lógica (aggregate_id + event_type, única pela 003), payload
// JSONB imutável, ocorrência, publicação (nil = ainda não publicado) e
// dead-letter (nil = não descartado). attempts/next_attempt_at dirigem o
// retry com backoff do publisher (B3.13).
type OutboxEvent struct {
	ID             string
	EventType      string
	AggregateID    string
	Payload        []byte
	OccurredAt     time.Time
	PublishedAt    *time.Time
	Attempts       int32
	NextAttemptAt  *time.Time
	DeadLetteredAt *time.Time
}

// OutboxRepository persiste eventos de saída e expõe a leitura de elegíveis,
// o registro de tentativas e as marcações de publicado/descartado para o
// publisher. Nunca chama SQS: apenas opera sobre pgx.Tx explícita, sem
// UnitOfWork genérico.
type OutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository monta o repository da outbox.
func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

// Create insere o evento dentro da transação fornecida — a mesma do wallet,
// da wager_transaction e do ledger quando chamada pelo WagerService. Falha
// de constraint ou de conexão retorna erro e provoca rollback do chamador:
// nunca existe financeiro committed sem o evento correspondente.
func (r *OutboxRepository) Create(
	ctx context.Context,
	tx pgx.Tx,
	event *OutboxEvent,
) error {
	if event.ID == "" {
		return errors.New("outbox event id is required")
	}
	if event.EventType == "" {
		return errors.New("outbox event type is required")
	}
	if event.AggregateID == "" {
		return errors.New("outbox aggregate id is required")
	}
	if len(event.Payload) == 0 || !json.Valid(event.Payload) {
		return errors.New("outbox payload must be valid JSON")
	}
	occurredAt := event.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO outbox_events (id, event_type, aggregate_id, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := tx.Exec(ctx, query, event.ID, event.EventType, event.AggregateID, event.Payload, occurredAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("%w: %v", ErrDuplicateOutboxEvent, pgErr.ConstraintName)
		}
		return fmt.Errorf("create outbox event: %w", err)
	}
	return nil
}

// scanOutboxEvent monta um OutboxEvent a partir de uma linha completa.
// next_attempt_at é NOT NULL no schema, mas o filtro de elegibilidade
// tolera NULL por robustez — por isso o scan usa ponteiro e nunca falha em
// linha legada/manual.
func scanOutboxEvent(
	id string,
	eventType string,
	aggregateID string,
	payload []byte,
	occurredAt time.Time,
	publishedAt *time.Time,
	attempts int32,
	nextAttemptAt *time.Time,
	deadLetteredAt *time.Time,
) *OutboxEvent {
	return &OutboxEvent{
		ID:             id,
		EventType:      eventType,
		AggregateID:    aggregateID,
		Payload:        payload,
		OccurredAt:     occurredAt,
		PublishedAt:    publishedAt,
		Attempts:       attempts,
		NextAttemptAt:  nextAttemptAt,
		DeadLetteredAt: deadLetteredAt,
	}
}

// ListByAggregate lista os eventos de um agregado para inspeção e testes,
// em ordem estável (ocorrência, tipo). Sem claim ou lock: a publicação com
// disputa é tratada em etapa posterior.
func (r *OutboxRepository) ListByAggregate(
	ctx context.Context,
	tx pgx.Tx,
	aggregateID string,
) ([]*OutboxEvent, error) {
	const query = `
		SELECT id, event_type, aggregate_id, payload, occurred_at, published_at,
		       attempts, next_attempt_at, dead_lettered_at
		FROM outbox_events
		WHERE aggregate_id = $1
		ORDER BY occurred_at, event_type
	`

	rows, err := tx.Query(ctx, query, aggregateID)
	if err != nil {
		return nil, fmt.Errorf("list outbox events: %w", err)
	}
	defer rows.Close()

	var out []*OutboxEvent
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
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		out = append(out, scanOutboxEvent(id, eventType, aggregateID, payload, occurredAt, publishedAt, attempts, nextAttemptAt, deadLetteredAt))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return out, nil
}

// ListPending lista até limit eventos elegíveis para publicação, em ordem
// estável (próxima tentativa, ocorrência, tipo):
//
//	published_at IS NULL (nunca entregue com sucesso)
//	AND dead_lettered_at IS NULL (nunca descartado para a DLQ)
//	AND (next_attempt_at IS NULL OR next_attempt_at <= now()) (backoff vencido)
//
// É a leitura do publisher. A ordenação por next_attempt_at evita que dois
// publishers processem continuamente a mesma janela quente: eventos com
// falha recente ficam para trás até o backoff vencer (sem claim/lease, sem
// infra nova — duplicatas ocasionais continuam possíveis e benignas, vide
// identidade estável do evento).
func (r *OutboxRepository) ListPending(
	ctx context.Context,
	tx pgx.Tx,
	limit int,
) ([]*OutboxEvent, error) {
	if limit <= 0 {
		return nil, errors.New("outbox pending limit must be positive")
	}
	const query = `
		SELECT id, event_type, aggregate_id, payload, occurred_at, published_at,
		       attempts, next_attempt_at, dead_lettered_at
		FROM outbox_events
		WHERE published_at IS NULL
		  AND dead_lettered_at IS NULL
		  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		ORDER BY next_attempt_at ASC NULLS FIRST, occurred_at, event_type
		LIMIT $1
	`

	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox events: %w", err)
	}
	defer rows.Close()

	var out []*OutboxEvent
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
			return nil, fmt.Errorf("scan pending outbox event: %w", err)
		}
		out = append(out, scanOutboxEvent(id, eventType, aggregateID, payload, occurredAt, publishedAt, attempts, nextAttemptAt, deadLetteredAt))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox events: %w", err)
	}
	return out, nil
}

// MarkPublished marca o evento como publicado após o SendMessage ter
// retornado sucesso — nunca antes. Usa o id estável e só transita NULL para
// timestamp: payload, tipo, agregado e ocorrência nunca são tocados (a
// outbox permanece auditável). Já publicada é sucesso idempotente; id
// inexistente retorna ErrOutboxEventNotFound.
func (r *OutboxRepository) MarkPublished(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) error {
	if id == "" {
		return errors.New("outbox event id is required")
	}
	const updateQuery = `
		UPDATE outbox_events
		SET published_at = now()
		WHERE id = $1
		  AND published_at IS NULL
	`

	tag, err := tx.Exec(ctx, updateQuery, id)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbox_events WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("check outbox event existence: %w", err)
	}
	if exists {
		return nil
	}
	return ErrOutboxEventNotFound
}

// RecordAttemptFailure registra atomicamente uma tentativa frustrada de
// envio: attempts + 1 e next_attempt_at reagendado, published_at preservado
// em NULL. É um único UPDATE (sem SELECT antes): sobrevive a restart, crash
// e múltiplas instâncias, pois o retry nasce do banco, não da memória. id
// inexistente retorna ErrOutboxEventNotFound; linha já publicada ou já em
// dead-letter é sucesso idempotente (corrida benigna entre publishers: a
// entrega ou o descarte já aconteceu).
func (r *OutboxRepository) RecordAttemptFailure(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	nextAttemptAt time.Time,
) error {
	if id == "" {
		return errors.New("outbox event id is required")
	}
	if nextAttemptAt.IsZero() {
		return errors.New("outbox next attempt is required")
	}
	const updateQuery = `
		UPDATE outbox_events
		SET attempts = attempts + 1,
		    next_attempt_at = $2
		WHERE id = $1
		  AND published_at IS NULL
		  AND dead_lettered_at IS NULL
	`

	tag, err := tx.Exec(ctx, updateQuery, id, nextAttemptAt.UTC())
	if err != nil {
		return fmt.Errorf("record outbox attempt failure: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbox_events WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("check outbox event existence: %w", err)
	}
	if exists {
		return nil
	}
	return ErrOutboxEventNotFound
}

// MarkDeadLettered registra o descarte do evento para a DLQ após o corpo
// idêntico ter sido encaminhado: preenche dead_lettered_at e preserva
// published_at em NULL — publicado com sucesso continua distinguível de
// dead-letter. Payload, tipo, agregado e ocorrência nunca são tocados. Já
// em dead-letter ou já publicado é sucesso idempotente; id inexistente
// retorna ErrOutboxEventNotFound.
func (r *OutboxRepository) MarkDeadLettered(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) error {
	if id == "" {
		return errors.New("outbox event id is required")
	}
	const updateQuery = `
		UPDATE outbox_events
		SET dead_lettered_at = now()
		WHERE id = $1
		  AND published_at IS NULL
		  AND dead_lettered_at IS NULL
	`

	tag, err := tx.Exec(ctx, updateQuery, id)
	if err != nil {
		return fmt.Errorf("mark outbox event dead-lettered: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbox_events WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("check outbox event existence: %w", err)
	}
	if exists {
		return nil
	}
	return ErrOutboxEventNotFound
}
