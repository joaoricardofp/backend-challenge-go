package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// OutboxStore é a superfície do OutboxRepository usada pelo Publisher:
// listar elegíveis, registrar falhas, marcar publicados e marcar
// descartados. Existe para os testes injetarem um stub sem banco; em
// produção o Fx injeta o *postgres.OutboxRepository real. Sem SQL, regras
// financeiras ou acesso a wallet/ledger aqui.
type OutboxStore interface {
	ListPending(ctx context.Context, tx pgx.Tx, limit int) ([]*postgres.OutboxEvent, error)
	MarkPublished(ctx context.Context, tx pgx.Tx, id string) error
	RecordAttemptFailure(ctx context.Context, tx pgx.Tx, id string, nextAttemptAt time.Time) error
	MarkDeadLettered(ctx context.Context, tx pgx.Tx, id string) error
}

// ComputeOutboxBackoff calcula o delay antes da próxima tentativa de envio a
// partir da contagem APÓS o incremento (primeira falha = 1):
//
//	1 → 1s, 2 → 2s, 3 → 4s, 4 → 8s, ... (2^(attempt-1) segundos)
//
// com teto em maxBackoff. Determinístico e sem jitter nesta etapa: o
// agendamento persiste no banco (next_attempt_at), então restart, crash e
// múltiplas instâncias convergem para o mesmo cálculo sem coordenação.
// Protegido contra overflow de shift: expoentes altos saturam no teto.
func ComputeOutboxBackoff(attempt int32, maxBackoff time.Duration) time.Duration {
	if maxBackoff <= 0 {
		maxBackoff = time.Second
	}
	if attempt <= 1 {
		if time.Second > maxBackoff {
			return maxBackoff
		}
		return time.Second
	}
	shift := uint(attempt - 1)
	if shift >= 30 {
		return maxBackoff
	}
	d := time.Second << shift
	if d <= 0 || d > maxBackoff {
		return maxBackoff
	}
	return d
}

// Publisher publica eventos elegíveis da outbox no SQS (B3.12/B3.13). Fluxo
// por evento, sempre nesta ordem:
//
//	SELECT elegíveis (published NULL, sem dead-letter, backoff vencido)
//	        ↓
//	SendMessage na fila de eventos (payload persistido, byte a byte)
//	        ↓
//	MarkPublished (só após sucesso do SQS)
//
// Em falha de envio, o publisher NÃO tenta de novo imediatamente: registra
// attempts + 1 e next_attempt_at com backoff exponencial (mesma transação
// lógica, operação atômica no banco) e o retry nasce do próximo ciclo —
// sobrevivendo a restart, crash e múltiplas instâncias. Com tentativas
// esgotadas, o corpo idêntico é encaminhado à DLQ e o evento é marcado como
// descartado (dead_lettered_at), NUNCA como publicado.
//
// A ordem inversa (marcar antes de enviar) perderia eventos e está proibida.
// Crash entre SendMessage e MarkPublished deixa published_at IS NULL: a
// próxima execução reenvia o MESMO payload com a MESMA identidade —
// at-least-once delivery, sem exactly-once fictício. A identidade estável
// (id/eventId da outbox) permite deduplicação posterior.
//
// Sem worker pool, canais internos ou scheduler: um batch sequencial por
// ciclo. Sem jitter, claim/lease, locks distribuídos ou infra nova: dois
// publishers podem enviar o mesmo evento (benigno pela identidade estável);
// a ordenação por next_attempt_at evita que disputem continuamente a mesma
// janela quente. Erro de envio ou de DLQ interrompe o batch (decisão
// documentada): o evento falho e os seguintes permanecem elegíveis para o
// próximo ciclo.
type Publisher struct {
	pool         *pgxpool.Pool
	outbox       OutboxStore
	sender       Sender
	dlq          Sender
	batchSize    int
	pollInterval time.Duration
	maxAttempts  int32
	maxBackoff   time.Duration
	enabled      bool
	metrics      *observability.Metrics
}

// NewPublisher monta o publisher. Com enabled=false (SQS sem fila de
// eventos ou flag desligada), os senders podem ser nil: Run retorna sem
// polling. Com enabled=true, o sender da fila de eventos é obrigatório; o
// da DLQ é opcional (sem DLQ, eventos esgotados são reagendados com backoff
// no teto em vez de descartados). metrics pode ser nil (descartadas); logs
// usam o slog default.
func NewPublisher(
	pool *pgxpool.Pool,
	outbox OutboxStore,
	sender Sender,
	dlq Sender,
	batchSize int,
	pollInterval time.Duration,
	maxAttempts int32,
	maxBackoff time.Duration,
	enabled bool,
	metrics *observability.Metrics,
) (*Publisher, error) {
	if pool == nil || outbox == nil {
		return nil, errors.New("sqs publisher requires pool and outbox store")
	}
	if batchSize <= 0 {
		return nil, errors.New("sqs publisher requires a positive batch size")
	}
	if pollInterval <= 0 {
		return nil, errors.New("sqs publisher requires a positive poll interval")
	}
	if maxAttempts <= 0 {
		return nil, errors.New("sqs publisher requires positive max attempts")
	}
	if maxBackoff <= 0 {
		return nil, errors.New("sqs publisher requires a positive max backoff")
	}
	if enabled && sender == nil {
		return nil, errors.New("sqs publisher enabled without sender")
	}
	return &Publisher{
		pool:         pool,
		outbox:       outbox,
		sender:       sender,
		dlq:          dlq,
		batchSize:    batchSize,
		pollInterval: pollInterval,
		maxAttempts:  maxAttempts,
		maxBackoff:   maxBackoff,
		enabled:      enabled,
		metrics:      metrics,
	}, nil
}

// Enabled retorna se o polling deve iniciar.
func (p *Publisher) Enabled() bool {
	return p.enabled && p.sender != nil
}

// PublishOnce busca um batch de elegíveis e publica sequencialmente,
// retornando quantos eventos foram marcados como publicados. Falha de envio
// registra attempt + backoff (sem marcar); falha de marcação retorna erro
// sem "despublicar" (o evento segue elegível para reenvio). Eventos com
// tentativas esgotadas vão para a DLQ (ou são reagendados, sem DLQ).
// Respeita o contexto entre eventos.
func (p *Publisher) PublishOnce(ctx context.Context) (int, error) {
	events, err := p.fetchPending(ctx)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, ev := range events {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		if ev.Attempts >= p.maxAttempts {
			if err := p.handleExhausted(ctx, ev); err != nil {
				return published, err
			}
			continue
		}
		if err := p.sender.SendMessage(ctx, ev.Payload); err != nil {
			p.metrics.IncOutboxPublishFailure(queueEvents)
			slog.WarnContext(ctx, "outbox send failed",
				slog.String("component", "sqs-publisher"),
				slog.String("event_id", ev.ID),
				slog.String("event_type", ev.EventType),
				slog.Int("attempts", int(ev.Attempts)),
				slog.String("error", err.Error()))
			if rerr := p.recordFailure(ctx, ev.ID, ev.Attempts+1); rerr != nil {
				return published, rerr
			}
			return published, fmt.Errorf("send outbox event %s: %w", ev.ID, err)
		}
		if err := p.markPublished(ctx, ev.ID); err != nil {
			p.metrics.IncOutboxPublishFailure(queueEvents)
			slog.WarnContext(ctx, "outbox mark published failed",
				slog.String("component", "sqs-publisher"),
				slog.String("event_id", ev.ID),
				slog.String("error", err.Error()))
			return published, fmt.Errorf("mark outbox event %s published: %w", ev.ID, err)
		}
		p.metrics.IncOutboxPublished(queueEvents)
		slog.InfoContext(ctx, "outbox event published",
			slog.String("component", "sqs-publisher"),
			slog.String("event_id", ev.ID),
			slog.String("event_type", ev.EventType))
		published++
	}
	return published, nil
}

// Run executa o polling até o contexto ser cancelado. Sem goroutines
// internas e sem busy loop: cada ciclo termina com espera do intervalo fixo
// (inclusive após batch vazio ou erro), sempre interrompível pelo contexto.
// O backoff é por evento via next_attempt_at no banco — o loop em si não
// dorme por evento (sem sleep individual, sem scheduler).
func (p *Publisher) Run(ctx context.Context) error {
	if !p.Enabled() {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := p.PublishOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.WarnContext(ctx, "sqs outbox publish failed",
				slog.String("component", "sqs-publisher"),
				slog.String("error", err.Error()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.pollInterval):
		}
	}
}

// fetchPending lê o batch em transação própria de leitura (rollback ao fim).
func (p *Publisher) fetchPending(ctx context.Context) ([]*postgres.OutboxEvent, error) {
	dbTx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox fetch: %w", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	events, err := p.outbox.ListPending(ctx, dbTx, p.batchSize)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// markPublished marca o evento em transação própria com commit. Falha volta
// como erro e o evento segue elegível (reenvio futuro). O attempts NÃO é
// incrementado aqui: o envio já foi aceito pelo SQS, só a confirmação no
// banco falhou (§7: sem incremento artificial).
func (p *Publisher) markPublished(ctx context.Context, id string) error {
	dbTx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox mark: %w", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := p.outbox.MarkPublished(ctx, dbTx, id); err != nil {
		return err
	}
	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox mark: %w", err)
	}
	return nil
}

// recordFailure agenda o retry com backoff em transação própria com commit:
// attempts + 1 e next_attempt_at no banco, published_at preservado em NULL.
// O retry nasce da persistência (não de loop em memória nem sleep), então
// sobrevive a restart, crash e múltiplas instâncias.
func (p *Publisher) recordFailure(ctx context.Context, id string, attempt int32) error {
	next := time.Now().UTC().Add(ComputeOutboxBackoff(attempt, p.maxBackoff))
	dbTx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox attempt: %w", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := p.outbox.RecordAttemptFailure(ctx, dbTx, id, next); err != nil {
		return fmt.Errorf("record outbox attempt failure for %s: %w", id, err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox attempt for %s: %w", id, err)
	}
	return nil
}

// handleExhausted trata o evento com tentativas esgotadas: encaminha o corpo
// idêntico à DLQ e marca dead_lettered_at (NUNCA published_at). Sem DLQ
// configurada, reagenda com backoff no teto em vez de descartar
// silenciosamente. Falha no envio à DLQ registra attempt e interrompe o
// batch como qualquer falha de envio.
func (p *Publisher) handleExhausted(ctx context.Context, ev *postgres.OutboxEvent) error {
	if p.dlq == nil {
		return p.recordFailure(ctx, ev.ID, ev.Attempts+1)
	}
	if err := p.dlq.SendMessage(ctx, ev.Payload); err != nil {
		p.metrics.IncOutboxPublishFailure(queueDLQ)
		slog.WarnContext(ctx, "outbox DLQ send failed",
			slog.String("component", "sqs-publisher"),
			slog.String("event_id", ev.ID),
			slog.String("event_type", ev.EventType),
			slog.String("error", err.Error()))
		if rerr := p.recordFailure(ctx, ev.ID, ev.Attempts+1); rerr != nil {
			return rerr
		}
		return fmt.Errorf("send outbox event %s to DLQ: %w", ev.ID, err)
	}
	dbTx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox dead-letter: %w", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := p.outbox.MarkDeadLettered(ctx, dbTx, ev.ID); err != nil {
		return fmt.Errorf("mark outbox event %s dead-lettered: %w", ev.ID, err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox dead-letter for %s: %w", ev.ID, err)
	}
	p.metrics.IncOutboxDeadLettered()
	slog.ErrorContext(ctx, "outbox event dead-lettered",
		slog.String("component", "sqs-publisher"),
		slog.String("event_id", ev.ID),
		slog.String("event_type", ev.EventType),
		slog.Int("attempts", int(ev.Attempts)))
	return nil
}
