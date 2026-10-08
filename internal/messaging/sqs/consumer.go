// Package sqs adapta o contrato de wagering (wager_message.go) para o
// consumo via SQS com Inbox (B3.10). O consumer é o único caminho SQS até o
// negócio e delega tudo ao WagerService.Process; nenhuma regra financeira
// vive aqui.
package sqs

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// ReceivedMessage é a unidade mínima que o consumer precisa do SQS. Os tipos
// do SDK AWS vivem somente no adapter (client.go); aqui só circulam handle e
// bytes crus, o que permite fakes sem AWS nos testes.
type ReceivedMessage struct {
	ReceiptHandle string
	Body          []byte
}

// Receiver é a interface própria e mínima do consumer para o SQS: receber um
// batch e deletar pelo receipt handle. Sem ChangeMessageVisibility, sem
// heartbeat e sem retry nesta etapa (limitação documentada).
type Receiver interface {
	ReceiveMessage(ctx context.Context, maxMessages, waitTimeSeconds, visibilityTimeout int32) ([]ReceivedMessage, error)
	DeleteMessage(ctx context.Context, receiptHandle string) error
}

// Processor é o único caminho de negócio: o application service usado pelo
// HTTP. O consumer nunca implementa saldo, ledger, idempotência de negócio
// ou reversal; apenas classifica o resultado para ack/redelivery.
type Processor interface {
	Process(ctx context.Context, input application.ProcessWagerInput) (*application.ProcessWagerResult, error)
}

// Consumer executa o caminho SQS ReceiveMessage -> parse/validate ->
// Inbox Reserve -> ToTransaction -> WagerService.Process -> Inbox Complete ->
// SQS DeleteMessage (ack), com ordem de crash/recovery preservada:
//
//	Processamento da aplicação
//	        ↓
//	Inbox Complete
//	        ↓
//	SQS Delete
//
// Delete nunca acontece antes do processamento estar definitivamente
// concluído. Inbox nunca é marcada antes de Process terminar.
type Consumer struct {
	pool              *pgxpool.Pool
	inbox             *postgres.InboxRepository
	processor         Processor
	receiver          Receiver
	consumerName      string
	maxMessages       int32
	waitTimeSeconds   int32
	visibilityTimeout int32
	enabled           bool
	metrics           *observability.Metrics
}

// NewConsumer monta o consumer. Com enabled=false (SQS sem queue URL ou flag
// desligada), o receiver pode ser nil: Run retorna sem polling e StartOn não
// lança goroutine. Com enabled=true, receiver é obrigatório. metrics pode
// ser nil (métricas descartadas); logs usam o slog default.
func NewConsumer(
	pool *pgxpool.Pool,
	inbox *postgres.InboxRepository,
	processor Processor,
	receiver Receiver,
	consumerName string,
	maxMessages, waitTimeSeconds, visibilityTimeout int32,
	enabled bool,
	metrics *observability.Metrics,
) (*Consumer, error) {
	if pool == nil || inbox == nil || processor == nil {
		return nil, errors.New("sqs consumer requires pool, inbox and processor")
	}
	if consumerName == "" {
		return nil, errors.New("sqs consumer requires a stable consumer name")
	}
	if enabled && receiver == nil {
		return nil, errors.New("sqs consumer enabled without receiver")
	}
	return &Consumer{
		pool:              pool,
		inbox:             inbox,
		processor:         processor,
		receiver:          receiver,
		consumerName:      consumerName,
		maxMessages:       maxMessages,
		waitTimeSeconds:   waitTimeSeconds,
		visibilityTimeout: visibilityTimeout,
		enabled:           enabled,
		metrics:           metrics,
	}, nil
}

// Enabled retorna se o polling deve iniciar.
func (c *Consumer) Enabled() bool {
	return c.enabled && c.receiver != nil
}

// ProcessMessage executa o fluxo completo de uma mensagem (usado pelo polling
// e diretamente pelos testes). receiptHandle identifica a mensagem no SQS
// para o DeleteMessage final.
//
// Ordem e decisões:
//
//  1. parse/validate (inclui rejeição de OPENING na borda): inválida não
//     chama o service, não reserva Inbox definitiva, não completa e não
//     deleta — permanece para redelivery nesta etapa (sem DLQ).
//  2. Inbox Reserve (consumer_name, message_id) com payload_hash SHA-256 dos
//     bytes crus. Já concluída -> apenas DeleteMessage, sem Process.
//     Incompleta (nova ou redelivery) -> segue para Process.
//  3. ToTransaction com UUID interno novo: falha estrutural não chama o
//     service, não completa e não deleta (redelivery, sem DLQ nesta etapa).
//  4. WagerService.Process, classificado em:
//     - terminal persistido (PROCESSED/REJECTED/FAILED, inclusive replay com
//     err == nil, ou primeiro desfecho com (nil, erroDeNegócio)) ->
//     Complete + Delete;
//     - PENDING_REFERENCE (resultado com erro nil) -> incompleto: sem
//     Complete e sem Delete, a mensagem reaparece por visibility timeout;
//     - erro transitório/infra/contexto/conflito/progresso -> sem Complete e
//     sem Delete, permite redelivery.
//  5. Complete só depois de Process terminar; Delete só depois de Complete.
//
// Mensagem inválida nunca chega ao WagerService.
func (c *Consumer) ProcessMessage(ctx context.Context, raw []byte, receiptHandle string) error {
	msg, err := ParseWagerMessage(raw)
	if err != nil {
		// Inclui JSON inválido, envelope/type/money inválidos e OPENING
		// (ErrOpeningNotAllowed): sem service, sem Inbox completa, sem ack.
		// Decisão B3.10: sem DLQ/descarte nesta etapa; fica para redelivery
		// e será visível nos logs do polling.
		c.metrics.IncSQSFailed(queueWager, "parse")
		slog.WarnContext(ctx, "sqs message parse failed",
			slog.String("component", "sqs-consumer"), slog.String("error", err.Error()))
		return fmt.Errorf("parse wager message: %w", err)
	}

	// Correlation operacional da execução: o messageId durável do envelope
	// (estável entre redeliveries); se inválido como request ID, gera um.
	corrID := "sqs-" + msg.MessageID
	if !observability.ValidRequestID(corrID) {
		corrID = observability.NewRequestID()
	}
	ctx = observability.WithRequestID(ctx, corrID)
	logAttrs := func(args ...any) []any {
		base := []any{
			slog.String("component", "sqs-consumer"),
			slog.String("message_id", msg.MessageID),
		}
		base = append(base, observability.AttrsFromContext(ctx)...)
		return append(base, args...)
	}

	payloadHash := MessageHash(raw)

	reserved, err := c.reserve(ctx, msg.MessageID, payloadHash)
	if err != nil {
		c.metrics.IncSQSFailed(queueWager, "reserve")
		slog.WarnContext(ctx, "sqs inbox reserve failed", logAttrs(slog.String("error", err.Error()))...)
		return err
	}
	if reserved.IsCompleted() {
		// Redelivery após crash entre Complete e Delete: o trabalho já está
		// concluído; apenas descarta/acka sem reprocessar (sem duplo efeito
		// financeiro).
		if err := c.receiver.DeleteMessage(ctx, receiptHandle); err != nil {
			c.metrics.IncSQSFailed(queueWager, "delete")
			slog.WarnContext(ctx, "sqs delete of duplicate failed", logAttrs(slog.String("error", err.Error()))...)
			return fmt.Errorf("delete already-completed message: %w", err)
		}
		c.metrics.IncSQSProcessed(queueWager, "duplicate")
		slog.InfoContext(ctx, "sqs duplicate acked", logAttrs()...)
		return nil
	}

	internalID, err := newInternalID()
	if err != nil {
		c.metrics.IncSQSFailed(queueWager, "convert")
		slog.WarnContext(ctx, "sqs internal id failed", logAttrs(slog.String("error", err.Error()))...)
		return fmt.Errorf("generate internal id: %w", err)
	}
	tx, err := msg.ToTransaction(internalID)
	if err != nil {
		// Estruturalmente inválida após parse (ex. REFUND sem referência no
		// domínio): sem service, sem Complete, sem Delete — redelivery nesta
		// etapa, sem DLQ.
		c.metrics.IncSQSFailed(queueWager, "convert")
		slog.WarnContext(ctx, "sqs message convert failed", logAttrs(slog.String("error", err.Error()))...)
		return fmt.Errorf("convert wager message: %w", err)
	}

	res, procErr := c.processor.Process(ctx, application.ProcessWagerInput{Transaction: tx})
	terminal, pendingRef := classifyOutcome(res, procErr)
	// Resultado pode ser nil no desfecho terminal via erro de negócio
	// (primeira rejeição/falha retorna (nil, erroDeNegócio)): nunca
	// dereferenciar res sem guarda nos logs abaixo.
	var resTxID, resKind, resStatus string
	if res != nil {
		resTxID = res.Transaction.ID
		resKind = string(res.Transaction.Kind)
		resStatus = string(res.Transaction.Status)
	}
	switch {
	case pendingRef:
		// PENDING_REFERENCE: a reversão foi persistida aguardando a
		// referência, sem movimento. Decisão B3.10: a Inbox permanece
		// incompleta e a mensagem NÃO é deletada — o visibility timeout a
		// devolve para redelivery, quando a referência pode já existir.
		// Sem retry/backoff próprios nesta etapa.
		c.metrics.IncSQSProcessed(queueWager, "pending")
		slog.InfoContext(ctx, "sqs message pending reference",
			logAttrs(slog.String("transaction_id", resTxID))...)
		return nil
	case !terminal:
		// Erro de infraestrutura/transitório, contexto, conflito de
		// identidade, in-progress ou wallet inexistente: nada foi concluído
		// (ou nada foi persistido como terminal). Sem Complete, sem Delete.
		c.metrics.IncSQSFailed(queueWager, "process")
		if procErr != nil {
			slog.WarnContext(ctx, "sqs process failed",
				logAttrs(
					slog.String("kind", string(tx.Kind)),
					slog.String("error", procErr.Error()),
				)...)
			return fmt.Errorf("process wager: %w", procErr)
		}
		slog.WarnContext(ctx, "sqs process incomplete", logAttrs()...)
		return errors.New("process wager: incomplete without terminal outcome")
	}

	// Terminal: Processamento -> Complete -> Delete, nesta ordem.
	if err := c.complete(ctx, msg.MessageID); err != nil {
		c.metrics.IncSQSFailed(queueWager, "complete")
		slog.WarnContext(ctx, "sqs inbox complete failed",
			logAttrs(
				slog.String("transaction_id", resTxID),
				slog.String("error", err.Error()),
			)...)
		return err
	}
	if err := c.receiver.DeleteMessage(ctx, receiptHandle); err != nil {
		// Crash aqui é seguro: a redelivery encontra a Inbox concluída e
		// apenas deleta (caminho do início desta função).
		c.metrics.IncSQSFailed(queueWager, "delete")
		slog.WarnContext(ctx, "sqs delete after complete failed",
			logAttrs(
				slog.String("transaction_id", resTxID),
				slog.String("error", err.Error()),
			)...)
		return fmt.Errorf("delete processed message: %w", err)
	}
	c.metrics.IncSQSProcessed(queueWager, "acked")
	slog.InfoContext(ctx, "sqs message acked",
		logAttrs(
			slog.String("transaction_id", resTxID),
			slog.String("kind", resKind),
			slog.String("status", resStatus),
		)...)
	return nil
}

// classifyOutcome distingue mensagem definitivamente consumida (ack) de
// processamento ainda não concluído. Retorna (terminal, pendingReference).
//
// Terminal persistido (Complete + Delete):
//   - err == nil com status PROCESSED, REJECTED ou FAILED (novo processamento
//     ou replay de linha terminal);
//   - err != nil com um dos 7 erros de negócio que o WagerService persiste
//     como terminal antes de retornar (primeiro desfecho REJECTED/FAILED com
//     resultado nil): InsufficientFunds, WalletPlayerMismatch,
//     CurrencyMismatch, InvalidReferenceKind, ReferenceNotProcessed,
//     DuplicateReversal (REJECTED) e Overflow (FAILED).
//
// Ainda não concluído (sem Complete, sem Delete, redelivery):
//   - err == nil com PENDING_REFERENCE (persistido aguardando referência);
//   - qualquer outro erro (infra, contexto, wallet inexistente, conflitos de
//     identidade, in-progress): nada terminal foi persistido.
func classifyOutcome(res *application.ProcessWagerResult, procErr error) (terminal bool, pendingRef bool) {
	if procErr == nil {
		if res == nil {
			return false, false
		}
		switch {
		case res.Transaction.IsProcessed(),
			res.Transaction.IsRejected(),
			res.Transaction.IsFailed():
			return true, false
		case res.Transaction.IsPendingReference():
			return false, true
		default:
			return false, false
		}
	}
	if isTerminalBusinessError(procErr) {
		return true, false
	}
	return false, false
}

// isTerminalBusinessError reconhece os erros que o WagerService persiste como
// linha terminal (REJECTED/FAILED) antes de devolvê-los com resultado nil.
// Ver persistTerminalOutcome: qualquer outro erro não persiste terminal.
func isTerminalBusinessError(err error) bool {
	switch {
	case errors.Is(err, domain.ErrInsufficientFunds),
		errors.Is(err, domain.ErrWalletPlayerMismatch),
		errors.Is(err, domain.ErrCurrencyMismatch),
		errors.Is(err, domain.ErrInvalidReferenceKind),
		errors.Is(err, domain.ErrReferenceNotProcessed),
		errors.Is(err, domain.ErrDuplicateReversal),
		errors.Is(err, domain.ErrOverflow):
		return true
	default:
		return false
	}
}

// reserve insere (consumer, messageID) em transação própria com commit. A PK
// é a autoridade: corrida devolve a linha vencedora com created=false via
// SAVEPOINT (ver InboxRepository). Falha de banco/contexto volta como erro
// transitório (sem ack).
func (c *Consumer) reserve(ctx context.Context, messageID, payloadHash string) (*postgres.InboxMessage, error) {
	dbTx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin inbox reserve: %w", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	msg, _, err := c.inbox.Reserve(ctx, dbTx, c.consumerName, messageID, payloadHash)
	if err != nil {
		return nil, fmt.Errorf("reserve inbox: %w", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit inbox reserve: %w", err)
	}
	return msg, nil
}

// complete marca a Inbox como concluída em transação própria com commit.
// Idempotente: reconcluir é sucesso. Falha volta como erro (sem Delete).
func (c *Consumer) complete(ctx context.Context, messageID string) error {
	dbTx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin inbox complete: %w", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()
	if err := c.inbox.Complete(ctx, dbTx, c.consumerName, messageID); err != nil {
		return fmt.Errorf("complete inbox: %w", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit inbox complete: %w", err)
	}
	return nil
}

// pollOnce recebe um batch e processa sequencialmente (correção antes de
// throughput; sem worker pool nesta etapa). Erro de ReceiveMessage volta para
// o Run decidir a pausa; erro de mensagem individual é logado e não aborta o
// batch.
func (c *Consumer) pollOnce(ctx context.Context) error {
	msgs, err := c.receiver.ReceiveMessage(ctx, c.maxMessages, c.waitTimeSeconds, c.visibilityTimeout)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.metrics.IncSQSReceived(queueWager)
		if err := c.ProcessMessage(ctx, m.Body, m.ReceiptHandle); err != nil {
			slog.WarnContext(ctx, "sqs message failed",
				slog.String("component", "sqs-consumer"),
				slog.String("error", err.Error()))
		}
	}
	return nil
}

// Run executa o polling até o contexto ser cancelado. Sem goroutines
// internas, sem fila paralela, sem busy loop: ReceiveMessage com long
// polling bloqueia; erro transitório de Receive gera pausa curta de 1s
// respeitando o contexto. Erro de contexto encerra sem log ruidoso.
func (c *Consumer) Run(ctx context.Context) error {
	if !c.Enabled() {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.pollOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.WarnContext(ctx, "sqs receive failed",
				slog.String("component", "sqs-consumer"),
				slog.String("error", err.Error()))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}

// newInternalID gera o UUID interno da transação com stdlib (mesma técnica do
// transport HTTP, sem nova dependência). A idempotência entre redeliveries
// não depende dele: vale (provider, idempotencyKey/externalID).
func newInternalID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
