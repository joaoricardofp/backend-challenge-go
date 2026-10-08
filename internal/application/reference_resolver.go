package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// ReferenceResolverConfig parametriza o worker de PENDING_REFERENCE
// (README §7: retry com backoff exponencial + número máximo de tentativas;
// ao esgotar, REJECTED com referência não encontrada).
type ReferenceResolverConfig struct {
	// Interval é o período entre varreduras de vencidos.
	Interval time.Duration
	// BatchSize limita quantas linhas devidas são tratadas por varredura.
	BatchSize int
	// MaxAttempts é o número máximo de tentativas de resolução; ao
	// esgotar, a reversão é finalizada como REJECTED.
	MaxAttempts int32
	// MaxBackoff limita o backoff exponencial 2^(n-1)s entre tentativas.
	MaxBackoff time.Duration
}

// DefaultReferenceResolverConfig traz os defaults documentados:
// varredura de 5s, 10 linhas, 10 tentativas, teto de 5min.
func DefaultReferenceResolverConfig() ReferenceResolverConfig {
	return ReferenceResolverConfig{
		Interval:    5 * time.Second,
		BatchSize:   10,
		MaxAttempts: 10,
		MaxBackoff:  5 * time.Minute,
	}
}

// ReferenceResolver reprocessa reversões PENDING_REFERENCE cuja referência
// pode ter chegado depois (ordem invertida) ou nunca chegar (TTL).
// Todo o estado (tentativas, próxima tentativa) vive no banco: sobrevive a
// reinícios e permite múltiplas instâncias — a linha é serializada por
// SELECT ... FOR UPDATE e o guard de status impede duplo desfecho.
type ReferenceResolver struct {
	service *WagerService
	cfg     ReferenceResolverConfig
}

// NewReferenceResolver monta o worker sobre o mesmo WagerService dos
// fluxos síncronos (regras idênticas, sem duplicação de domínio).
func NewReferenceResolver(service *WagerService, cfg ReferenceResolverConfig) *ReferenceResolver {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 10
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Minute
	}
	return &ReferenceResolver{service: service, cfg: cfg}
}

// Run varre vencidos até o cancelamento. Erros de varredura são
// transitórios (log + próxima varredura); nunca derrubam o worker.
func (r *ReferenceResolver) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.ResolveDue(ctx); err != nil {
				slog.WarnContext(ctx, "pending reference sweep failed",
					slog.String("component", "reference-resolver"),
					slog.String("error", err.Error()))
			}
		}
	}
}

// ResolveDue trata uma leva de PENDING_REFERENCE vencidos e retorna
// quantos atingiram desfecho terminal (PROCESSED, REJECTED ou FAILED).
func (r *ReferenceResolver) ResolveDue(ctx context.Context) (int, error) {
	ids, err := r.service.transactions.ListDuePendingReferences(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	settled := 0
	for _, id := range ids {
		done, err := r.resolveOne(ctx, id)
		if err != nil {
			// Falha transitória (conexão, serialização): a linha mantém o
			// estado e será retomada na próxima varredura, por esta ou
			// outra instância. Não contamina as demais linhas da leva.
			slog.WarnContext(ctx, "pending reference attempt failed",
				slog.String("component", "reference-resolver"),
				slog.String("transaction_id", id),
				slog.String("error", err.Error()))
			continue
		}
		if done {
			settled++
		}
	}
	return settled, nil
}

// resolveOne tenta concluir uma reversão pendente em UMA transação
// PostgreSQL. Ordem de locks: linha pendente → wallet → referência (a
// linha pendente nunca conflita com a referência: linhas distintas;
// wallet antes da referência segue a convenção global).
func (r *ReferenceResolver) resolveOne(ctx context.Context, id string) (bool, error) {
	s := r.service
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = pgxTx.Rollback(ctx)
	}()

	pending, err := s.transactions.GetByIDForUpdate(ctx, pgxTx, id)
	if err != nil {
		return false, err
	}
	if !pending.IsPendingReference() {
		// Outro resolvedor concluiu entre a listagem e o lock.
		return false, nil
	}

	wallet, err := s.wallets.GetByIDForUpdate(ctx, pgxTx, pending.WalletID)
	if err != nil {
		return false, err
	}

	reference, err := s.transactions.GetByProviderExternalIDForUpdate(ctx, pgxTx, pending.ProviderID, pending.ReferenceExternalTransactionID)
	if err != nil {
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			return false, err
		}
		return r.retryOrExhaust(ctx, pgxTx, pending, wallet)
	}
	if !reference.IsProcessed() {
		if reference.IsTerminal() {
			return r.rejectPending(ctx, pgxTx, pending, wallet, domain.ErrReferenceNotProcessed)
		}
		return r.retryOrExhaust(ctx, pgxTx, pending, wallet)
	}

	var existingReversal *domain.WagerTransaction
	found, err := s.transactions.FindProcessedReversal(ctx, pgxTx, pending.ProviderID, pending.ReferenceExternalTransactionID)
	if err != nil {
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			return false, err
		}
	} else {
		existingReversal = found
	}
	if err := domain.ValidateReversal(reference, existingReversal, pending); err != nil {
		return r.rejectPending(ctx, pgxTx, pending, wallet, err)
	}

	effect, err := pending.FinancialEffect(reference)
	if err != nil {
		return r.rejectPending(ctx, pgxTx, pending, wallet, err)
	}

	balanceBefore := wallet.Balance
	if effect.HasMovement {
		if err := applyMovement(wallet, effect); err != nil {
			return r.rejectPending(ctx, pgxTx, pending, wallet, err)
		}
		if err := s.wallets.Update(ctx, pgxTx, wallet); err != nil {
			return false, err
		}
	}

	var movementEntry *domain.LedgerEntry
	if effect.HasMovement {
		ledgerID, err := newLedgerID(ctx, pgxTx)
		if err != nil {
			return false, err
		}
		entry, err := domain.NewLedgerEntry(
			ledgerID,
			wallet.ID,
			pending.ID,
			effect.Direction,
			effect.Amount,
			balanceBefore,
			wallet.Balance,
		)
		if err != nil {
			return false, err
		}
		if err := s.ledger.Create(ctx, pgxTx, entry); err != nil {
			return false, err
		}
		movementEntry = entry
	}

	if err := pending.Complete(wallet.Balance); err != nil {
		return false, err
	}
	if err := s.transactions.UpdateStatus(ctx, pgxTx, pending); err != nil {
		return false, err
	}
	now := time.Now().UTC()
	if err := s.persistDecisionEvent(ctx, pgxTx, pending, now); err != nil {
		return false, err
	}
	if effect.HasMovement {
		if err := s.persistBalanceChangedEvent(ctx, pgxTx, movementEntry, wallet.Version, pending.IdempotencyKey, now); err != nil {
			return false, err
		}
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit pending resolution: %w", err)
	}
	return true, nil
}

// retryOrExhaust reagenda a tentativa com backoff exponencial 2^(n-1)s com
// teto, ou finaliza como REJECTED/REFERENCE_NOT_FOUND ao esgotar
// MaxAttempts. O guard de status no UPDATE impede que dois resolvedores
// regridam uma linha já concluída.
func (r *ReferenceResolver) retryOrExhaust(ctx context.Context, pgxTx pgx.Tx, pending *domain.WagerTransaction, wallet *domain.Wallet) (bool, error) {
	s := r.service
	attempts, err := s.transactions.GetPendingProgress(ctx, pgxTx, pending.ID)
	if err != nil {
		return false, err
	}
	if attempts >= r.cfg.MaxAttempts {
		return r.rejectPending(ctx, pgxTx, pending, wallet, domain.ErrReferenceNotFound)
	}
	next := attempts + 1
	delay := time.Second << (next - 1)
	if delay > r.cfg.MaxBackoff || delay <= 0 {
		delay = r.cfg.MaxBackoff
	}
	claimed, err := s.transactions.UpdatePendingRetry(ctx, pgxTx, pending.ID, next, time.Now().UTC().Add(delay))
	if err != nil {
		return false, err
	}
	if !claimed {
		return false, nil
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit pending retry: %w", err)
	}
	return false, nil
}

// rejectPending finaliza uma linha ainda PENDING_REFERENCE como terminal
// (REJECTED, ou FAILED para overflow) com evento de decisão, no mesmo
// commit. Códigos desconhecidos são erro transitório, sem mudança de
// estado: a linha será retomada (e pode esgotar para REFERENCE_NOT_FOUND
// apenas pelo caminho de retry).
func (r *ReferenceResolver) rejectPending(ctx context.Context, pgxTx pgx.Tx, pending *domain.WagerTransaction, wallet *domain.Wallet, opErr error) (bool, error) {
	s := r.service
	failureCode, rejected, ok := failureCodeFor(opErr)
	if !ok {
		return false, opErr
	}
	if rejected {
		if err := pending.Reject(failureCode); err != nil {
			return false, err
		}
		balance := wallet.Balance
		pending.ResultingBalance = &balance
	} else {
		if err := pending.Fail(failureCode); err != nil {
			return false, err
		}
	}
	if err := s.transactions.UpdateStatus(ctx, pgxTx, pending); err != nil {
		return false, err
	}
	if err := s.persistDecisionEvent(ctx, pgxTx, pending, time.Now().UTC()); err != nil {
		return false, err
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit pending rejection: %w", err)
	}
	return true, nil
}
