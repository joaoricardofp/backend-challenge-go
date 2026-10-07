package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// ErrIdentityConflict indica que as duas identidades persistentes apontam
// para transactions diferentes: (providerID, idempotencyKey) encontrou A e
// (providerID, externalTransactionID) encontrou B, com A.ID != B.ID. Nenhuma
// das duas é escolhida silenciosamente; nenhum efeito financeiro ocorre.
var ErrIdentityConflict = errors.New("idempotency and external identity point to different transactions")

// ProcessWagerInput carrega a transação a processar. A wallet é resolvida
// pelo service a partir de Transaction.WalletID; nenhum outro dado de
// entrada é necessário.
type ProcessWagerInput struct {
	Transaction domain.WagerTransaction
}

// ProcessWagerResult carrega a transação processada e o saldo resultante.
// Balance é o saldo final da wallet, inclusive para operações sem
// movimento (LOSS, OPENING 0). Replayed indica que nenhum processamento
// novo ocorreu: o resultado vem de uma transaction já persistida (replay
// idempotente pela mesma chave ou operação externa já conhecida por outra
// chave). Para replays de transactions terminais sem resulting balance
// (ex. REJECTED/FAILED), Balance é o zero da moeda da transação.
type ProcessWagerResult struct {
	Transaction domain.WagerTransaction
	Balance     domain.Money
	Replayed    bool
}

// WagerService é o caso de uso central do processamento financeiro. HTTP e
// SQS devem chamar este mesmo service; nenhuma regra financeira vive nos
// handlers/consumidores.
type WagerService struct {
	pool         *pgxpool.Pool
	wallets      *postgres.WalletRepository
	transactions *postgres.WagerTransactionRepository
	ledger       *postgres.LedgerRepository
}

// NewWagerService monta o service a partir do pool e dos repositories
// existentes, que já operam sobre pgx.Tx.
func NewWagerService(
	pool *pgxpool.Pool,
	wallets *postgres.WalletRepository,
	transactions *postgres.WagerTransactionRepository,
	ledger *postgres.LedgerRepository,
) *WagerService {
	return &WagerService{
		pool:         pool,
		wallets:      wallets,
		transactions: transactions,
		ledger:       ledger,
	}
}

// Process executa uma transação reconhecendo antes as identidades
// persistentes (idempotência + identidade externa) e processando somente o
// que é NEW, tudo sob a mesma transação PostgreSQL quando há efeito
// financeiro:
//
//	calculate fingerprint (payload_hash)
//	BEGIN
//	lookup (provider, idempotency_key) + (provider, external_transaction_id)
//	resolve: REPLAY/KNOWN terminal -> retorna persistido (rollback, sem escrita)
//	         IN_PROGRESS/conflito/inconsistência -> erro (rollback, sem escrita)
//	         NEW -> segue o fluxo financeiro abaixo
//	lock wallet (FOR UPDATE)
//	validate player/currency
//	REFUND/ROLLBACK: lock reference (FOR UPDATE), validate, check duplicate
//	FinancialEffect (com a referência resolvida, quando houver)
//	UPDATE wallet (somente com movimento)
//	INSERT wager_transaction (PENDING)
//	INSERT ledger (somente com movimento)
//	UPDATE wager_transaction -> PROCESSED + resulting_balance
//	COMMIT
//
// Ordem global de locks: wallet → reference (reversões nunca invertem).
// Reversão sem referência resolvível estaciona como PENDING_REFERENCE, sem
// movimento. Referência terminal não-PROCESSED ou kind inválido/duplicata
// persiste REJECTED com o erro de domínio correspondente.
//
// Se o INSERT perder uma corrida numa unique, a tentativa perdedora faz
// rollback e relê as identidades numa transação nova (recuperação única e
// determinística): replay, conflito ou in-progress. As uniques continuam
// sendo a autoridade final; SELECTs sozinhos não bastariam.
//
// Qualquer outra falha reverte tudo via rollback do banco.
//
// Regras de negócio esperadas viram linhas terminais atômicas: saldo
// insuficiente persiste REJECTED (failure_code INSUFFICIENT_FUNDS),
// player mismatch persiste REJECTED (PLAYER_WALLET_MISMATCH) e currency
// mismatch persiste REJECTED (CURRENCY_MISMATCH), todos com
// resulting_balance = saldo no momento; o caller continua recebendo o erro
// de domínio original. Referência inexistente ou ainda não processada
// estaciona a reversão como PENDING_REFERENCE, sem movimento; referência
// terminal não-PROCESSED, kind inválido e duplicata persistem REJECTED com
// o erro de domínio correspondente. Overflow ao creditar persiste FAILED
// (failure_code OVERFLOW, sem resulting balance, seguindo o domínio) e o
// caller recebe ErrOverflow. Wallet inexistente continua erro puro (ver
// decisão em GetByIDForUpdate): sem wallet não há saldo a registrar e a
// condição é transitória (a wallet pode ser aberta depois), de modo que um
// REJECTED terminal congelaria a resposta errada. Erros de infraestrutura,
// contexto cancelado e demais validações nunca viram FAILED/REJECTED: são
// propagados como estão, sem persistência.
func (s *WagerService) Process(ctx context.Context, input ProcessWagerInput) (*ProcessWagerResult, error) {
	tx := input.Transaction

	switch tx.Kind {
	case domain.TransactionOpening,
		domain.TransactionBet,
		domain.TransactionWin,
		domain.TransactionLoss,
		domain.TransactionRefund,
		domain.TransactionRollback:
	default:
		return nil, domain.ErrInvalidTransactionKind
	}

	if !tx.IsPending() {
		return nil, domain.ErrTransactionNotPending
	}

	fp, err := domain.CanonicalWagerFingerprint(tx)
	if err != nil {
		return nil, err
	}
	tx.PayloadHash = fp

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = pgxTx.Rollback(ctx)
	}()

	byKey, err := lookupByKey(ctx, s.transactions, pgxTx, tx.ProviderID, tx.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	byExt, err := lookupByExternal(ctx, s.transactions, pgxTx, tx.ProviderID, tx.ExternalTransactionID)
	if err != nil {
		return nil, err
	}

	existing, err := resolveIdentities(byKey, byExt, fp)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// Replay/known: somente leitura até aqui; o rollback do defer
		// descarta a transação. Nenhum lock financeiro foi tomado.
		return replayResult(existing), nil
	}

	wallet, err := s.wallets.GetByIDForUpdate(ctx, pgxTx, tx.WalletID)
	if err != nil {
		return nil, err
	}

	// O lock serializa concorrentes: quem esperou pode estar obsoleto se o
	// detentor já commitou. Revalida as identidades após o lock (a mesma
	// transação enxerga o commit alheio em READ COMMITTED) antes de qualquer
	// escrita financeira.
	byKey, err = lookupByKey(ctx, s.transactions, pgxTx, tx.ProviderID, tx.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	byExt, err = lookupByExternal(ctx, s.transactions, pgxTx, tx.ProviderID, tx.ExternalTransactionID)
	if err != nil {
		return nil, err
	}

	existing, err = resolveIdentities(byKey, byExt, fp)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return replayResult(existing), nil
	}

	if wallet.PlayerID != tx.PlayerID {
		return s.persistTerminalOutcome(ctx, pgxTx, &tx, wallet, domain.ErrWalletPlayerMismatch)
	}
	if tx.Amount.Currency() != wallet.Balance.Currency() {
		return s.persistTerminalOutcome(ctx, pgxTx, &tx, wallet, domain.ErrCurrencyMismatch)
	}

	// Reversões resolvem a referência com lock (ordem global wallet →
	// reference) antes de qualquer efeito financeiro. A referência nunca é
	// modificada: apenas lida com FOR UPDATE para serializar reversões
	// concorrentes.
	var reference *domain.WagerTransaction
	if tx.IsReversal() {
		resolved, pending, err := s.resolveReversalReference(ctx, pgxTx, &tx, wallet)
		if err != nil {
			return nil, err
		}
		if pending != nil {
			return pending, nil
		}
		reference = resolved
	}

	effect, err := tx.FinancialEffect(reference)
	if err != nil {
		return nil, err
	}

	balanceBefore := wallet.Balance

	if effect.HasMovement {
		if err := applyMovement(wallet, effect); err != nil {
			return s.persistTerminalOutcome(ctx, pgxTx, &tx, wallet, err)
		}
		if err := s.wallets.Update(ctx, pgxTx, wallet); err != nil {
			return nil, err
		}
	}

	if err := s.transactions.Create(ctx, pgxTx, &tx); err != nil {
		if errors.Is(err, domain.ErrDuplicateIdempotencyKey) ||
			errors.Is(err, domain.ErrDuplicateExternalID) {
			_ = pgxTx.Rollback(ctx)
			return s.recoverFromDuplicate(ctx, err, fp, tx.ProviderID, tx.IdempotencyKey, tx.ExternalTransactionID)
		}
		return nil, err
	}

	if effect.HasMovement {
		ledgerID, err := newLedgerID(ctx, pgxTx)
		if err != nil {
			return nil, err
		}
		entry, err := domain.NewLedgerEntry(
			ledgerID,
			wallet.ID,
			tx.ID,
			effect.Direction,
			effect.Amount,
			balanceBefore,
			wallet.Balance,
		)
		if err != nil {
			return nil, err
		}
		if err := s.ledger.Create(ctx, pgxTx, entry); err != nil {
			return nil, err
		}
	}

	if err := tx.Complete(wallet.Balance); err != nil {
		return nil, err
	}
	if err := s.transactions.UpdateStatus(ctx, pgxTx, &tx); err != nil {
		return nil, err
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return &ProcessWagerResult{
		Transaction: tx,
		Balance:     wallet.Balance,
	}, nil
}

// lookupByKey busca por (provider, idempotency_key), traduzindo ausência
// para nil sem erro.
func lookupByKey(ctx context.Context, repo *postgres.WagerTransactionRepository, pgxTx pgx.Tx, providerID, key string) (*domain.WagerTransaction, error) {
	found, err := repo.GetByIdempotencyKey(ctx, pgxTx, providerID, key)
	if err != nil {
		if errors.Is(err, domain.ErrTransactionNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return found, nil
}

// lookupByExternal busca por (provider, external_transaction_id),
// traduzindo ausência para nil sem erro.
func lookupByExternal(ctx context.Context, repo *postgres.WagerTransactionRepository, pgxTx pgx.Tx, providerID, externalID string) (*domain.WagerTransaction, error) {
	found, err := repo.GetByProviderExternalID(ctx, pgxTx, providerID, externalID)
	if err != nil {
		if errors.Is(err, domain.ErrTransactionNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return found, nil
}

// resolveIdentities combina as duas identidades persistentes usando as
// decisões B2.2/B2.3. Retorna a transaction a replayar, ou nil quando a
// operação é NEW. Conflitos, in-progress e inconsistência voltam como erro.
func resolveIdentities(byKey, byExt *domain.WagerTransaction, fp string) (*domain.WagerTransaction, error) {
	if byKey != nil && byExt != nil && byKey.ID != byExt.ID {
		return nil, ErrIdentityConflict
	}

	if byKey != nil {
		res, err := DecideIdempotency(byKey, fp)
		if err != nil {
			return nil, err
		}
		return res.Existing, nil
	}

	if byExt != nil {
		res, err := DecideExternalTransaction(byExt, fp)
		if err != nil {
			return nil, err
		}
		if !res.Existing.IsTerminal() {
			return nil, ErrIdempotencyInProgress
		}
		return res.Existing, nil
	}

	return nil, nil
}

// replayResult monta o resultado a partir da transaction persistida, sem
// recalcular nada e sem efeitos colaterais.
func replayResult(existing *domain.WagerTransaction) *ProcessWagerResult {
	balance, err := domain.NewMoney(0, existing.Amount.Currency())
	if err != nil {
		balance = domain.Money{}
	}
	if existing.ResultingBalance != nil {
		balance = *existing.ResultingBalance
	}
	return &ProcessWagerResult{
		Transaction: *existing,
		Balance:     balance,
		Replayed:    true,
	}
}

// recoverFromDuplicate é a recuperação única e determinística após perder
// uma corrida de unique: relê as identidades numa transação nova e decide
// replay/conflito/in-progress. Se nem assim houver linha (vencedor
// revertido), devolve o erro original da duplicata.
func (s *WagerService) recoverFromDuplicate(ctx context.Context, dupErr error, fp, providerID, key, externalID string) (*ProcessWagerResult, error) {
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, dupErr
	}
	defer func() {
		_ = pgxTx.Rollback(ctx)
	}()

	byKey, err := lookupByKey(ctx, s.transactions, pgxTx, providerID, key)
	if err != nil {
		return nil, dupErr
	}
	byExt, err := lookupByExternal(ctx, s.transactions, pgxTx, providerID, externalID)
	if err != nil {
		return nil, dupErr
	}

	existing, err := resolveIdentities(byKey, byExt, fp)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, dupErr
	}
	return replayResult(existing), nil
}

// resolveReversalReference localiza e valida a referência de uma reversão
// dentro da mesma transação PostgreSQL, com lock FOR UPDATE na linha da
// referência (ordem global wallet → reference). A referência nunca é
// modificada. Retorna a referência validada para prosseguir, ou um resultado
// pronto quando a reversão foi estacionada como PENDING_REFERENCE:
//   - referência inexistente -> própria tx persiste PENDING_REFERENCE
//   - referência PENDING/PENDING_REFERENCE -> própria tx persiste PENDING_REFERENCE
//   - referência REJECTED/FAILED -> própria tx persiste REJECTED (erro preservado)
//   - kind inválido/duplicata (A3.2) -> própria tx persiste REJECTED (erro preservado)
func (s *WagerService) resolveReversalReference(ctx context.Context, pgxTx pgx.Tx, tx *domain.WagerTransaction, wallet *domain.Wallet) (*domain.WagerTransaction, *ProcessWagerResult, error) {
	reference, err := s.transactions.GetByProviderExternalIDForUpdate(ctx, pgxTx, tx.ProviderID, tx.ReferenceExternalTransactionID)
	if err != nil {
		if errors.Is(err, domain.ErrTransactionNotFound) {
			res, err := s.persistPendingReference(ctx, pgxTx, tx, wallet)
			return nil, res, err
		}
		return nil, nil, err
	}

	if !reference.IsProcessed() {
		if reference.IsTerminal() {
			_, termErr := s.persistTerminalOutcome(ctx, pgxTx, tx, wallet, domain.ErrReferenceNotProcessed)
			return nil, nil, termErr
		}
		res, err := s.persistPendingReference(ctx, pgxTx, tx, wallet)
		return nil, res, err
	}

	var existingReversal *domain.WagerTransaction
	found, err := s.transactions.FindProcessedReversal(ctx, pgxTx, tx.ProviderID, tx.ReferenceExternalTransactionID)
	if err != nil {
		if !errors.Is(err, domain.ErrTransactionNotFound) {
			return nil, nil, err
		}
	} else {
		existingReversal = found
	}

	if err := domain.ValidateReversal(reference, existingReversal, tx); err != nil {
		_, termErr := s.persistTerminalOutcome(ctx, pgxTx, tx, wallet, err)
		return nil, nil, termErr
	}

	return reference, nil, nil
}

// persistPendingReference persiste a própria reversão como PENDING_REFERENCE
// (sem movimento, sem ledger) e devolve o resultado com erro nil: a operação
// foi aceita e será resolvida quando a referência chegar. Duplicata no
// INSERT cai na recuperação B3.1 como no fluxo feliz.
func (s *WagerService) persistPendingReference(ctx context.Context, pgxTx pgx.Tx, tx *domain.WagerTransaction, wallet *domain.Wallet) (*ProcessWagerResult, error) {
	if err := s.transactions.Create(ctx, pgxTx, tx); err != nil {
		if errors.Is(err, domain.ErrDuplicateIdempotencyKey) ||
			errors.Is(err, domain.ErrDuplicateExternalID) {
			_ = pgxTx.Rollback(ctx)
			return s.recoverFromDuplicate(ctx, err, tx.PayloadHash, tx.ProviderID, tx.IdempotencyKey, tx.ExternalTransactionID)
		}
		return nil, err
	}
	if err := tx.MarkPendingReference(); err != nil {
		return nil, err
	}
	if err := s.transactions.UpdateStatus(ctx, pgxTx, tx); err != nil {
		return nil, err
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit pending reference: %w", err)
	}
	return &ProcessWagerResult{
		Transaction: *tx,
		Balance:     wallet.Balance,
	}, nil
}

// persistTerminalOutcome persiste atomicamente o desfecho terminal de uma
// operação NEW que uma regra de negócio (ou overflow) impediu de prosseguir,
// e devolve o erro original da operação (para errors.Is) — nunca mascarado
// por códigos genéricos:
//
//	ErrInsufficientFunds    -> REJECTED + INSUFFICIENT_FUNDS (+ resulting balance)
//	ErrWalletPlayerMismatch -> REJECTED + PLAYER_WALLET_MISMATCH (+ resulting balance)
//	ErrCurrencyMismatch     -> REJECTED + CURRENCY_MISMATCH (+ resulting balance)
//	ErrInvalidReferenceKind -> REJECTED + INVALID_REFERENCE_KIND (+ resulting balance)
//	ErrReferenceNotProcessed -> REJECTED + REFERENCE_NOT_PROCESSED (+ resulting balance)
//	ErrDuplicateReversal    -> REJECTED + DUPLICATE_REVERSAL (+ resulting balance)
//	ErrOverflow             -> FAILED + OVERFLOW (sem resulting balance)
//
// A linha PENDING, a transição terminal e a ausência de efeito financeiro
// (sem wallet update, sem ledger) fazem parte do mesmo commit. Qualquer
// outro erro é propagado sem persistência: infraestrutura, contexto,
// wallet inexistente e validações estruturais não viram terminal.
func (s *WagerService) persistTerminalOutcome(ctx context.Context, pgxTx pgx.Tx, tx *domain.WagerTransaction, wallet *domain.Wallet, opErr error) (*ProcessWagerResult, error) {
	var failureCode string
	rejected := false
	switch {
	case errors.Is(opErr, domain.ErrInsufficientFunds):
		failureCode, rejected = "INSUFFICIENT_FUNDS", true
	case errors.Is(opErr, domain.ErrWalletPlayerMismatch):
		failureCode, rejected = "PLAYER_WALLET_MISMATCH", true
	case errors.Is(opErr, domain.ErrCurrencyMismatch):
		failureCode, rejected = "CURRENCY_MISMATCH", true
	case errors.Is(opErr, domain.ErrInvalidReferenceKind):
		failureCode, rejected = "INVALID_REFERENCE_KIND", true
	case errors.Is(opErr, domain.ErrReferenceNotProcessed):
		failureCode, rejected = "REFERENCE_NOT_PROCESSED", true
	case errors.Is(opErr, domain.ErrDuplicateReversal):
		failureCode, rejected = "DUPLICATE_REVERSAL", true
	case errors.Is(opErr, domain.ErrOverflow):
		failureCode, rejected = "OVERFLOW", false
	default:
		return nil, opErr
	}

	if err := s.transactions.Create(ctx, pgxTx, tx); err != nil {
		if errors.Is(err, domain.ErrDuplicateIdempotencyKey) ||
			errors.Is(err, domain.ErrDuplicateExternalID) {
			_ = pgxTx.Rollback(ctx)
			return s.recoverFromDuplicate(ctx, err, tx.PayloadHash, tx.ProviderID, tx.IdempotencyKey, tx.ExternalTransactionID)
		}
		return nil, errors.Join(opErr, fmt.Errorf("persist terminal outcome: %w", err))
	}

	if rejected {
		if err := tx.Reject(failureCode); err != nil {
			return nil, errors.Join(opErr, err)
		}
		balance := wallet.Balance
		tx.ResultingBalance = &balance
	} else {
		if err := tx.Fail(failureCode); err != nil {
			return nil, errors.Join(opErr, err)
		}
	}
	if err := s.transactions.UpdateStatus(ctx, pgxTx, tx); err != nil {
		return nil, errors.Join(opErr, err)
	}
	if err := pgxTx.Commit(ctx); err != nil {
		return nil, errors.Join(opErr, fmt.Errorf("commit terminal outcome: %w", err))
	}
	return nil, opErr
}

// applyMovement aplica o efeito financeiro na wallet em memória usando as
// operações seguras de Money. Não toca no banco.
func applyMovement(wallet *domain.Wallet, effect domain.FinancialEffect) error {
	switch effect.Direction {
	case domain.LedgerCredit:
		return wallet.Credit(effect.Amount)
	case domain.LedgerDebit:
		return wallet.Debit(effect.Amount)
	default:
		return domain.ErrInvalidLedgerDirection
	}
}

// newLedgerID gera um UUID via PostgreSQL, sem dependência extra.
func newLedgerID(ctx context.Context, pgxTx pgx.Tx) (string, error) {
	var id string
	if err := pgxTx.QueryRow(ctx, "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		return "", fmt.Errorf("generate ledger id: %w", err)
	}
	return id, nil
}
