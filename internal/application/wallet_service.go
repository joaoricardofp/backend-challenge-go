package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

var (
	ErrWalletAlreadyExists = errors.New("wallet already exists for player and currency")
	ErrWalletNotFound      = errors.New("wallet not found")

	// InternalProviderID é o UUID fixo para o serviço interno de abertura de carteiras.
	// Deve ser um UUID válido (não "internal" que não é UUID).
	InternalProviderID = "00000000-0000-0000-0000-000000000001"
)

type OpenWalletInput struct {
	WalletID       string
	PlayerID       string
	InitialBalance domain.Money
}

type OpenWalletResult struct {
	Wallet      domain.Wallet
	Transaction *domain.WagerTransaction
	LedgerEntry *domain.LedgerEntry
	Replayed    bool
}

type GetWalletInput struct {
	WalletID string
}

type GetWalletResult struct {
	Wallet domain.Wallet
}

type ListWalletLedgerInput struct {
	WalletID string
	Cursor   string
	Limit    int
}

type LedgerPage struct {
	Entries    []domain.LedgerEntry
	NextCursor string
}

type GetTransactionInput struct {
	TransactionID string
}

type GetTransactionResult struct {
	Transaction domain.WagerTransaction
}

type GetTransactionByProviderExternalIDInput struct {
	ProviderID            string
	ExternalTransactionID string
}

type GetTransactionByProviderExternalIDResult struct {
	Transaction domain.WagerTransaction
}

// ReconciliationIssueType representa o tipo de inconsistência encontrada na reconciliação.
type ReconciliationIssueType string

const (
	IssueBalanceMismatch            ReconciliationIssueType = "BALANCE_MISMATCH"
	IssueFirstBalanceBeforeMismatch ReconciliationIssueType = "FIRST_BALANCE_BEFORE_MISMATCH"
	IssueBalanceChainMismatch       ReconciliationIssueType = "BALANCE_CHAIN_MISMATCH"
	IssueEntryBalanceMismatch       ReconciliationIssueType = "ENTRY_BALANCE_MISMATCH"
	IssueNegativeBalance            ReconciliationIssueType = "NEGATIVE_BALANCE"
	IssueInvalidLedgerAmount        ReconciliationIssueType = "INVALID_LEDGER_AMOUNT"
	IssueUnknownDirection           ReconciliationIssueType = "UNKNOWN_DIRECTION"
)

// ReconciliationIssue representa uma inconsistência encontrada durante a reconciliação.
type ReconciliationIssue struct {
	Type     ReconciliationIssueType `json:"type"`
	EntryID  string                  `json:"entryId,omitempty"`
	Expected string                  `json:"expected,omitempty"`
	Actual   string                  `json:"actual,omitempty"`
	Message  string                  `json:"message"`
}

// ReconciliationInput representa a entrada para a operação de reconciliação.
type ReconciliationInput struct {
	WalletID string
}

// ReconciliationResult representa o resultado da reconciliação de uma carteira.
type ReconciliationResult struct {
	WalletID          string                `json:"walletId"`
	StoredBalance     domain.Money          `json:"storedBalance"`
	CalculatedBalance domain.Money          `json:"calculatedBalance"`
	Difference        domain.Money          `json:"difference"`
	Consistent        bool                  `json:"consistent"`
	CheckedEntries    int                   `json:"checkedEntries"`
	Issues            []ReconciliationIssue `json:"issues"`
}

type WalletService struct {
	pool         *pgxpool.Pool
	wallets      *postgres.WalletRepository
	transactions *postgres.WagerTransactionRepository
	ledger       *postgres.LedgerRepository
	outbox       *postgres.OutboxRepository
}

func NewWalletService(
	pool *pgxpool.Pool,
	wallets *postgres.WalletRepository,
	transactions *postgres.WagerTransactionRepository,
	ledger *postgres.LedgerRepository,
	outbox *postgres.OutboxRepository,
) *WalletService {
	return &WalletService{
		pool:         pool,
		wallets:      wallets,
		transactions: transactions,
		ledger:       ledger,
		outbox:       outbox,
	}
}

func (s *WalletService) OpenWallet(ctx context.Context, input OpenWalletInput) (*OpenWalletResult, error) {
	if input.InitialBalance.IsZero() {
		wallet, err := domain.NewWallet(input.WalletID, input.PlayerID, input.InitialBalance.Currency())
		if err != nil {
			return nil, err
		}

		pgxTx, err := s.pool.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin transaction: %w", err)
		}
		defer func() {
			_ = pgxTx.Rollback(ctx)
		}()

		if err := s.wallets.CreateWithTx(ctx, pgxTx, wallet); err != nil {
			if errors.Is(err, postgres.ErrWalletPlayerCurrencyConflict) {
				return nil, ErrWalletAlreadyExists
			}
			return nil, fmt.Errorf("create wallet: %w", err)
		}

		if err := pgxTx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit transaction: %w", err)
		}

		return &OpenWalletResult{
			Wallet:   *wallet,
			Replayed: false,
		}, nil
	}

	openingTx, err := domain.NewWagerTransaction(
		input.WalletID,
		InternalProviderID,
		"opening-"+input.WalletID,
		"opening-"+input.WalletID,
		"opening",
		input.PlayerID,
		input.WalletID,
		"",
		"",
		domain.TransactionOpening,
		input.InitialBalance,
		"",
	)
	if err != nil {
		return nil, err
	}

	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = pgxTx.Rollback(ctx)
	}()

	wallet, err := domain.NewWallet(input.WalletID, input.PlayerID, input.InitialBalance.Currency())
	if err != nil {
		return nil, err
	}

	// Credit the initial balance before persisting, so wallet is created with correct balance and version 1
	if err := wallet.Credit(input.InitialBalance); err != nil {
		return nil, err
	}

	if err := s.wallets.CreateWithTx(ctx, pgxTx, wallet); err != nil {
		if errors.Is(err, postgres.ErrWalletPlayerCurrencyConflict) {
			return nil, ErrWalletAlreadyExists
		}
		return nil, fmt.Errorf("create wallet: %w", err)
	}

	balanceBefore := domain.Money{} // zero money for ledger (balance before opening)
	_ = balanceBefore               // avoid unused warning if needed, but we need the actual zero money

	// Create zero money of the same currency for balanceBefore
	zeroMoney, err := domain.NewMoney(0, input.InitialBalance.Currency())
	if err != nil {
		return nil, err
	}
	balanceBefore = zeroMoney

	if err := openingTx.Complete(wallet.Balance); err != nil {
		return nil, err
	}
	if err := s.transactions.Create(ctx, pgxTx, openingTx); err != nil {
		return nil, err
	}

	ledgerID, err := newLedgerID(ctx, pgxTx)
	if err != nil {
		return nil, err
	}
	ledgerEntry, err := domain.NewLedgerEntry(
		ledgerID,
		wallet.ID,
		openingTx.ID,
		domain.LedgerCredit,
		input.InitialBalance,
		balanceBefore,
		wallet.Balance,
	)
	if err != nil {
		return nil, err
	}
	if err := s.ledger.Create(ctx, pgxTx, ledgerEntry); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	if err := s.persistDecisionEvent(ctx, pgxTx, openingTx, now); err != nil {
		return nil, err
	}
	if err := s.persistBalanceChangedEvent(ctx, pgxTx, ledgerEntry, wallet.Version, openingTx.IdempotencyKey, now); err != nil {
		return nil, err
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return &OpenWalletResult{
		Wallet:      *wallet,
		Transaction: openingTx,
		LedgerEntry: ledgerEntry,
		Replayed:    false,
	}, nil
}

func (s *WalletService) GetWallet(ctx context.Context, input GetWalletInput) (*GetWalletResult, error) {
	wallet, err := s.wallets.GetByID(ctx, input.WalletID)
	if err != nil {
		if errors.Is(err, domain.ErrWalletNotFound) {
			return nil, ErrWalletNotFound
		}
		return nil, err
	}
	return &GetWalletResult{Wallet: *wallet}, nil
}

func (s *WalletService) ListWalletLedger(ctx context.Context, input ListWalletLedgerInput) (*LedgerPage, error) {
	if input.Limit <= 0 {
		input.Limit = 50
	}
	if input.Limit > 100 {
		input.Limit = 100
	}

	entries, nextCursor, err := s.ledger.ListByWallet(ctx, input.WalletID, input.Cursor, input.Limit)
	if err != nil {
		return nil, err
	}

	return &LedgerPage{
		Entries:    entries,
		NextCursor: nextCursor,
	}, nil
}

func (s *WalletService) GetTransaction(ctx context.Context, input GetTransactionInput) (*GetTransactionResult, error) {
	tx, err := s.transactions.GetByID(ctx, input.TransactionID)
	if err != nil {
		if errors.Is(err, domain.ErrTransactionNotFound) {
			return nil, domain.ErrTransactionNotFound
		}
		return nil, err
	}
	return &GetTransactionResult{Transaction: *tx}, nil
}

func (s *WalletService) GetTransactionByProviderExternalID(ctx context.Context, input GetTransactionByProviderExternalIDInput) (*GetTransactionByProviderExternalIDResult, error) {
	tx, err := s.transactions.GetByProviderExternalID(ctx, nil, input.ProviderID, input.ExternalTransactionID)
	if err != nil {
		if errors.Is(err, domain.ErrTransactionNotFound) {
			return nil, domain.ErrTransactionNotFound
		}
		return nil, err
	}
	return &GetTransactionByProviderExternalIDResult{Transaction: *tx}, nil
}

func (s *WalletService) persistDecisionEvent(ctx context.Context, pgxTx pgx.Tx, tx *domain.WagerTransaction, now time.Time) error {
	var ev *domain.WagerEvent
	var err error
	switch {
	case tx.IsProcessed():
		ev, err = domain.NewWagerTransactionProcessedEvent(tx.ID, *tx, now)
	case tx.IsRejected():
		ev, err = domain.NewWagerTransactionRejectedEvent(tx.ID, *tx, now)
	case tx.IsFailed():
		ev, err = domain.NewWagerTransactionFailedEvent(tx.ID, *tx, now)
	case tx.IsPendingReference():
		ev, err = domain.NewWagerTransactionPendingReferenceEvent(tx.ID, *tx, now)
	default:
		return fmt.Errorf("outbox: no decision event for status %q", tx.Status)
	}
	if err != nil {
		return fmt.Errorf("outbox: build decision event: %w", err)
	}
	return s.insertOutboxEvent(ctx, pgxTx, ev, tx.ID, now)
}

func (s *WalletService) persistBalanceChangedEvent(ctx context.Context, pgxTx pgx.Tx, entry *domain.LedgerEntry, walletVersion int64, correlationID string, now time.Time) error {
	if entry == nil {
		return errors.New("outbox: missing ledger entry for balance event")
	}
	ev, err := domain.NewWalletBalanceChangedEvent(entry.ID, *entry, walletVersion, correlationID, now)
	if err != nil {
		return fmt.Errorf("outbox: build balance event: %w", err)
	}
	return s.insertOutboxEvent(ctx, pgxTx, ev, entry.TransactionID, now)
}

func (s *WalletService) insertOutboxEvent(ctx context.Context, pgxTx pgx.Tx, ev *domain.WagerEvent, aggregateID string, now time.Time) error {
	payload, err := ev.Payload()
	if err != nil {
		return err
	}
	if err := s.outbox.Create(ctx, pgxTx, &postgres.OutboxEvent{
		ID:          ev.EventID,
		EventType:   ev.EventType,
		AggregateID: aggregateID,
		Payload:     payload,
		OccurredAt:  now,
	}); err != nil {
		return fmt.Errorf("outbox: persist %s: %w", ev.EventType, err)
	}
	return nil
}

// ReconcileWallet executa a reconciliação financeira de uma carteira.
// A reconciliação é uma operação somente leitura que compara o saldo armazenado
// na wallet com o saldo calculado a partir do ledger, validando também a
// integridade da cadeia de saldos do ledger.
func (s *WalletService) ReconcileWallet(ctx context.Context, input ReconciliationInput) (*ReconciliationResult, error) {
	// Buscar a wallet para obter o saldo armazenado
	wallet, err := s.wallets.GetByID(ctx, input.WalletID)
	if err != nil {
		if errors.Is(err, domain.ErrWalletNotFound) {
			return nil, ErrWalletNotFound
		}
		return nil, err
	}

	// Buscar todas as entradas do ledger em ordem determinística
	entries, err := s.ledger.ListAllByWallet(ctx, input.WalletID)
	if err != nil {
		return nil, err
	}

	storedBalance := wallet.Balance
	currency := storedBalance.Currency()

	// Calcular saldo a partir do ledger
	calculatedBalance, issues := s.calculateBalanceFromLedger(entries, currency)
	checkedEntries := len(entries)

	// Calcular diferença como magnitude |stored - calculated| com a currency
	// da carteira. domain.Money não representa valores negativos, então a
	// direção que falha na subtração indica qual operando é maior.
	difference, err := storedBalance.Sub(calculatedBalance)
	if err != nil {
		difference, _ = calculatedBalance.Sub(storedBalance)
	}

	// Divergência entre saldo persistido e saldo reconstruído a partir do
	// ledger é reportada como issue própria.
	if storedBalance.Cents() != calculatedBalance.Cents() {
		issues = append(issues, ReconciliationIssue{
			Type:     IssueBalanceMismatch,
			Expected: calculatedBalance.AmountString(),
			Actual:   storedBalance.AmountString(),
			Message:  "stored wallet balance does not match balance reconstructed from ledger",
		})
	}

	consistent := len(issues) == 0 && storedBalance.Cents() == calculatedBalance.Cents()

	return &ReconciliationResult{
		WalletID:          input.WalletID,
		StoredBalance:     storedBalance,
		CalculatedBalance: calculatedBalance,
		Difference:        difference,
		Consistent:        consistent,
		CheckedEntries:    checkedEntries,
		Issues:            issues,
	}, nil
}

// calculateBalanceFromLedger reconstrói o saldo a partir do ledger e valida
// a integridade da cadeia de saldos. Retorna o saldo calculado e lista de issues.
func (s *WalletService) calculateBalanceFromLedger(entries []domain.LedgerEntry, currency string) (domain.Money, []ReconciliationIssue) {
	var issues []ReconciliationIssue

	// Saldo inicial esperado é zero
	expectedBalance, _ := domain.NewMoney(0, currency)
	calculatedBalance := expectedBalance

	if len(entries) == 0 {
		// Ledger vazio: saldo calculado deve ser zero
		return calculatedBalance, issues
	}

	// Validar primeira entrada: balanceBefore deve ser zero
	first := entries[0]
	if first.BalanceBefore.Cents() != 0 {
		issues = append(issues, ReconciliationIssue{
			Type:     IssueFirstBalanceBeforeMismatch,
			EntryID:  first.ID,
			Expected: "0.00",
			Actual:   first.BalanceBefore.AmountString(),
			Message:  "first ledger entry balanceBefore must be zero",
		})
	}

	var previousBalanceAfter domain.Money
	previousBalanceAfter, _ = domain.NewMoney(0, currency)

	for i, entry := range entries {
		// Validar direção
		if entry.Direction != domain.LedgerCredit && entry.Direction != domain.LedgerDebit {
			issues = append(issues, ReconciliationIssue{
				Type:     IssueUnknownDirection,
				EntryID:  entry.ID,
				Expected: "CREDIT or DEBIT",
				Actual:   string(entry.Direction),
				Message:  "invalid ledger direction",
			})
			continue
		}

		// Validar amount > 0
		if entry.Amount.IsZero() {
			issues = append(issues, ReconciliationIssue{
				Type:     IssueInvalidLedgerAmount,
				EntryID:  entry.ID,
				Expected: "> 0",
				Actual:   "0.00",
				Message:  "ledger amount must be positive",
			})
		}

		// Validar matemática da entrada: balanceAfter = balanceBefore ± amount
		var expectedBalanceAfter domain.Money
		var err error
		switch entry.Direction {
		case domain.LedgerCredit:
			expectedBalanceAfter, err = entry.BalanceBefore.Add(entry.Amount)
		case domain.LedgerDebit:
			expectedBalanceAfter, err = entry.BalanceBefore.Sub(entry.Amount)
		}
		if err != nil {
			issues = append(issues, ReconciliationIssue{
				Type:     IssueEntryBalanceMismatch,
				EntryID:  entry.ID,
				Expected: "valid calculation",
				Actual:   "calculation error",
				Message:  "failed to calculate expected balanceAfter",
			})
		} else if expectedBalanceAfter.Cents() != entry.BalanceAfter.Cents() {
			issues = append(issues, ReconciliationIssue{
				Type:     IssueEntryBalanceMismatch,
				EntryID:  entry.ID,
				Expected: expectedBalanceAfter.AmountString(),
				Actual:   entry.BalanceAfter.AmountString(),
				Message:  "entry balanceAfter does not match balanceBefore ± amount",
			})
		}

		// Validar continuidade da cadeia (exceto primeira entrada)
		if i > 0 {
			if entry.BalanceBefore.Cents() != previousBalanceAfter.Cents() {
				issues = append(issues, ReconciliationIssue{
					Type:     IssueBalanceChainMismatch,
					EntryID:  entry.ID,
					Expected: previousBalanceAfter.AmountString(),
					Actual:   entry.BalanceBefore.AmountString(),
					Message:  "entry balanceBefore does not match previous entry balanceAfter",
				})
			}
		}

		// Detectar saldo negativo em qualquer ponto
		if entry.BalanceAfter.Cents() < 0 {
			issues = append(issues, ReconciliationIssue{
				Type:     IssueNegativeBalance,
				EntryID:  entry.ID,
				Expected: ">= 0",
				Actual:   entry.BalanceAfter.AmountString(),
				Message:  "negative balance detected in ledger",
			})
		}

		// Aplicar movimento ao saldo calculado
		switch entry.Direction {
		case domain.LedgerCredit:
			calculatedBalance, _ = calculatedBalance.Add(entry.Amount)
		case domain.LedgerDebit:
			calculatedBalance, _ = calculatedBalance.Sub(entry.Amount)
		}

		previousBalanceAfter = entry.BalanceAfter
	}

	// Validar saldo final calculado vs última entrada do ledger
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		if calculatedBalance.Cents() != last.BalanceAfter.Cents() {
			issues = append(issues, ReconciliationIssue{
				Type:     IssueBalanceMismatch,
				EntryID:  last.ID,
				Expected: last.BalanceAfter.AmountString(),
				Actual:   calculatedBalance.AmountString(),
				Message:  "calculated final balance does not match last ledger entry balanceAfter",
			})
		}
	}

	return calculatedBalance, issues
}
