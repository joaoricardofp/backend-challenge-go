package domain

import (
	"errors"
)

type WagerTransactionKind string

const (
	TransactionOpening  WagerTransactionKind = "OPENING"
	TransactionBet      WagerTransactionKind = "BET"
	TransactionWin      WagerTransactionKind = "WIN"
	TransactionLoss     WagerTransactionKind = "LOSS"
	TransactionRefund   WagerTransactionKind = "REFUND"
	TransactionRollback WagerTransactionKind = "ROLLBACK"
)

type WagerTransactionStatus string

const (
	TransactionPending          WagerTransactionStatus = "PENDING"
	TransactionPendingReference WagerTransactionStatus = "PENDING_REFERENCE"
	TransactionProcessed        WagerTransactionStatus = "PROCESSED"
	TransactionRejected         WagerTransactionStatus = "REJECTED"
	TransactionFailed           WagerTransactionStatus = "FAILED"
)

var (
	ErrInvalidTransactionKind      = errors.New("invalid transaction kind")
	ErrInvalidTransactionStatus    = errors.New("invalid transaction status")
	ErrInvalidLossAmount           = errors.New("loss amount must be zero")
	ErrTransactionNotPending       = errors.New("transaction is not pending")
	ErrTransactionNotFound         = errors.New("transaction not found")
	ErrDuplicateExternalID         = errors.New("external transaction id already exists")
	ErrDuplicateIdempotencyKey     = errors.New("idempotency key already exists")
	ErrIdempotencyConflict         = errors.New("idempotency conflict")
	ErrExternalTransactionConflict = errors.New("external transaction conflict")
	ErrInvalidReferenceKind        = errors.New("invalid reference kind")
	ErrReferenceNotProcessed       = errors.New("reference is not processed")
	ErrDuplicateReversal           = errors.New("reference already reversed")
)

type WagerTransaction struct {
	ID                             string
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           WagerTransactionKind
	Status                         WagerTransactionStatus
	Amount                         Money
	ReferenceExternalTransactionID string
	ResultingBalance               *Money
	FailureCode                    string
}

func NewWagerTransaction(
	id string,
	providerID string,
	externalTransactionID string,
	idempotencyKey string,
	payloadHash string,
	playerID string,
	walletID string,
	roundID string,
	gameID string,
	kind WagerTransactionKind,
	amount Money,
	referenceExternalTransactionID string,
) (*WagerTransaction, error) {
	if id == "" {
		return nil, errors.New("transaction id is required")
	}
	if providerID == "" {
		return nil, errors.New("provider id is required")
	}
	if externalTransactionID == "" {
		return nil, errors.New("external transaction id is required")
	}
	if idempotencyKey == "" {
		return nil, errors.New("idempotency key is required")
	}
	if payloadHash == "" {
		return nil, errors.New("payload hash is required")
	}
	if playerID == "" {
		return nil, errors.New("player id is required")
	}
	if walletID == "" {
		return nil, errors.New("wallet id is required")
	}
	switch kind {
	case TransactionBet,
		TransactionWin,
		TransactionLoss,
		TransactionRefund,
		TransactionRollback:
		if roundID == "" {
			return nil, errors.New("round id is required")
		}
		if gameID == "" {
			return nil, errors.New("game id is required")
		}
	case TransactionOpening:
	default:
		return nil, ErrInvalidTransactionKind
	}

	switch kind {
	case TransactionBet,
		TransactionWin,
		TransactionRefund,
		TransactionRollback:
		if amount.IsZero() {
			return nil, ErrInvalidAmount
		}
	case TransactionLoss:
		if !amount.IsZero() {
			return nil, ErrInvalidLossAmount
		}
	case TransactionOpening:
	}

	switch kind {
	case TransactionRefund,
		TransactionRollback:
		if referenceExternalTransactionID == "" {
			return nil, errors.New("reference external transaction id is required")
		}
	}

	return &WagerTransaction{
		ID:                             id,
		ProviderID:                     providerID,
		ExternalTransactionID:          externalTransactionID,
		IdempotencyKey:                 idempotencyKey,
		PayloadHash:                    payloadHash,
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        roundID,
		GameID:                         gameID,
		Kind:                           kind,
		Status:                         TransactionPending,
		Amount:                         amount,
		ReferenceExternalTransactionID: referenceExternalTransactionID,
	}, nil
}

// Complete marca a transação como processada (PROCESSED) e registra o saldo
// resultante da carteira. Válido a partir de PENDING ou PENDING_REFERENCE.
func (wt *WagerTransaction) Complete(resultingBalance Money) error {
	if wt.Status != TransactionPending && wt.Status != TransactionPendingReference {
		return ErrTransactionNotPending
	}

	if resultingBalance.Currency() != wt.Amount.Currency() {
		return ErrCurrencyMismatch
	}

	wt.Status = TransactionProcessed
	wt.ResultingBalance = &resultingBalance

	return nil
}

// MarkPendingReference marca a transação como aguardando referência
// (PENDING_REFERENCE). Válido somente a partir de PENDING.
func (wt *WagerTransaction) MarkPendingReference() error {
	if wt.Status != TransactionPending {
		return ErrTransactionNotPending
	}

	wt.Status = TransactionPendingReference

	return nil
}

// Reject marca a transação como rejeitada por regra de negócio (REJECTED)
// com um código de falha obrigatório. Válido a partir de PENDING ou
// PENDING_REFERENCE. Não confundir com Fail (falha de infraestrutura).
func (wt *WagerTransaction) Reject(failureCode string) error {
	if wt.Status != TransactionPending && wt.Status != TransactionPendingReference {
		return ErrTransactionNotPending
	}
	if failureCode == "" {
		return errors.New("failure code is required")
	}

	wt.Status = TransactionRejected
	wt.FailureCode = failureCode

	return nil
}

// Fail marca a transação como falha permanente de infraestrutura (FAILED)
// com um código de falha. Válido a partir de PENDING ou PENDING_REFERENCE.
func (wt *WagerTransaction) Fail(failureCode string) error {
	if wt.Status != TransactionPending && wt.Status != TransactionPendingReference {
		return ErrTransactionNotPending
	}
	if failureCode == "" {
		return errors.New("failure code is required")
	}

	wt.Status = TransactionFailed
	wt.FailureCode = failureCode

	return nil
}

// IsReversal retorna se a transação é uma reversão (REFUND ou ROLLBACK).
func (wt *WagerTransaction) IsReversal() bool {
	return wt.Kind == TransactionRefund || wt.Kind == TransactionRollback
}

// ValidateReversal avalia, apenas sobre objetos de domínio já carregados, se
// reversal pode reverter reference. existingReversal é uma reversão
// bem-sucedida (PROCESSED) já existente para a mesma referência, ou nil
// quando não há nenhuma. Nenhuma infraestrutura é acessada: a resolução da
// referência, as igualdades de provider/player/wallet/currency/round/amount
// e a busca por reversões existentes pertencem à camada de aplicação.
func ValidateReversal(reference *WagerTransaction, existingReversal *WagerTransaction, reversal *WagerTransaction) error {
	if reversal == nil {
		return errors.New("reversal transaction is required")
	}
	if reference == nil {
		return errors.New("reference transaction is required")
	}

	switch reversal.Kind {
	case TransactionRefund, TransactionRollback:
	default:
		return ErrInvalidTransactionKind
	}

	if !reference.IsProcessed() {
		return ErrReferenceNotProcessed
	}

	switch reversal.Kind {
	case TransactionRefund:
		if reference.Kind != TransactionBet {
			return ErrInvalidReferenceKind
		}
	case TransactionRollback:
		switch reference.Kind {
		case TransactionBet, TransactionWin, TransactionRefund:
		default:
			return ErrInvalidReferenceKind
		}
	}

	if existingReversal != nil && existingReversal.IsProcessed() {
		return ErrDuplicateReversal
	}

	return nil
}

// FinancialEffect descreve o efeito financeiro de uma transação: se há
// movimentação de saldo e, em caso afirmativo, em qual direção do ledger e
// de qual valor. Quando HasMovement é false, Direction e Amount não têm
// significado e um ledger não deve ser gerado.
type FinancialEffect struct {
	HasMovement bool
	Direction   LedgerDirection
	Amount      Money
}

// FinancialEffect calcula o efeito financeiro da transação sem mutar a
// transação, a referência ou qualquer wallet/ledger. É uma operação pura e
// determinística: o Money retornado carrega a currency da própria transação.
//
// Para ROLLBACK a direção depende do kind da referência já resolvida em
// memória (BET => CREDIT; WIN ou REFUND => DEBIT). O status da referência,
// a igualdade de amounts/campos e a existência de outra reversão pertencem
// às regras de A3.2 e à camada de aplicação, não a este mapeamento.
func (wt *WagerTransaction) FinancialEffect(reference *WagerTransaction) (FinancialEffect, error) {
	switch wt.Kind {
	case TransactionBet:
		return FinancialEffect{HasMovement: true, Direction: LedgerDebit, Amount: wt.Amount}, nil
	case TransactionWin:
		return FinancialEffect{HasMovement: true, Direction: LedgerCredit, Amount: wt.Amount}, nil
	case TransactionRefund:
		return FinancialEffect{HasMovement: true, Direction: LedgerCredit, Amount: wt.Amount}, nil
	case TransactionLoss:
		return FinancialEffect{}, nil
	case TransactionOpening:
		if wt.Amount.IsZero() {
			return FinancialEffect{}, nil
		}
		return FinancialEffect{HasMovement: true, Direction: LedgerCredit, Amount: wt.Amount}, nil
	case TransactionRollback:
		if reference == nil {
			return FinancialEffect{}, errors.New("reference transaction is required")
		}
		switch reference.Kind {
		case TransactionBet:
			return FinancialEffect{HasMovement: true, Direction: LedgerCredit, Amount: wt.Amount}, nil
		case TransactionWin, TransactionRefund:
			return FinancialEffect{HasMovement: true, Direction: LedgerDebit, Amount: wt.Amount}, nil
		default:
			return FinancialEffect{}, ErrInvalidReferenceKind
		}
	default:
		return FinancialEffect{}, ErrInvalidTransactionKind
	}
}

// IsPending retorna se a transação está pendente.
func (wt *WagerTransaction) IsPending() bool {
	return wt.Status == TransactionPending
}

// IsPendingReference retorna se a transação aguarda referência.
func (wt *WagerTransaction) IsPendingReference() bool {
	return wt.Status == TransactionPendingReference
}

// IsProcessed retorna se a transação foi processada.
func (wt *WagerTransaction) IsProcessed() bool {
	return wt.Status == TransactionProcessed
}

// IsRejected retorna se a transação foi rejeitada por regra de negócio.
func (wt *WagerTransaction) IsRejected() bool {
	return wt.Status == TransactionRejected
}

// IsFailed retorna se a transação falhou.
func (wt *WagerTransaction) IsFailed() bool {
	return wt.Status == TransactionFailed
}

// IsTerminal retorna se a transação está em estado final (PROCESSED,
// REJECTED ou FAILED) e portanto não admite nova transição.
func (wt *WagerTransaction) IsTerminal() bool {
	return wt.IsProcessed() || wt.IsRejected() || wt.IsFailed()
}
