package domain

import (
	"errors"
)

type WagerTransactionKind string

const (
	TransactionDebit  WagerTransactionKind = "DEBIT"
	TransactionCredit WagerTransactionKind = "CREDIT"
)

type WagerTransactionStatus string

const (
	TransactionPending   WagerTransactionStatus = "PENDING"
	TransactionCompleted WagerTransactionStatus = "COMPLETED"
	TransactionFailed    WagerTransactionStatus = "FAILED"
)

var (
	ErrInvalidTransactionKind   = errors.New("invalid transaction kind")
	ErrInvalidTransactionStatus = errors.New("invalid transaction status")
	ErrTransactionNotPending    = errors.New("transaction is not pending")
	ErrTransactionNotFound      = errors.New("transaction not found")
	ErrDuplicateExternalID      = errors.New("external transaction id already exists")
	ErrDuplicateIdempotencyKey  = errors.New("idempotency key already exists")
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
	if roundID == "" {
		return nil, errors.New("round id is required")
	}
	if gameID == "" {
		return nil, errors.New("game id is required")
	}
	if kind != TransactionDebit && kind != TransactionCredit {
		return nil, ErrInvalidTransactionKind
	}
	if amount.IsZero() {
		return nil, ErrInvalidAmount
	}

	return &WagerTransaction{
		ID:                    id,
		ProviderID:            providerID,
		ExternalTransactionID: externalTransactionID,
		IdempotencyKey:        idempotencyKey,
		PayloadHash:           payloadHash,
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               roundID,
		GameID:                gameID,
		Kind:                  kind,
		Status:                TransactionPending,
		Amount:                amount,
	}, nil
}

// Complete marca a transação como concluída e registra o saldo resultante da carteira.
func (wt *WagerTransaction) Complete(resultingBalance Money) error {
	if wt.Status != TransactionPending {
		return ErrTransactionNotPending
	}

	if resultingBalance.Currency() != wt.Amount.Currency() {
		return ErrCurrencyMismatch
	}

	wt.Status = TransactionCompleted
	wt.ResultingBalance = &resultingBalance

	return nil
}

// Fail marca a transação como falhada com um código de falha.
func (wt *WagerTransaction) Fail(failureCode string) error {
	if wt.Status != TransactionPending {
		return ErrTransactionNotPending
	}
	if failureCode == "" {
		return errors.New("failure code is required")
	}

	wt.Status = TransactionFailed
	wt.FailureCode = failureCode

	return nil
}

// IsPending retorna se a transação está pendente.
func (wt *WagerTransaction) IsPending() bool {
	return wt.Status == TransactionPending
}

// IsCompleted retorna se a transação foi concluída.
func (wt *WagerTransaction) IsCompleted() bool {
	return wt.Status == TransactionCompleted
}

// IsFailed retorna se a transação falhou.
func (wt *WagerTransaction) IsFailed() bool {
	return wt.Status == TransactionFailed
}
