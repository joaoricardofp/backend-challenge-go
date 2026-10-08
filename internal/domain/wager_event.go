package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Tipos de evento de saída (B3.11, README §11). Cada tipo tem versão própria
// definida pelo construtor (sempre 1 nesta etapa). WagerTransactionFailed é
// uma extensão documentada: o README exige eventos para decisões persistidas
// e o domínio distingue REJECTED (regra de negócio, com resulting balance)
// de FAILED (OVERFLOW, sem resulting balance); rotular FAILED como Rejected
// perderia essa distinção e as semânticas de saldo.
const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWagerTransactionFailed           = "WagerTransactionFailed"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
)

// WagerEventVersion é a versão de todos os construtores desta etapa.
const WagerEventVersion = 1

// eventMoney espelha o contrato externo {"amount":"25.00","currency":"BRL"}:
// strings decimais com duas casas via AmountString, nunca float.
type eventMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func toEventMoney(m Money) eventMoney {
	return eventMoney{Amount: m.AmountString(), Currency: m.Currency()}
}

// WagerTransactionData é o snapshot imutável da decisão persistida sobre uma
// wager_transaction. Inclui os identificadores persistidos na linha (para
// OPENING interno, carregam identidades internas — contrato uniforme, sem
// schema condicional). resultingBalance está presente em PROCESSED e
// REJECTED, ausente em FAILED e PENDING_REFERENCE, seguindo o domínio.
// failureCode só existe em REJECTED/FAILED.
type WagerTransactionData struct {
	TransactionID                  string      `json:"transactionId"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	IdempotencyKey                 string      `json:"idempotencyKey"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId,omitempty"`
	GameID                         string      `json:"gameId,omitempty"`
	Kind                           string      `json:"kind"`
	Status                         string      `json:"status"`
	Money                          eventMoney  `json:"money"`
	ResultingBalance               *eventMoney `json:"resultingBalance,omitempty"`
	FailureCode                    string      `json:"failureCode,omitempty"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}

// WalletBalanceData é o snapshot imutável de uma alteração efetiva de saldo
// (README §11): wallet, transação de origem, direção, valor, saldos antes e
// depois e a versão da wallet após a mutação.
type WalletBalanceData struct {
	WalletID      string     `json:"walletId"`
	TransactionID string     `json:"transactionId"`
	Direction     string     `json:"direction"`
	Money         eventMoney `json:"money"`
	BalanceBefore eventMoney `json:"balanceBefore"`
	BalanceAfter  eventMoney `json:"balanceAfter"`
	WalletVersion int64      `json:"walletVersion"`
}

// WagerEvent é o envelope de saída (README §11): eventId estável,
// eventType, aggregateId (sempre o ID da wager_transaction de origem),
// correlationId (a idempotencyKey: estável entre redeliveries HTTP/SQS da
// mesma operação), occurredAt UTC em RFC 3339, version e data tipado.
// causationId é opcional pelo README e fica para a etapa do worker, quando
// houver encadeamento de eventos.
type WagerEvent struct {
	EventID       string `json:"eventId"`
	EventType     string `json:"eventType"`
	AggregateID   string `json:"aggregateId"`
	CorrelationID string `json:"correlationId"`
	OccurredAt    string `json:"occurredAt"`
	Version       int    `json:"version"`
	Data          any    `json:"data"`
}

// Payload serializa o envelope deterministicamente (ordem de campos da
// struct, sem maps): o mesmo evento sempre gera os mesmos bytes. Snapshot
// imutável para a coluna JSONB da outbox.
func (e *WagerEvent) Payload() ([]byte, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal wager event: %w", err)
	}
	return raw, nil
}

func transactionData(tx WagerTransaction) (WagerTransactionData, error) {
	if tx.ID == "" || tx.ProviderID == "" || tx.ExternalTransactionID == "" ||
		tx.IdempotencyKey == "" || tx.PlayerID == "" || tx.WalletID == "" {
		return WagerTransactionData{}, errors.New("wager event requires persisted transaction identities")
	}
	data := WagerTransactionData{
		TransactionID:                  tx.ID,
		ProviderID:                     tx.ProviderID,
		ExternalTransactionID:          tx.ExternalTransactionID,
		IdempotencyKey:                 tx.IdempotencyKey,
		PlayerID:                       tx.PlayerID,
		WalletID:                       tx.WalletID,
		RoundID:                        tx.RoundID,
		GameID:                         tx.GameID,
		Kind:                           string(tx.Kind),
		Status:                         string(tx.Status),
		Money:                          toEventMoney(tx.Amount),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID,
	}
	if tx.ResultingBalance != nil {
		bal := toEventMoney(*tx.ResultingBalance)
		data.ResultingBalance = &bal
	}
	if tx.FailureCode != "" {
		data.FailureCode = tx.FailureCode
	}
	return data, nil
}

func newEnvelope(eventID, eventType, aggregateID, correlationID string, occurredAt time.Time, data any) (*WagerEvent, error) {
	if eventID == "" {
		return nil, errors.New("wager event id is required")
	}
	if aggregateID == "" {
		return nil, errors.New("wager event aggregate id is required")
	}
	if correlationID == "" {
		return nil, errors.New("wager event correlation id is required")
	}
	return &WagerEvent{
		EventID:       eventID,
		EventType:     eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		OccurredAt:    occurredAt.UTC().Format(time.RFC3339),
		Version:       WagerEventVersion,
		Data:          data,
	}, nil
}

// NewWagerTransactionProcessedEvent constrói o evento de conclusão
// bem-sucedida (inclui LOSS sem movimento). Exige tx PROCESSED com
// resulting balance, seguindo o domínio.
func NewWagerTransactionProcessedEvent(eventID string, tx WagerTransaction, occurredAt time.Time) (*WagerEvent, error) {
	if !tx.IsProcessed() {
		return nil, fmt.Errorf("processed event requires PROCESSED transaction, got %q", tx.Status)
	}
	if tx.ResultingBalance == nil {
		return nil, errors.New("processed event requires resulting balance")
	}
	data, err := transactionData(tx)
	if err != nil {
		return nil, err
	}
	return newEnvelope(eventID, EventWagerTransactionProcessed, tx.ID, tx.IdempotencyKey, occurredAt, data)
}

// NewWagerTransactionRejectedEvent constrói o evento de rejeição definitiva
// por regra de negócio. Exige tx REJECTED com failure code e resulting
// balance, seguindo o domínio.
func NewWagerTransactionRejectedEvent(eventID string, tx WagerTransaction, occurredAt time.Time) (*WagerEvent, error) {
	if !tx.IsRejected() {
		return nil, fmt.Errorf("rejected event requires REJECTED transaction, got %q", tx.Status)
	}
	if tx.FailureCode == "" {
		return nil, errors.New("rejected event requires failure code")
	}
	if tx.ResultingBalance == nil {
		return nil, errors.New("rejected event requires resulting balance")
	}
	data, err := transactionData(tx)
	if err != nil {
		return nil, err
	}
	return newEnvelope(eventID, EventWagerTransactionRejected, tx.ID, tx.IdempotencyKey, occurredAt, data)
}

// NewWagerTransactionFailedEvent constrói o evento de falha permanente
// (OVERFLOW). Exige tx FAILED com failure code e SEM resulting balance,
// seguindo a semântica de Fail do domínio.
func NewWagerTransactionFailedEvent(eventID string, tx WagerTransaction, occurredAt time.Time) (*WagerEvent, error) {
	if !tx.IsFailed() {
		return nil, fmt.Errorf("failed event requires FAILED transaction, got %q", tx.Status)
	}
	if tx.FailureCode == "" {
		return nil, errors.New("failed event requires failure code")
	}
	if tx.ResultingBalance != nil {
		return nil, errors.New("failed event must not carry resulting balance")
	}
	data, err := transactionData(tx)
	if err != nil {
		return nil, err
	}
	return newEnvelope(eventID, EventWagerTransactionFailed, tx.ID, tx.IdempotencyKey, occurredAt, data)
}

// NewWagerTransactionPendingReferenceEvent constrói o evento de espera pela
// referência. Exige tx PENDING_REFERENCE, sem failure nem resulting.
func NewWagerTransactionPendingReferenceEvent(eventID string, tx WagerTransaction, occurredAt time.Time) (*WagerEvent, error) {
	if !tx.IsPendingReference() {
		return nil, fmt.Errorf("pending reference event requires PENDING_REFERENCE transaction, got %q", tx.Status)
	}
	data, err := transactionData(tx)
	if err != nil {
		return nil, err
	}
	return newEnvelope(eventID, EventWagerTransactionPendingReference, tx.ID, tx.IdempotencyKey, occurredAt, data)
}

// NewWalletBalanceChangedEvent constrói o evento de alteração efetiva de
// saldo a partir do ledger entry persistido e da versão resultante da
// wallet. Só existe quando houve movimento (a entrada do ledger só é criada
// nesse caso).
func NewWalletBalanceChangedEvent(eventID string, entry LedgerEntry, walletVersion int64, correlationID string, occurredAt time.Time) (*WagerEvent, error) {
	if entry.ID == "" || entry.WalletID == "" || entry.TransactionID == "" {
		return nil, errors.New("balance event requires persisted ledger identities")
	}
	if entry.Direction != LedgerCredit && entry.Direction != LedgerDebit {
		return nil, ErrInvalidLedgerDirection
	}
	if walletVersion < 1 {
		return nil, errors.New("balance event requires wallet version")
	}
	data := WalletBalanceData{
		WalletID:      entry.WalletID,
		TransactionID: entry.TransactionID,
		Direction:     string(entry.Direction),
		Money:         toEventMoney(entry.Amount),
		BalanceBefore: toEventMoney(entry.BalanceBefore),
		BalanceAfter:  toEventMoney(entry.BalanceAfter),
		WalletVersion: walletVersion,
	}
	return newEnvelope(eventID, EventWalletBalanceChanged, entry.TransactionID, correlationID, occurredAt, data)
}
