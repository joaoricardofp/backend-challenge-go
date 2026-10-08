// Package sqs define o contrato da mensagem SQS de wagering (README §10)
// sem implementar consumer, polling ou ack. É um adapter futuro do mesmo
// WagerService.Process usado pelo HTTP: SQS JSON -> DTO -> validação ->
// domínio -> application.ProcessWagerInput.
package sqs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// WagerTransactionRequested é o único type aceito no envelope.
const WagerTransactionRequested = "WagerTransactionRequested"

// ErrOpeningNotAllowed indica kind OPENING em mensagem externa: operação
// interna, barrada na fronteira (o domínio continua aceitando para uso interno).
var ErrOpeningNotAllowed = errors.New("opening not allowed in external message")

// ErrInvalidMessage indica envelope/JSON inválido ou campo obrigatório ausente.
var ErrInvalidMessage = errors.New("invalid wager message")

// moneyMessage espelha {"amount":"25.00","currency":"BRL"} (sempre string,
// nunca número JSON).
type moneyMessage struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// WagerData carrega os campos de negócio (README §10 data + referência
// opcional para reversões, consistente com o contrato HTTP).
type WagerData struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	IdempotencyKey                 string       `json:"idempotencyKey"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          moneyMessage `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId"`
}

// WagerMessage é o envelope SQS (README §10).
type WagerMessage struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt string    `json:"occurredAt"`
	Data       WagerData `json:"data"`
}

// MessageHash é o SHA-256 hex dos bytes crus da mensagem: identidade de
// auditoria da Inbox, estável entre redeliveries. Distinto do fingerprint
// canônico da operação (message identity != request fingerprint).
func MessageHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// ParseWagerMessage desserializa com JSON estrito (campos desconhecidos
// rejeitados) e valida envelope + money (via domínio, sem float) + kind.
// Não toca em banco nem no service.
func ParseWagerMessage(raw []byte) (WagerMessage, error) {
	var msg WagerMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&msg); err != nil {
		return WagerMessage{}, fmt.Errorf("%w: malformed JSON: %v", ErrInvalidMessage, err)
	}
	if msg.MessageID == "" {
		return WagerMessage{}, fmt.Errorf("%w: messageId is required", ErrInvalidMessage)
	}
	if msg.Type != WagerTransactionRequested {
		return WagerMessage{}, fmt.Errorf("%w: unsupported type %q", ErrInvalidMessage, msg.Type)
	}
	if _, err := time.Parse(time.RFC3339, msg.OccurredAt); err != nil {
		return WagerMessage{}, fmt.Errorf("%w: invalid occurredAt: %v", ErrInvalidMessage, err)
	}
	if err := validateData(msg.Data); err != nil {
		return WagerMessage{}, err
	}
	return msg, nil
}

func validateData(d WagerData) error {
	for _, f := range []struct {
		name  string
		value string
	}{
		{"providerId", d.ProviderID},
		{"externalTransactionId", d.ExternalTransactionID},
		{"idempotencyKey", d.IdempotencyKey},
		{"playerId", d.PlayerID},
		{"walletId", d.WalletID},
	} {
		if f.value == "" {
			return fmt.Errorf("%w: data.%s is required", ErrInvalidMessage, f.name)
		}
	}
	switch domain.WagerTransactionKind(d.Kind) {
	case domain.TransactionBet,
		domain.TransactionWin,
		domain.TransactionLoss,
		domain.TransactionRefund,
		domain.TransactionRollback:
	case domain.TransactionOpening:
		return fmt.Errorf("%w: %v", ErrOpeningNotAllowed, d.Kind)
	default:
		return fmt.Errorf("%w: invalid kind %q", ErrInvalidMessage, d.Kind)
	}
	if _, err := domain.NewMoneyFromDecimal(d.Money.Amount, d.Money.Currency); err != nil {
		return fmt.Errorf("%w: invalid money: %v", ErrInvalidMessage, err)
	}
	return nil
}

// ToTransaction converte a mensagem validada em WagerTransaction via os
// construtores de domínio (sem regra financeira aqui). internalID é o UUID
// interno da transação. A chave de idempotência vem de data.idempotencyKey.
func (m WagerMessage) ToTransaction(internalID string) (domain.WagerTransaction, error) {
	if domain.WagerTransactionKind(m.Data.Kind) == domain.TransactionOpening {
		return domain.WagerTransaction{}, fmt.Errorf("%w: %v", ErrOpeningNotAllowed, m.Data.Kind)
	}
	amount, err := domain.NewMoneyFromDecimal(m.Data.Money.Amount, m.Data.Money.Currency)
	if err != nil {
		return domain.WagerTransaction{}, fmt.Errorf("%w: invalid money: %v", ErrInvalidMessage, err)
	}
	tx, err := domain.NewWagerTransaction(
		internalID,
		m.Data.ProviderID,
		m.Data.ExternalTransactionID,
		m.Data.IdempotencyKey,
		"sqs",
		m.Data.PlayerID,
		m.Data.WalletID,
		m.Data.RoundID,
		m.Data.GameID,
		domain.WagerTransactionKind(m.Data.Kind),
		amount,
		m.Data.ReferenceExternalTransactionID,
	)
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	return *tx, nil
}
