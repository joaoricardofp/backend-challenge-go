package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// canonicalWagerRequest é a forma canônica de uma operação de wager para
// cálculo de fingerprint. Ordem e nomes de campos são fixos (struct +
// encoding/json, sem reflection e sem dependência da representação interna
// de WagerTransaction). Campos opcionais: string vazia equivale a ausente
// (null). Ficam de fora: IdempotencyKey, status, saldos, version,
// timestamps, IDs de banco e qualquer metadata de transporte.
type canonicalWagerRequest struct {
	ProviderID                     string  `json:"providerId"`
	ExternalTransactionID          string  `json:"externalTransactionId"`
	PlayerID                       string  `json:"playerId"`
	WalletID                       string  `json:"walletId"`
	RoundID                        *string `json:"roundId"`
	GameID                         *string `json:"gameId"`
	Kind                           string  `json:"kind"`
	Amount                         string  `json:"amount"`
	Currency                       string  `json:"currency"`
	ReferenceExternalTransactionID *string `json:"referenceExternalTransactionId"`
}

func canonicalString(s string) *string {
	if s == "" {
		return nil
	}
	out := s
	return &out
}

func canonicalPayload(tx WagerTransaction) canonicalWagerRequest {
	return canonicalWagerRequest{
		ProviderID:                     tx.ProviderID,
		ExternalTransactionID:          tx.ExternalTransactionID,
		PlayerID:                       tx.PlayerID,
		WalletID:                       tx.WalletID,
		RoundID:                        canonicalString(tx.RoundID),
		GameID:                         canonicalString(tx.GameID),
		Kind:                           string(tx.Kind),
		Amount:                         tx.Amount.AmountString(),
		Currency:                       tx.Amount.Currency(),
		ReferenceExternalTransactionID: canonicalString(tx.ReferenceExternalTransactionID),
	}
}

// CanonicalWagerFingerprint calcula o SHA-256 (hex minúsculo, 64 chars) do
// payload canônico da transação. Função pura: sem banco, relógio, random
// ou mutação do input; mesmo input produz sempre o mesmo fingerprint.
// O amount usa AmountString ("25.00") — nenhum float em nenhum ponto.
//
// A partir desta etapa: payload_hash = este fingerprint. A Idempotency-Key
// NÃO participa do fingerprint.
func CanonicalWagerFingerprint(tx WagerTransaction) (string, error) {
	raw, err := json.Marshal(canonicalPayload(tx))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
