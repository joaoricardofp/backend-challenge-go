package application

import (
	"errors"

	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// ErrIdempotencyInProgress indica que já existe uma transaction para
// (providerID, idempotencyKey) com o mesmo fingerprint, mas ela ainda não
// está em estado terminal. Não é replay final nem conflito: a camada acima
// deve aguardar/reconsultar em vez de reprocessar.
var ErrIdempotencyInProgress = errors.New("idempotency transaction in progress")

// IdempotencyOutcome é a decisão persistente de idempotência para um par
// (providerID, idempotencyKey) + fingerprint de entrada.
type IdempotencyOutcome int

const (
	// IdempotencyNew: nenhuma transaction para a identidade; pode processar.
	IdempotencyNew IdempotencyOutcome = iota
	// IdempotencyReplay: mesma operação lógica já terminal; retornar o
	// resultado persistido sem repetir efeitos financeiros.
	IdempotencyReplay
	// IdempotencyInProgress: mesma operação ainda não terminal; aguardar.
	IdempotencyInProgress
)

// IdempotencyResult carrega a decisão e, quando há linha existente, a
// transaction persistida correspondente.
type IdempotencyResult struct {
	Outcome  IdempotencyOutcome
	Existing *domain.WagerTransaction
}

// DecideIdempotency decide, sem nenhum acesso a infraestrutura, o que fazer
// com uma requisição entrante dado o lookup persistente por
// (providerID, idempotencyKey):
//
//	existing == nil                        -> NEW (pode processar)
//	existing + hash diferente               -> ErrIdempotencyConflict
//	existing + mesmo hash + terminal        -> REPLAY (retornar persistido)
//	existing + mesmo hash + não terminal    -> IN_PROGRESS (+ ErrIdempotencyInProgress)
//
// A comparação é sempre entre o fingerprint entrante e o payload_hash
// persistido (SHA-256 canônico, B2.1). PENDING nunca é replay final.
func DecideIdempotency(existing *domain.WagerTransaction, incomingHash string) (IdempotencyResult, error) {
	if existing == nil {
		return IdempotencyResult{Outcome: IdempotencyNew}, nil
	}

	if existing.PayloadHash != incomingHash {
		return IdempotencyResult{Existing: existing}, domain.ErrIdempotencyConflict
	}

	if !existing.IsTerminal() {
		return IdempotencyResult{Outcome: IdempotencyInProgress, Existing: existing}, ErrIdempotencyInProgress
	}

	return IdempotencyResult{Outcome: IdempotencyReplay, Existing: existing}, nil
}
