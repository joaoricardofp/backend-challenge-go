package application

import (
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
)

// ExternalTransactionOutcome é a decisão persistente de identidade externa
// para um par (providerID, externalTransactionID) + fingerprint de entrada.
// É independente da Idempotency-Key: a mesma operação externa é reconhecida
// mesmo quando chega com outra chave.
type ExternalTransactionOutcome int

const (
	// ExternalTransactionNew: operação externa ainda não registrada.
	ExternalTransactionNew ExternalTransactionOutcome = iota
	// ExternalTransactionKnown: mesma operação externa já registrada (mesmo
	// fingerprint). Não confundir com REPLAY de idempotência: o replay final
	// será integrado em B3; aqui apenas se reconhece a operação.
	ExternalTransactionKnown
)

// ExternalTransactionResult carrega a decisão e, quando há linha existente,
// a transaction persistida correspondente.
type ExternalTransactionResult struct {
	Outcome  ExternalTransactionOutcome
	Existing *domain.WagerTransaction
}

// DecideExternalTransaction decide, sem nenhum acesso a infraestrutura, o
// que fazer com uma operação entrante dado o lookup persistente por
// (providerID, externalTransactionID):
//
//	existing == nil          -> NEW (operação ainda não registrada)
//	existing + mesmo hash    -> KNOWN (qualquer status, inclusive PENDING)
//	existing + hash diferente -> ErrExternalTransactionConflict
//
// A comparação é sempre entre o fingerprint entrante e o payload_hash
// persistido. A decisão independe da Idempotency-Key da requisição.
//
// OPENING: o decididor não diferencia kinds de propósito. OPENING segue com
// metadata externa NOT NULL (decisão B1) e sua identidade interna será
// tratada em etapa própria; callers não devem rotear OPENING por identidade
// externa.
func DecideExternalTransaction(existing *domain.WagerTransaction, incomingHash string) (ExternalTransactionResult, error) {
	if existing == nil {
		return ExternalTransactionResult{Outcome: ExternalTransactionNew}, nil
	}

	if existing.PayloadHash != incomingHash {
		return ExternalTransactionResult{Existing: existing}, domain.ErrExternalTransactionConflict
	}

	return ExternalTransactionResult{Outcome: ExternalTransactionKnown, Existing: existing}, nil
}
