package http

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/auth"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
)

// WagerHandler é um adapter fino de transporte: valida o contrato HTTP,
// converte para o domínio e chama exclusivamente WagerService.Process.
// Nenhuma regra financeira vive aqui.
type WagerHandler struct {
	service *application.WagerService
	metrics *observability.Metrics
}

// NewWagerHandler monta o handler a partir do único use case de wager.
// O handler guarda somente service e métricas: prova estrutural de que o
// HTTP não acessa repositories, domínio financeiro direto ou outro use case.
// metrics pode ser nil (descartadas); logs usam o slog default.
func NewWagerHandler(service *application.WagerService, metrics *observability.Metrics) *WagerHandler {
	return &WagerHandler{service: service, metrics: metrics}
}

// moneyDTO espelha o contrato externo {"amount":"25.00","currency":"BRL"}.
type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// wagerRequestDTO espelha POST /wagering/transactions (README §9).
type wagerRequestDTO struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId"`
}

// wagerResponse espelha o contrato de resposta do README:
// {transactionId, status, balance, idempotentReplay} + failureCode quando há.
type wagerResponse struct {
	TransactionID    string   `json:"transactionId"`
	Status           string   `json:"status"`
	Balance          moneyDTO `json:"balance"`
	IdempotentReplay bool     `json:"idempotentReplay"`
	FailureCode      string   `json:"failureCode,omitempty"`
}

// errorResponse é o corpo estável de erro: código sem detalhes internos.
type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ProcessWager implementa POST /wagering/transactions. Contagem de requests
// por status vive no middleware (uma única fonte); aqui contam-se apenas
// desfechos de negócio: transações servidas, rejeições e falhas.
func (h *WagerHandler) ProcessWager(w http.ResponseWriter, r *http.Request) {
	// Correlation ID: do middleware, do header validado ou gerado aqui
	// (chamadas diretas sem middleware). Ecoado na resposta.
	_, r = observability.EnsureRequestID(w, r)
	ctx := r.Context()
	logBase := func(args ...any) []any {
		base := []any{
			slog.String("component", "http-wager"),
			slog.String("method", r.Method),
		}
		base = append(base, observability.AttrsFromContext(ctx)...)
		return append(base, args...)
	}

	if r.Method != http.MethodPost {
		h.respond(w, ctx, logBase(), http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		h.respond(w, ctx, logBase(), http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", "content type must be application/json")
		return
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required")
		return
	}

	var dto wagerRequestDTO
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&dto); err != nil {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_JSON", "malformed JSON body")
		return
	}

	// OPENING é operação interna: a fronteira externa nunca pode criá-la.
	// Barrado aqui, antes de qualquer construção de domínio ou chamada ao
	// use case (o domínio continua aceitando OPENING para uso interno).
	if dto.Kind == string(domain.TransactionOpening) {
		h.respond(w, ctx, logBase(slog.String("kind", dto.Kind)), http.StatusBadRequest, "INVALID_KIND", "invalid kind")
		return
	}

	tx, err := dtoToTransaction(dto, idempotencyKey)
	if err != nil {
		var te *transportError
		if errors.As(err, &te) {
			h.respond(w, ctx, logBase(slog.String("kind", dto.Kind)), te.status, te.code, te.message)
			return
		}
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "invalid request")
		return
	}

	// O providerId efetivo vem da identidade autenticada, nunca do corpo:
	// quando o middleware validou um principal, ele prevalece. Sem
	// middleware (fluxos internos/testes), vale o DTO já validado.
	if principal, ok := auth.PrincipalFrom(r.Context()); ok {
		tx.ProviderID = principal.ProviderID
	}

	res, err := h.service.Process(r.Context(), application.ProcessWagerInput{Transaction: tx})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		status, code, message := mapAppError(err)
		fields := logBase(
			slog.String("kind", string(tx.Kind)),
			slog.String("failure_code", code),
			slog.String("provider_id", tx.ProviderID),
			slog.String("external_transaction_id", tx.ExternalTransactionID),
		)
		switch {
		case isRejectionCode(code):
			h.metrics.IncWagerRejection(code)
			slog.InfoContext(ctx, "wager rejected", fields...)
		case code == "OVERFLOW":
			h.metrics.IncWagerFailure(code)
			slog.ErrorContext(ctx, "wager failed", append(fields, slog.String("error", err.Error()))...)
		default:
			slog.WarnContext(ctx, "wager error", append(fields, slog.String("error", err.Error()))...)
		}
		writeError(w, status, code, message)
		return
	}

	status := http.StatusOK
	if res.Transaction.Status == domain.TransactionPendingReference {
		status = http.StatusAccepted
	}
	h.metrics.IncWagerTransaction(string(res.Transaction.Kind), string(res.Transaction.Status))
	if res.Transaction.IsRejected() {
		h.metrics.IncWagerRejection(res.Transaction.FailureCode)
	}
	if res.Transaction.IsFailed() {
		h.metrics.IncWagerFailure(res.Transaction.FailureCode)
	}
	slog.InfoContext(ctx, "wager processed", logBase(
		slog.String("kind", string(res.Transaction.Kind)),
		slog.String("status", string(res.Transaction.Status)),
		slog.String("transaction_id", res.Transaction.ID),
		slog.String("provider_id", res.Transaction.ProviderID),
		slog.Bool("idempotent_replay", res.Replayed),
	)...)
	writeJSON(w, status, wagerResponse{
		TransactionID:    res.Transaction.ID,
		Status:           string(res.Transaction.Status),
		Balance:          moneyDTO{Amount: res.Balance.AmountString(), Currency: res.Balance.Currency()},
		IdempotentReplay: res.Replayed,
		FailureCode:      res.Transaction.FailureCode,
	})
}

// respond escreve um erro de validação da fronteira com log e métrica.
// Códigos de transporte (validação de entrada) não são rejeições de negócio
// persistidas: contam apenas em wager_requests_total.
func (h *WagerHandler) respond(w http.ResponseWriter, ctx context.Context, fields []any, status int, code, message string) {
	if status >= 500 {
		slog.ErrorContext(ctx, "wager request failed", append(fields, slog.String("failure_code", code))...)
	} else {
		slog.WarnContext(ctx, "wager request rejected", append(fields, slog.String("failure_code", code))...)
	}
	writeError(w, status, code, message)
}

// isRejectionCode reconhece os códigos de rejeição de negócio persistida
// (REJECTED). OVERFLOW é FAILED e vai para IncWagerFailure; o resto são
// erros de validação/conflito/transitórios, não decisões persistidas.
func isRejectionCode(code string) bool {
	switch code {
	case "INSUFFICIENT_FUNDS",
		"PLAYER_WALLET_MISMATCH",
		"CURRENCY_MISMATCH",
		"INVALID_REFERENCE_KIND",
		"REFERENCE_NOT_PROCESSED",
		"DUPLICATE_REVERSAL":
		return true
	default:
		return false
	}
}

// transportError é um erro de validação do próprio transporte (nunca do domínio).
type transportError struct {
	status  int
	code    string
	message string
}

func (e *transportError) Error() string { return e.code + ": " + e.message }

// dtoToTransaction valida o DTO e constrói o domínio com os construtores
// existentes. Erros estruturais do domínio viram 400 com códigos estáveis.
func dtoToTransaction(dto wagerRequestDTO, idempotencyKey string) (domain.WagerTransaction, error) {
	for _, f := range []struct {
		name  string
		value string
	}{
		{"providerId", dto.ProviderID},
		{"externalTransactionId", dto.ExternalTransactionID},
		{"playerId", dto.PlayerID},
		{"walletId", dto.WalletID},
	} {
		if f.value == "" {
			return domain.WagerTransaction{}, &transportError{http.StatusBadRequest, "INVALID_REQUEST", "field " + f.name + " is required"}
		}
	}

	amount, err := domain.NewMoneyFromDecimal(dto.Money.Amount, dto.Money.Currency)
	if err != nil {
		return domain.WagerTransaction{}, mapDomainToTransportError(err)
	}

	id, err := newUUID()
	if err != nil {
		return domain.WagerTransaction{}, err
	}

	tx, err := domain.NewWagerTransaction(
		id,
		dto.ProviderID,
		dto.ExternalTransactionID,
		idempotencyKey,
		"http",
		dto.PlayerID,
		dto.WalletID,
		dto.RoundID,
		dto.GameID,
		domain.WagerTransactionKind(dto.Kind),
		amount,
		dto.ReferenceExternalTransactionID,
	)
	if err != nil {
		return domain.WagerTransaction{}, mapDomainToTransportError(err)
	}
	return *tx, nil
}

// mapDomainToTransportError converte erros estruturais do domínio (400).
func mapDomainToTransportError(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidAmountFormat),
		errors.Is(err, domain.ErrNegativeAmount),
		errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrInvalidLossAmount):
		return &transportError{http.StatusBadRequest, "INVALID_AMOUNT", "invalid amount"}
	case errors.Is(err, domain.ErrInvalidCurrency):
		return &transportError{http.StatusBadRequest, "INVALID_CURRENCY", "invalid currency"}
	case errors.Is(err, domain.ErrInvalidTransactionKind):
		return &transportError{http.StatusBadRequest, "INVALID_KIND", "invalid kind"}
	case errors.Is(err, domain.ErrOverflow):
		return &transportError{http.StatusBadRequest, "AMOUNT_OVERFLOW", "amount overflow"}
	default:
		return &transportError{http.StatusBadRequest, "INVALID_REQUEST", "invalid request"}
	}
}

// mapAppError converte erros da application em (status, code, message
// estáveis). Códigos HTTP exatos não são normativos no README (§9: apenas
// distinguíveis); a tabela abaixo é a decisão mínima documentada.
// Mensagens são literais estáticos: mesmo erros com errors.Join (que podem
// carregar texto do PostgreSQL) nunca vazam detalhes internos.
func mapAppError(err error) (int, string, string) {
	switch {
	case errors.Is(err, domain.ErrIdempotencyConflict),
		errors.Is(err, domain.ErrDuplicateIdempotencyKey):
		return http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency conflict"
	case errors.Is(err, domain.ErrExternalTransactionConflict),
		errors.Is(err, domain.ErrDuplicateExternalID):
		return http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT", "external transaction conflict"
	case errors.Is(err, application.ErrIdentityConflict):
		return http.StatusConflict, "IDENTITY_CONFLICT", "identity conflict"
	case errors.Is(err, domain.ErrDuplicateReversal):
		return http.StatusConflict, "DUPLICATE_REVERSAL", "duplicate reversal"
	case errors.Is(err, application.ErrIdempotencyInProgress):
		return http.StatusAccepted, "IN_PROGRESS", "operation in progress"
	case errors.Is(err, domain.ErrInsufficientFunds):
		return http.StatusUnprocessableEntity, "INSUFFICIENT_FUNDS", "insufficient funds"
	case errors.Is(err, domain.ErrWalletPlayerMismatch):
		return http.StatusUnprocessableEntity, "PLAYER_WALLET_MISMATCH", "player wallet mismatch"
	case errors.Is(err, domain.ErrCurrencyMismatch):
		return http.StatusUnprocessableEntity, "CURRENCY_MISMATCH", "currency mismatch"
	case errors.Is(err, domain.ErrInvalidReferenceKind):
		return http.StatusUnprocessableEntity, "INVALID_REFERENCE_KIND", "invalid reference kind"
	case errors.Is(err, domain.ErrReferenceNotProcessed):
		return http.StatusUnprocessableEntity, "REFERENCE_NOT_PROCESSED", "reference not processed"
	case errors.Is(err, domain.ErrInvalidTransactionKind):
		return http.StatusBadRequest, "INVALID_KIND", "invalid kind"
	case errors.Is(err, domain.ErrTransactionNotPending):
		return http.StatusBadRequest, "INVALID_STATUS", "invalid status"
	case errors.Is(err, domain.ErrWalletNotFound):
		return http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, "UNAVAILABLE", "internal error"
	case errors.Is(err, domain.ErrOverflow):
		return http.StatusInternalServerError, "OVERFLOW", "internal error"
	default:
		return http.StatusInternalServerError, "INTERNAL_ERROR", "internal error"
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// newUUID gera UUID v4 com stdlib (sem nova dependência).
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
