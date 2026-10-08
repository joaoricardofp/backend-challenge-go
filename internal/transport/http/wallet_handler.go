package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/auth"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
)

type WalletHandler struct {
	walletService *application.WalletService
	wagerService  *application.WagerService
	metrics       *observability.Metrics
}

func NewWalletHandler(
	walletService *application.WalletService,
	wagerService *application.WagerService,
	metrics *observability.Metrics,
) *WalletHandler {
	return &WalletHandler{
		walletService: walletService,
		wagerService:  wagerService,
		metrics:       metrics,
	}
}

type openWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	ID        string   `json:"id"`
	PlayerID  string   `json:"playerId"`
	Balance   moneyDTO `json:"balance"`
	Currency  string   `json:"currency"`
	Version   int64    `json:"version"`
	CreatedAt string   `json:"createdAt"`
	UpdatedAt string   `json:"updatedAt"`
}

type ledgerEntryResponse struct {
	ID            string   `json:"id"`
	WalletID      string   `json:"walletId"`
	TransactionID string   `json:"transactionId"`
	Direction     string   `json:"direction"`
	Money         moneyDTO `json:"money"`
	BalanceBefore moneyDTO `json:"balanceBefore"`
	BalanceAfter  moneyDTO `json:"balanceAfter"`
	CreatedAt     string   `json:"createdAt"`
}

type ledgerResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

type transactionResponse struct {
	TransactionID                  string    `json:"transactionId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId,omitempty"`
	GameID                         string    `json:"gameId,omitempty"`
	Kind                           string    `json:"kind"`
	Money                          moneyDTO  `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
	Status                         string    `json:"status"`
	FailureCode                    string    `json:"failureCode,omitempty"`
	ResultingBalance               *moneyDTO `json:"resultingBalance,omitempty"`
	CreatedAt                      string    `json:"createdAt"`
	UpdatedAt                      string    `json:"updatedAt"`
}

func (h *WalletHandler) OpenWallet(w http.ResponseWriter, r *http.Request) {
	_, r = observability.EnsureRequestID(w, r)
	ctx := r.Context()
	logBase := func(args ...any) []any {
		base := []any{
			slog.String("component", "http-wallet"),
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

	var dto openWalletRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&dto); err != nil {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_JSON", "malformed JSON body")
		return
	}

	if dto.PlayerID == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "field playerId is required")
		return
	}
	if dto.InitialBalance.Amount == "" || dto.InitialBalance.Currency == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "field initialBalance is required")
		return
	}

	initialBalance, err := domain.NewMoneyFromDecimal(dto.InitialBalance.Amount, dto.InitialBalance.Currency)
	if err != nil {
		h.respond(w, ctx, logBase(slog.String("error", err.Error())), http.StatusBadRequest, "INVALID_AMOUNT", "invalid amount")
		return
	}

	walletID, err := newUUID()
	if err != nil {
		h.respond(w, ctx, logBase(), http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
		return
	}

	input := application.OpenWalletInput{
		WalletID:       walletID,
		PlayerID:       dto.PlayerID,
		InitialBalance: initialBalance,
	}

	res, err := h.walletService.OpenWallet(ctx, input)
	if err != nil {
		status, code, message := mapWalletError(err)
		fields := logBase(slog.String("player_id", dto.PlayerID), slog.String("failure_code", code))
		switch {
		case code == "WALLET_ALREADY_EXISTS":
			h.respond(w, ctx, fields, status, code, message)
		default:
			slog.ErrorContext(ctx, "open wallet failed", append(fields, slog.String("error", err.Error()))...)
			writeError(w, status, code, message)
		}
		return
	}

	h.respondCreated(w, ctx, logBase(), walletResponse{
		ID:        res.Wallet.ID,
		PlayerID:  res.Wallet.PlayerID,
		Balance:   moneyDTO{Amount: res.Wallet.Balance.AmountString(), Currency: res.Wallet.Balance.Currency()},
		Currency:  res.Wallet.Currency,
		Version:   res.Wallet.Version,
		CreatedAt: "",
		UpdatedAt: "",
	})
}

func (h *WalletHandler) GetWallet(w http.ResponseWriter, r *http.Request) {
	_, r = observability.EnsureRequestID(w, r)
	ctx := r.Context()
	logBase := func(args ...any) []any {
		base := []any{
			slog.String("component", "http-wallet"),
			slog.String("method", r.Method),
		}
		base = append(base, observability.AttrsFromContext(ctx)...)
		return append(base, args...)
	}

	walletID := r.PathValue("walletId")
	if walletID == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "walletId is required")
		return
	}

	res, err := h.walletService.GetWallet(ctx, application.GetWalletInput{WalletID: walletID})
	if err != nil {
		status, code, message := mapWalletError(err)
		h.respond(w, ctx, logBase(slog.String("wallet_id", walletID)), status, code, message)
		return
	}

	h.respondOK(w, ctx, logBase(), walletResponse{
		ID:        res.Wallet.ID,
		PlayerID:  res.Wallet.PlayerID,
		Balance:   moneyDTO{Amount: res.Wallet.Balance.AmountString(), Currency: res.Wallet.Balance.Currency()},
		Currency:  res.Wallet.Currency,
		Version:   res.Wallet.Version,
		CreatedAt: "",
		UpdatedAt: "",
	})
}

func (h *WalletHandler) GetWalletLedger(w http.ResponseWriter, r *http.Request) {
	_, r = observability.EnsureRequestID(w, r)
	ctx := r.Context()
	logBase := func(args ...any) []any {
		base := []any{
			slog.String("component", "http-wallet"),
			slog.String("method", r.Method),
		}
		base = append(base, observability.AttrsFromContext(ctx)...)
		return append(base, args...)
	}

	walletID := r.PathValue("walletId")
	if walletID == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "walletId is required")
		return
	}

	cursor := r.URL.Query().Get("cursor")
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed <= 0 {
			h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_LIMIT", "limit must be a positive integer")
			return
		}
		if parsed > 100 {
			h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_LIMIT", "limit cannot exceed 100")
			return
		}
		limit = parsed
	}

	res, err := h.walletService.ListWalletLedger(ctx, application.ListWalletLedgerInput{
		WalletID: walletID,
		Cursor:   cursor,
		Limit:    limit,
	})
	if err != nil {
		status, code, message := mapWalletError(err)
		h.respond(w, ctx, logBase(slog.String("wallet_id", walletID)), status, code, message)
		return
	}

	entries := make([]ledgerEntryResponse, len(res.Entries))
	for i, e := range res.Entries {
		entries[i] = ledgerEntryResponse{
			ID:            e.ID,
			WalletID:      e.WalletID,
			TransactionID: e.TransactionID,
			Direction:     string(e.Direction),
			Money:         moneyDTO{Amount: e.Amount.AmountString(), Currency: e.Amount.Currency()},
			BalanceBefore: moneyDTO{Amount: e.BalanceBefore.AmountString(), Currency: e.BalanceBefore.Currency()},
			BalanceAfter:  moneyDTO{Amount: e.BalanceAfter.AmountString(), Currency: e.BalanceAfter.Currency()},
			CreatedAt:     "",
		}
	}

	h.respondOK(w, ctx, logBase(), ledgerResponse{
		Entries:    entries,
		NextCursor: res.NextCursor,
	})
}

func (h *WalletHandler) GetTransaction(w http.ResponseWriter, r *http.Request) {
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

	transactionID := r.PathValue("transactionId")
	if transactionID == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "transactionId is required")
		return
	}

	res, err := h.walletService.GetTransaction(ctx, application.GetTransactionInput{TransactionID: transactionID})
	if err != nil {
		status, code, message := mapWalletError(err)
		h.respond(w, ctx, logBase(slog.String("transaction_id", transactionID)), status, code, message)
		return
	}

	h.checkProviderAuthorization(w, ctx, logBase, res.Transaction, func() {
		h.respondOK(w, ctx, logBase(), transactionToResponse(res.Transaction))
	})
}

func (h *WalletHandler) GetTransactionByProviderExternalID(w http.ResponseWriter, r *http.Request) {
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

	providerID := r.PathValue("providerId")
	externalTransactionID := r.PathValue("externalTransactionId")
	if providerID == "" || externalTransactionID == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "providerId and externalTransactionId are required")
		return
	}

	principal, ok := auth.PrincipalFrom(ctx)
	if !ok {
		h.respond(w, ctx, logBase(), http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
		return
	}
	if principal.ProviderID != providerID {
		h.respond(w, ctx, logBase(), http.StatusForbidden, "PROVIDER_MISMATCH", "provider mismatch")
		return
	}

	res, err := h.walletService.GetTransactionByProviderExternalID(ctx, application.GetTransactionByProviderExternalIDInput{
		ProviderID:            providerID,
		ExternalTransactionID: externalTransactionID,
	})
	if err != nil {
		status, code, message := mapWalletError(err)
		h.respond(w, ctx, logBase(slog.String("provider_id", providerID), slog.String("external_transaction_id", externalTransactionID)), status, code, message)
		return
	}

	h.respondOK(w, ctx, logBase(), transactionToResponse(res.Transaction))
}

func (h *WalletHandler) checkProviderAuthorization(w http.ResponseWriter, ctx context.Context, logBase func(...any) []any, tx domain.WagerTransaction, next func()) {
	principal, ok := auth.PrincipalFrom(ctx)
	if !ok {
		h.respond(w, ctx, logBase(), http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
		return
	}
	if principal.ProviderID != tx.ProviderID {
		h.respond(w, ctx, logBase(), http.StatusForbidden, "PROVIDER_MISMATCH", "provider mismatch")
		return
	}
	next()
}

func (h *WalletHandler) respond(w http.ResponseWriter, ctx context.Context, fields []any, status int, code, message string) {
	if status >= 500 {
		slog.ErrorContext(ctx, "wallet request failed", append(fields, slog.String("failure_code", code))...)
	} else {
		slog.WarnContext(ctx, "wallet request rejected", append(fields, slog.String("failure_code", code))...)
	}
	writeError(w, status, code, message)
}

func (h *WalletHandler) respondOK(w http.ResponseWriter, ctx context.Context, fields []any, v any) {
	slog.InfoContext(ctx, "wallet request succeeded", fields...)
	writeJSON(w, http.StatusOK, v)
}

func (h *WalletHandler) respondCreated(w http.ResponseWriter, ctx context.Context, fields []any, v any) {
	slog.InfoContext(ctx, "wallet created", fields...)
	writeJSON(w, http.StatusCreated, v)
}

func mapWalletError(err error) (int, string, string) {
	switch {
	case errors.Is(err, application.ErrWalletAlreadyExists):
		return http.StatusConflict, "WALLET_ALREADY_EXISTS", "wallet already exists for player and currency"
	case errors.Is(err, domain.ErrWalletNotFound), errors.Is(err, application.ErrWalletNotFound):
		return http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found"
	case errors.Is(err, domain.ErrTransactionNotFound):
		return http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found"
	case errors.Is(err, domain.ErrWalletPlayerMismatch):
		return http.StatusUnprocessableEntity, "PLAYER_WALLET_MISMATCH", "player wallet mismatch"
	case errors.Is(err, domain.ErrCurrencyMismatch):
		return http.StatusUnprocessableEntity, "CURRENCY_MISMATCH", "currency mismatch"
	case errors.Is(err, domain.ErrInvalidAmountFormat),
		errors.Is(err, domain.ErrNegativeAmount),
		errors.Is(err, domain.ErrInvalidAmount):
		return http.StatusBadRequest, "INVALID_AMOUNT", "invalid amount"
	case errors.Is(err, domain.ErrInvalidCurrency):
		return http.StatusBadRequest, "INVALID_CURRENCY", "invalid currency"
	default:
		return http.StatusInternalServerError, "INTERNAL_ERROR", "internal error"
	}
}

func transactionToResponse(tx domain.WagerTransaction) transactionResponse {
	resp := transactionResponse{
		TransactionID:                  tx.ID,
		ProviderID:                     tx.ProviderID,
		ExternalTransactionID:          tx.ExternalTransactionID,
		PlayerID:                       tx.PlayerID,
		WalletID:                       tx.WalletID,
		Kind:                           string(tx.Kind),
		Money:                          moneyDTO{Amount: tx.Amount.AmountString(), Currency: tx.Amount.Currency()},
		Status:                         string(tx.Status),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID,
	}
	if tx.RoundID != "" {
		resp.RoundID = tx.RoundID
	}
	if tx.GameID != "" {
		resp.GameID = tx.GameID
	}
	if tx.FailureCode != "" {
		resp.FailureCode = tx.FailureCode
	}
	if tx.ResultingBalance != nil {
		resp.ResultingBalance = &moneyDTO{Amount: tx.ResultingBalance.AmountString(), Currency: tx.ResultingBalance.Currency()}
	}
	return resp
}

type reconciliationIssueResponse struct {
	Type     string `json:"type"`
	EntryID  string `json:"entryId,omitempty"`
	Expected string `json:"expected,omitempty"`
	Actual   string `json:"actual,omitempty"`
	Message  string `json:"message"`
}

type reconciliationResponse struct {
	WalletID          string                        `json:"walletId"`
	StoredBalance     moneyDTO                      `json:"storedBalance"`
	CalculatedBalance moneyDTO                      `json:"calculatedBalance"`
	Difference        moneyDTO                      `json:"difference"`
	Consistent        bool                          `json:"consistent"`
	CheckedEntries    int                           `json:"checkedEntries"`
	Issues            []reconciliationIssueResponse `json:"issues"`
}

func (h *WalletHandler) ReconcileWallet(w http.ResponseWriter, r *http.Request) {
	_, r = observability.EnsureRequestID(w, r)
	ctx := r.Context()
	logBase := func(args ...any) []any {
		base := []any{
			slog.String("component", "http-wallet"),
			slog.String("method", r.Method),
		}
		base = append(base, observability.AttrsFromContext(ctx)...)
		return append(base, args...)
	}

	if r.Method != http.MethodPost {
		h.respond(w, ctx, logBase(), http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	walletID := r.PathValue("walletId")
	if walletID == "" {
		h.respond(w, ctx, logBase(), http.StatusBadRequest, "INVALID_REQUEST", "walletId is required")
		return
	}

	res, err := h.walletService.ReconcileWallet(ctx, application.ReconciliationInput{WalletID: walletID})
	if err != nil {
		status, code, message := mapWalletError(err)
		h.respond(w, ctx, logBase(slog.String("wallet_id", walletID)), status, code, message)
		return
	}

	issues := make([]reconciliationIssueResponse, len(res.Issues))
	for i, issue := range res.Issues {
		issues[i] = reconciliationIssueResponse{
			Type:     string(issue.Type),
			EntryID:  issue.EntryID,
			Expected: issue.Expected,
			Actual:   issue.Actual,
			Message:  issue.Message,
		}
	}

	// A reconciliação sempre retorna 200, mesmo se inconsistent
	// O campo "consistent" indica o resultado
	result := "consistent"
	if !res.Consistent {
		result = "inconsistent"
	}
	h.metrics.IncWalletReconciliation(result)
	h.respondOK(w, ctx, logBase(slog.String("wallet_id", walletID), slog.Bool("consistent", res.Consistent)), reconciliationResponse{
		WalletID:          res.WalletID,
		StoredBalance:     moneyDTO{Amount: res.StoredBalance.AmountString(), Currency: res.StoredBalance.Currency()},
		CalculatedBalance: moneyDTO{Amount: res.CalculatedBalance.AmountString(), Currency: res.CalculatedBalance.Currency()},
		Difference:        moneyDTO{Amount: res.Difference.AmountString(), Currency: res.Difference.Currency()},
		Consistent:        res.Consistent,
		CheckedEntries:    res.CheckedEntries,
		Issues:            issues,
	})
}
