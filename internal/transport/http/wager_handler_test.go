package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
	wagerhttp "github.com/joaoricardofp/backend-challenge-go/internal/transport/http"
)

const defaultTestDatabaseURL = "postgres://postgres:postgres@localhost:5432/backend-challenge-go"

type httpFixture struct {
	pool    *pgxpool.Pool
	handler *wagerhttp.WagerHandler
	wallets *postgres.WalletRepository
}

func newHTTPFixture(t *testing.T) *httpFixture {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultTestDatabaseURL
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	wallets := postgres.NewWalletRepository(pool)
	transactions := postgres.NewWagerTransactionRepository(pool)
	ledger := postgres.NewLedgerRepository(pool)
	service := application.NewWagerService(pool, wallets, transactions, ledger)

	return &httpFixture{pool: pool, handler: wagerhttp.NewWagerHandler(service), wallets: wallets}
}

func (f *httpFixture) uuid(t *testing.T) string {
	t.Helper()

	var id string
	if err := f.pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id
}

// createWallet persiste uma wallet BRL e registra limpeza (wallet + transactions + ledger).
func (f *httpFixture) createWallet(t *testing.T, balanceCents int64) *domain.Wallet {
	t.Helper()

	wallet, err := domain.NewWallet(f.uuid(t), f.uuid(t), "BRL")
	if err != nil {
		t.Fatalf("new wallet: %v", err)
	}
	if balanceCents > 0 {
		deposit, err := domain.NewMoney(balanceCents, "BRL")
		if err != nil {
			t.Fatalf("new money: %v", err)
		}
		if err := wallet.Credit(deposit); err != nil {
			t.Fatalf("credit: %v", err)
		}
	}
	if err := f.wallets.Create(context.Background(), wallet); err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = f.pool.Exec(ctx, "DELETE FROM wallet_ledger_entries WHERE wallet_id = $1", wallet.ID)
		_, _ = f.pool.Exec(ctx, "DELETE FROM wager_transactions WHERE wallet_id = $1", wallet.ID)
		_, _ = f.pool.Exec(ctx, "DELETE FROM wallets WHERE id = $1", wallet.ID)
	})
	return wallet
}

func (f *httpFixture) post(t *testing.T, method, body, key, contentType string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, "/wagering/transactions", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	f.handler.ProcessWager(rec, req)
	return rec
}

func betBody(providerID, extTxID, playerID, walletID, amount, currency string) string {
	return betBodyRaw(providerID, extTxID, playerID, walletID, fmt.Sprintf(`%q`, amount), currency)
}

func betBodyRaw(providerID, extTxID, playerID, walletID, amountJSON, currency string) string {
	return fmt.Sprintf(
		`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
			`"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":%s,"currency":%q}}`,
		providerID, extTxID, playerID, walletID, amountJSON, currency,
	)
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, rec.Body.String())
	}
	return body
}

func TestWagerHTTP_HappyPathAndReplay(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	providerID := f.uuid(t)
	extTxID := "ext-" + f.uuid(t)
	key := "key-" + f.uuid(t)
	body := betBody(providerID, extTxID, wallet.PlayerID, wallet.ID, "30.00", "BRL")

	first := f.post(t, http.MethodPost, body, key, "application/json")
	if first.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", first.Code, first.Body.String())
	}
	firstBody := decodeBody(t, first)
	if firstBody["status"] != "PROCESSED" {
		t.Errorf("status = %v, want PROCESSED", firstBody["status"])
	}
	if firstBody["idempotentReplay"] != false {
		t.Errorf("idempotentReplay = %v, want false", firstBody["idempotentReplay"])
	}
	balance, _ := firstBody["balance"].(map[string]any)
	if balance["amount"] != "70.00" || balance["currency"] != "BRL" {
		t.Errorf("balance = %v, want 70.00 BRL", balance)
	}
	txID, _ := firstBody["transactionId"].(string)
	if txID == "" {
		t.Fatal("transactionId empty")
	}
	if _, hasFailure := firstBody["failureCode"]; hasFailure {
		t.Errorf("failureCode present on success: %v", firstBody)
	}

	second := f.post(t, http.MethodPost, body, key, "application/json")
	if second.Code != http.StatusOK {
		t.Fatalf("replay status = %d, want 200", second.Code)
	}
	secondBody := decodeBody(t, second)
	if secondBody["idempotentReplay"] != true {
		t.Errorf("idempotentReplay = %v, want true", secondBody["idempotentReplay"])
	}
	if secondBody["transactionId"] != txID {
		t.Errorf("transactionId = %v, want %v", secondBody["transactionId"], txID)
	}
}

func TestWagerHTTP_MoneyValidation(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)

	for _, amount := range []string{`"25"`, `"25.0"`, `"25.000"`, `"1e2"`, `"1E2"`, `"-25.00"`, `"NaN"`, `"Infinity"`, `""`} {
		t.Run("amount "+amount, func(t *testing.T) {
			body := betBodyRaw(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, amount, "BRL")
			rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
			if decodeBody(t, rec)["code"] != "INVALID_AMOUNT" {
				t.Errorf("code = %v, want INVALID_AMOUNT (%s)", decodeBody(t, rec)["code"], rec.Body.String())
			}
		})
	}

	t.Run("json number instead of string is malformed contract", func(t *testing.T) {
		body := betBodyRaw(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, `25.00`, "BRL")
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
		if decodeBody(t, rec)["code"] != "INVALID_JSON" {
			t.Errorf("code = %v, want INVALID_JSON", decodeBody(t, rec)["code"])
		}
	})

	t.Run("invalid currency", func(t *testing.T) {
		body := betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "25.00", "BR")
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("valid zero loss amount shape accepted by transport", func(t *testing.T) {
		body := strings.Replace(betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "0.00", "BRL"), `"kind":"BET"`, `"kind":"LOSS"`, 1)
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
	})
}

func TestWagerHTTP_InputValidation(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	valid := func() string {
		return betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "25.00", "BRL")
	}

	t.Run("malformed json", func(t *testing.T) {
		rec := f.post(t, http.MethodPost, `{"providerId":`, "k", "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("empty body", func(t *testing.T) {
		rec := f.post(t, http.MethodPost, ``, "k", "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("missing provider", func(t *testing.T) {
		body := strings.Replace(valid(), `"providerId":`, `"missing":`, 1)
		rec := f.post(t, http.MethodPost, body, "k", "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("invalid kind", func(t *testing.T) {
		body := strings.Replace(valid(), `"kind":"BET"`, `"kind":"DEBIT"`, 1)
		rec := f.post(t, http.MethodPost, body, "k", "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
		if decodeBody(t, rec)["code"] != "INVALID_KIND" {
			t.Errorf("code = %v, want INVALID_KIND", decodeBody(t, rec)["code"])
		}
	})

	t.Run("unknown field rejected", func(t *testing.T) {
		body := strings.Replace(valid(), `"kind":"BET"`, `"kind":"BET","hack":1`, 1)
		rec := f.post(t, http.MethodPost, body, "k", "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("missing idempotency key", func(t *testing.T) {
		rec := f.post(t, http.MethodPost, valid(), "", "application/json")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
		if decodeBody(t, rec)["code"] != "MISSING_IDEMPOTENCY_KEY" {
			t.Errorf("code = %v, want MISSING_IDEMPOTENCY_KEY", decodeBody(t, rec)["code"])
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		rec := f.post(t, http.MethodGet, valid(), "k", "application/json")
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
	})

	t.Run("wrong content type", func(t *testing.T) {
		rec := f.post(t, http.MethodPost, valid(), "k", "text/plain")
		if rec.Code != http.StatusUnsupportedMediaType {
			t.Errorf("status = %d, want 415", rec.Code)
		}
	})
}

func TestWagerHTTP_IdempotencyConflict(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	providerID := f.uuid(t)
	key := "key-" + f.uuid(t)

	first := betBody(providerID, "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "30.00", "BRL")
	if rec := f.post(t, http.MethodPost, first, key, "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("first status = %d", rec.Code)
	}

	second := betBody(providerID, "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "50.00", "BRL")
	rec := f.post(t, http.MethodPost, second, key, "application/json")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if decodeBody(t, rec)["code"] != "IDEMPOTENCY_CONFLICT" {
		t.Errorf("code = %v, want IDEMPOTENCY_CONFLICT", decodeBody(t, rec)["code"])
	}
}

func TestWagerHTTP_BusinessErrors(t *testing.T) {
	f := newHTTPFixture(t)

	t.Run("insufficient funds", func(t *testing.T) {
		wallet := f.createWallet(t, 10000)
		body := betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "200.00", "BRL")
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", rec.Code)
		}
		if decodeBody(t, rec)["code"] != "INSUFFICIENT_FUNDS" {
			t.Errorf("code = %v, want INSUFFICIENT_FUNDS", decodeBody(t, rec)["code"])
		}
	})

	t.Run("currency mismatch", func(t *testing.T) {
		wallet := f.createWallet(t, 10000)
		body := betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "10.00", "USD")
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", rec.Code)
		}
	})

	t.Run("unknown wallet", func(t *testing.T) {
		body := betBody(f.uuid(t), "ext-"+f.uuid(t), f.uuid(t), f.uuid(t), "10.00", "BRL")
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
		if decodeBody(t, rec)["code"] != "WALLET_NOT_FOUND" {
			t.Errorf("code = %v, want WALLET_NOT_FOUND", decodeBody(t, rec)["code"])
		}
	})
}

func TestWagerHTTP_Reversals(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	providerID := f.uuid(t)

	betBodyStr := betBody(providerID, "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "30.00", "BRL")
	if rec := f.post(t, http.MethodPost, betBodyStr, "key-"+f.uuid(t), "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("bet status = %d", rec.Code)
	}

	t.Run("valid refund", func(t *testing.T) {
		extBet := extractExt(t, betBodyStr)
		body := fmt.Sprintf(
			`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
				`"roundId":"round-987","gameId":"fortune-chimp","kind":"REFUND",`+
				`"money":{"amount":"30.00","currency":"BRL"},"referenceExternalTransactionId":%q}`,
			providerID, "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, extBet,
		)
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
		if decodeBody(t, rec)["status"] != "PROCESSED" {
			t.Errorf("status = %v, want PROCESSED", decodeBody(t, rec)["status"])
		}
	})

	t.Run("missing reference is pending", func(t *testing.T) {
		body := fmt.Sprintf(
			`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
				`"roundId":"round-987","gameId":"fortune-chimp","kind":"REFUND",`+
				`"money":{"amount":"30.00","currency":"BRL"},"referenceExternalTransactionId":"ext-never"}`,
			providerID, "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID,
		)
		rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body.String())
		}
		if decodeBody(t, rec)["status"] != "PENDING_REFERENCE" {
			t.Errorf("status = %v, want PENDING_REFERENCE", decodeBody(t, rec)["status"])
		}
	})

	t.Run("duplicate reversal conflicts", func(t *testing.T) {
		extBet := extractExt(t, betBodyStr)
		ref := func(ext string) string {
			return fmt.Sprintf(
				`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
					`"roundId":"round-987","gameId":"fortune-chimp","kind":"REFUND",`+
					`"money":{"amount":"30.00","currency":"BRL"},"referenceExternalTransactionId":%q}`,
				providerID, ext, wallet.PlayerID, wallet.ID, extBet,
			)
		}
		// Segunda REFUND da mesma referência (a primeira foi processada acima).
		rec := f.post(t, http.MethodPost, ref("ext-"+f.uuid(t)), "key-"+f.uuid(t), "application/json")
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409 (%s)", rec.Code, rec.Body.String())
		}
		if decodeBody(t, rec)["code"] != "DUPLICATE_REVERSAL" {
			t.Errorf("code = %v, want DUPLICATE_REVERSAL", decodeBody(t, rec)["code"])
		}
	})
}

func extractExt(t *testing.T, body string) string {
	t.Helper()

	var dto map[string]any
	if err := json.Unmarshal([]byte(body), &dto); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	ext, _ := dto["externalTransactionId"].(string)
	return ext
}

func TestWagerHTTP_NoInternalLeak(t *testing.T) {
	f := newHTTPFixture(t)

	// UUID inválido força erro do PostgreSQL no lookup da wallet.
	body := betBody(f.uuid(t), "ext-"+f.uuid(t), f.uuid(t), "not-a-uuid", "10.00", "BRL")
	rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	respBody := decodeBody(t, rec)
	if respBody["code"] != "INTERNAL_ERROR" || respBody["message"] != "internal error" {
		t.Errorf("body = %v, want sanitized INTERNAL_ERROR", respBody)
	}
	for _, leak := range []string{"wallets", "wager", "SQLSTATE", "postgres", "pq:", "relation", "uuid", "connection", "syntax"} {
		if strings.Contains(strings.ToLower(rec.Body.String()), strings.ToLower(leak)) {
			t.Errorf("response leaks %q: %s", leak, rec.Body.String())
		}
	}
}

func TestWagerHTTP_HandlerUsesSingleUseCase(t *testing.T) {
	// Prova estrutural: o handler guarda somente o *application.WagerService.
	// Qualquer acesso direto a repository/domínio financeiro pelo transporte
	// exigiria um campo novo e quebraria este teste de propósito.
	rt := reflect.TypeOf(wagerhttp.NewWagerHandler(nil)).Elem()
	if rt.NumField() != 1 {
		t.Fatalf("WagerHandler fields = %d, want exactly 1", rt.NumField())
	}
	field := rt.Field(0)
	want := reflect.TypeOf((*application.WagerService)(nil))
	if field.Type != want {
		t.Fatalf("WagerHandler field = %v, want %v", field.Type, want)
	}
}
