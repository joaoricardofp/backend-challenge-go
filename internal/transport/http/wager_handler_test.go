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
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
	wagerhttp "github.com/joaoricardofp/backend-challenge-go/internal/transport/http"
)

const defaultTestDatabaseURL = "postgres://postgres:postgres@localhost:5432/backend-challenge-go"

type httpFixture struct {
	pool    *pgxpool.Pool
	handler *wagerhttp.WagerHandler
	wallets *postgres.WalletRepository
	metrics *observability.Metrics
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
	outbox := postgres.NewOutboxRepository(pool)
	service := application.NewWagerService(pool, wallets, transactions, ledger, outbox)
	metrics := observability.NewMetrics()

	return &httpFixture{pool: pool, handler: wagerhttp.NewWagerHandler(service, metrics), wallets: wallets, metrics: metrics}
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
		_, _ = f.pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`, wallet.ID)
		_, _ = f.pool.Exec(ctx, "TRUNCATE wallet_ledger_entries")
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
	// Prova estrutural: o handler guarda o *application.WagerService e as
	// métricas operacionais — e nada mais. Qualquer acesso direto a
	// repository/domínio financeiro pelo transporte exigiria um campo novo e
	// quebraria este teste de propósito. Métricas são observabilidade, não
	// acesso a dados: permitidas explicitamente, sem curingas.
	rt := reflect.TypeOf(wagerhttp.NewWagerHandler(nil, nil)).Elem()
	if rt.NumField() != 2 {
		t.Fatalf("WagerHandler fields = %d, want exactly 2 (service, metrics)", rt.NumField())
	}
	if got := rt.Field(0).Type; got != reflect.TypeOf((*application.WagerService)(nil)) {
		t.Fatalf("WagerHandler field 0 = %v, want *application.WagerService", got)
	}
	if got := rt.Field(1).Type; got != reflect.TypeOf((*observability.Metrics)(nil)) {
		t.Fatalf("WagerHandler field 1 = %v, want *observability.Metrics", got)
	}
}

func TestWagerHTTP_OpeningRejectedAtBoundary(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)

	body := fmt.Sprintf(
		`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
			`"money":{"amount":"100.00","currency":"BRL"},"kind":"OPENING"}`,
		f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID,
	)
	rec := f.post(t, http.MethodPost, body, "key-"+f.uuid(t), "application/json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if decodeBody(t, rec)["code"] != "INVALID_KIND" {
		t.Errorf("code = %v, want INVALID_KIND", decodeBody(t, rec)["code"])
	}

	// A fronteira barrou antes do use case: o service processaria OPENING
	// normalmente, então ausência total de persistência prova a não-chamada.
	var txCount int
	if err := f.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1`, wallet.ID,
	).Scan(&txCount); err != nil {
		t.Fatalf("count: %v", err)
	}
	if txCount != 0 {
		t.Errorf("transactions = %d, want 0 (service not called)", txCount)
	}
	stored, err := f.wallets.GetByID(context.Background(), wallet.ID)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if stored.Balance.Cents() != 10000 || stored.Version != 1 {
		t.Errorf("wallet = %d/v%d, want 10000/v1", stored.Balance.Cents(), stored.Version)
	}
	var ledgerCount int
	if err := f.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, wallet.ID,
	).Scan(&ledgerCount); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if ledgerCount != 0 {
		t.Errorf("ledger entries = %d, want 0", ledgerCount)
	}
}

func TestWagerHTTP_ResponseShapesFrozen(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	providerID := f.uuid(t)
	extTxID := "ext-" + f.uuid(t)
	key := "key-" + f.uuid(t)
	body := betBody(providerID, extTxID, wallet.PlayerID, wallet.ID, "30.00", "BRL")

	assertKeys := func(t *testing.T, body map[string]any, want []string) {
		t.Helper()
		if len(body) != len(want) {
			t.Fatalf("keys = %v, want exactly %v", keysOf(body), want)
		}
		for _, k := range want {
			if _, ok := body[k]; !ok {
				t.Fatalf("keys = %v, want exactly %v", keysOf(body), want)
			}
		}
	}

	first := f.post(t, http.MethodPost, body, key, "application/json")
	if first.Code != http.StatusOK {
		t.Fatalf("status = %d", first.Code)
	}
	firstBody := decodeBody(t, first)
	assertKeys(t, firstBody, []string{"transactionId", "status", "balance", "idempotentReplay"})
	if bal, _ := firstBody["balance"].(map[string]any); len(bal) != 2 {
		t.Errorf("balance keys = %v, want exactly [amount currency]", keysOf(bal))
	}

	second := f.post(t, http.MethodPost, body, key, "application/json")
	secondBody := decodeBody(t, second)
	assertKeys(t, secondBody, []string{"transactionId", "status", "balance", "idempotentReplay"})
	if secondBody["idempotentReplay"] != true {
		t.Errorf("idempotentReplay = %v, want true", secondBody["idempotentReplay"])
	}

	errRec := f.post(t, http.MethodPost, body, key, "application/json")
	if errRec.Code != http.StatusOK {
		t.Fatalf("third identical post status = %d, want 200", errRec.Code)
	}
	conflictBody := betBody(providerID, "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "50.00", "BRL")
	conflict := f.post(t, http.MethodPost, conflictBody, key, "application/json")
	assertKeys(t, decodeBody(t, conflict), []string{"code", "message"})
}

func keysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestWagerHTTP_RejectedReplayCarriesFailureCode(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	providerID := f.uuid(t)
	extTxID := "ext-" + f.uuid(t)
	key := "key-" + f.uuid(t)
	body := betBody(providerID, extTxID, wallet.PlayerID, wallet.ID, "200.00", "BRL")

	first := f.post(t, http.MethodPost, body, key, "application/json")
	if first.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", first.Code)
	}

	second := f.post(t, http.MethodPost, body, key, "application/json")
	if second.Code != http.StatusOK {
		t.Fatalf("replay status = %d, want 200", second.Code)
	}
	replay := decodeBody(t, second)
	if replay["status"] != "REJECTED" {
		t.Errorf("status = %v, want REJECTED", replay["status"])
	}
	if replay["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Errorf("failureCode = %v, want INSUFFICIENT_FUNDS", replay["failureCode"])
	}
	if replay["idempotentReplay"] != true {
		t.Errorf("idempotentReplay = %v, want true", replay["idempotentReplay"])
	}
}

func TestWagerHTTP_InProgress(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	providerID := f.uuid(t)
	extTxID := "ext-" + f.uuid(t)
	key := "key-" + f.uuid(t)

	// Seed PENDING com o fingerprint real do conteúdo que será postado.
	amount, err := domain.NewMoney(3000, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	seed, err := domain.NewWagerTransaction(
		f.uuid(t), providerID, extTxID, key, "hash-seed",
		wallet.PlayerID, wallet.ID, "round-987", "fortune-chimp",
		domain.TransactionBet, amount, "",
	)
	if err != nil {
		t.Fatalf("new wager transaction: %v", err)
	}
	fp, err := domain.CanonicalWagerFingerprint(*seed)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	seed.PayloadHash = fp
	transactions := postgres.NewWagerTransactionRepository(f.pool)
	ctx := context.Background()
	dbTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := transactions.Create(ctx, dbTx, seed); err != nil {
		_ = dbTx.Rollback(ctx)
		t.Fatalf("seed: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), "DELETE FROM wager_transactions WHERE id = $1", seed.ID)
	})

	body := betBody(providerID, extTxID, wallet.PlayerID, wallet.ID, "30.00", "BRL")
	rec := f.post(t, http.MethodPost, body, key, "application/json")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if decodeBody(t, rec)["code"] != "IN_PROGRESS" {
		t.Errorf("code = %v, want IN_PROGRESS", decodeBody(t, rec)["code"])
	}
}

func TestWagerHTTP_MissingKeyCreatesNothing(t *testing.T) {

	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	body := betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "10.00", "BRL")

	rec := f.post(t, http.MethodPost, body, "", "application/json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	var n int
	if err := f.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1`, wallet.ID,
	).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("transactions = %d, want 0 (use case not reached)", n)
	}
}

func postWithRequestID(t *testing.T, f *httpFixture, body, key, requestID string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	if requestID != "" {
		req.Header.Set("X-Request-ID", requestID)
	}
	rec := httptest.NewRecorder()
	f.handler.ProcessWager(rec, req)
	return rec
}

func TestWagerHTTP_RequestIDEchoed(t *testing.T) {
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	body := betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "10.00", "BRL")

	// ID válido do cliente é ecoado.
	rec := postWithRequestID(t, f, body, "key-"+f.uuid(t), "client-req-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Request-ID"); got != "client-req-1" {
		t.Errorf("X-Request-ID = %q, want client-req-1", got)
	}

	// Ausente: gerado e ecoado.
	rec = postWithRequestID(t, f, body, "key-"+f.uuid(t), "")
	if got := rec.Header().Get("X-Request-ID"); got == "" {
		t.Error("missing X-Request-ID echo for generated ID")
	}

	// Inválido: substituído, nunca ecoado cru.
	rec = postWithRequestID(t, f, body, "key-"+f.uuid(t), "bad\nid")
	if got := rec.Header().Get("X-Request-ID"); got == "bad\nid" || got == "" {
		t.Errorf("X-Request-ID = %q, want generated replacement", got)
	}
}

func TestWagerHTTP_OutcomeMetrics(t *testing.T) {
	f := newHTTPFixture(t)

	// Sucesso conta transação por kind/status.
	wallet := f.createWallet(t, 10000)
	okBody := betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "30.00", "BRL")
	if rec := f.post(t, http.MethodPost, okBody, "key-"+f.uuid(t), "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := f.metrics.Value("wager_transactions_total", map[string]string{"kind": "BET", "status": "PROCESSED"}); got != 1 {
		t.Errorf("transactions BET/PROCESSED = %d, want 1", got)
	}

	// Rejeição de negócio conta por failure code.
	rejWallet := f.createWallet(t, 10000)
	rejBody := betBody(f.uuid(t), "ext-"+f.uuid(t), rejWallet.PlayerID, rejWallet.ID, "200.00", "BRL")
	if rec := f.post(t, http.MethodPost, rejBody, "key-"+f.uuid(t), "application/json"); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	if got := f.metrics.Value("wager_transaction_rejections_total", map[string]string{"failure_code": "INSUFFICIENT_FUNDS"}); got != 1 {
		t.Errorf("rejections INSUFFICIENT_FUNDS = %d, want 1", got)
	}

	// Erro de validação não vira transação/rejeição/falha.
	badBody := betBody(f.uuid(t), "ext-"+f.uuid(t), wallet.PlayerID, wallet.ID, "10.00", "BRL")
	if rec := f.post(t, http.MethodPost, badBody, "", "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if got := f.metrics.Value("wager_transactions_total", map[string]string{"kind": "BET", "status": "REJECTED"}); got != 0 {
		t.Errorf("unexpected rejected transaction count = %d", got)
	}

	// Exposição sem labels de alta cardinalidade.
	var b strings.Builder
	f.metrics.WritePrometheus(&b)
	for _, forbidden := range []string{"transactionId=", "externalTransactionId=", "idempotencyKey=", "providerId=", "walletId="} {
		if strings.Contains(b.String(), forbidden) {
			t.Errorf("metrics leak high-cardinality label %q", forbidden)
		}
	}
}
