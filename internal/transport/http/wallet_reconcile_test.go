package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
	wallethttp "github.com/joaoricardofp/backend-challenge-go/internal/transport/http"
)

type reconcileHTTPFixture struct {
	pool    *pgxpool.Pool
	handler *wallethttp.WalletHandler
	chain   http.Handler
	issuer  *testIssuer
	metrics *observability.Metrics
}

func newReconcileHTTPFixture(t *testing.T) *reconcileHTTPFixture {
	t.Helper()
	f := newHTTPFixture(t)

	walletService := application.NewWalletService(
		f.pool,
		postgres.NewWalletRepository(f.pool),
		postgres.NewWagerTransactionRepository(f.pool),
		postgres.NewLedgerRepository(f.pool),
		postgres.NewOutboxRepository(f.pool),
	)
	metrics := observability.NewMetrics()
	handler := wallethttp.NewWalletHandler(walletService, nil, metrics)

	iss := newTestIssuer(t)
	verifier := newTestVerifier(t, iss)
	chain := wallethttp.RequireInternalServiceAuth(verifier, http.HandlerFunc(handler.ReconcileWallet))

	return &reconcileHTTPFixture{pool: f.pool, handler: handler, chain: chain, issuer: iss, metrics: metrics}
}

func (f *reconcileHTTPFixture) uuid(t *testing.T) string {
	t.Helper()
	var id string
	if err := f.pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id
}

// seedWallet cria wallet + (opcionalmente) uma entrada de crédito válida.
func (f *reconcileHTTPFixture) seedWallet(t *testing.T, storedCents, ledgerCents int64) string {
	t.Helper()
	ctx := context.Background()
	walletID := f.uuid(t)
	playerID := f.uuid(t)
	if _, err := f.pool.Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance, version) VALUES ($1, $2, 'BRL', $3, 1)`,
		walletID, playerID, storedCents); err != nil {
		t.Fatalf("seed wallet: %v", err)
	}
	if ledgerCents > 0 {
		txID := f.uuid(t)
		uniq := f.uuid(t)
		if _, err := f.pool.Exec(ctx,
			`INSERT INTO wager_transactions
				(id, provider_id, external_transaction_id, idempotency_key, payload_hash,
				 player_id, wallet_id, kind, status, amount, currency)
			 VALUES ($1, $2, $3, $4, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', $5, $6, 'BET', 'PROCESSED', $7, 'BRL')`,
			txID, f.uuid(t), "http-recon-ext-"+uniq, "http-recon-key-"+uniq, playerID, walletID, ledgerCents); err != nil {
			t.Fatalf("seed transaction: %v", err)
		}
		if _, err := f.pool.Exec(ctx,
			`INSERT INTO wallet_ledger_entries
				(id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
			 VALUES ($1, $2, $3, 'CREDIT', $4, 0, $4, $5)`,
			f.uuid(t), walletID, txID, ledgerCents, time.Now().Add(-time.Hour)); err != nil {
			t.Fatalf("seed ledger entry: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`, walletID)
		_, _ = f.pool.Exec(ctx, `TRUNCATE wallet_ledger_entries`)
		_, _ = f.pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1`, walletID)
		_, _ = f.pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, walletID)
	})
	return walletID
}

func (f *reconcileHTTPFixture) doReconcile(token, walletID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/wallets/"+walletID+"/reconciliation", nil)
	req.SetPathValue("walletId", walletID)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.chain.ServeHTTP(rec, req)
	return rec
}

func TestReconcileHTTP(t *testing.T) {
	f := newReconcileHTTPFixture(t)
	future := time.Now().Add(time.Hour)
	internalToken := f.issuer.mint(t, wallethttp.InternalProviderID, future)
	externalToken := f.issuer.mint(t, f.uuid(t), future)

	healthyWallet := f.seedWallet(t, 10000, 10000)
	divergentWallet := f.seedWallet(t, 8000, 10000)

	t.Run("reconciled wallet returns 200 with JSON contract", func(t *testing.T) {
		rec := f.doReconcile(internalToken, healthyWallet)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		if body["walletId"] != healthyWallet {
			t.Errorf("walletId = %v, want %s", body["walletId"], healthyWallet)
		}
		if body["consistent"] != true {
			t.Errorf("consistent = %v, want true (%s)", body["consistent"], rec.Body.String())
		}
		stored, _ := body["storedBalance"].(map[string]any)
		calculated, _ := body["calculatedBalance"].(map[string]any)
		difference, _ := body["difference"].(map[string]any)
		if stored["amount"] != "100.00" || stored["currency"] != "BRL" {
			t.Errorf("storedBalance = %v, want 100.00 BRL", stored)
		}
		if calculated["amount"] != "100.00" || calculated["currency"] != "BRL" {
			t.Errorf("calculatedBalance = %v, want 100.00 BRL", calculated)
		}
		if difference["amount"] != "0.00" || difference["currency"] != "BRL" {
			t.Errorf("difference = %v, want 0.00 BRL", difference)
		}
		if body["checkedEntries"] != float64(1) {
			t.Errorf("checkedEntries = %v, want 1", body["checkedEntries"])
		}
		if issues, _ := body["issues"].([]any); issues == nil || len(issues) != 0 {
			t.Errorf("issues = %v, want empty array", body["issues"])
		}
	})

	t.Run("divergent wallet returns 200 with consistent false and issues", func(t *testing.T) {
		rec := f.doReconcile(internalToken, divergentWallet)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		if body["consistent"] != false {
			t.Errorf("consistent = %v, want false", body["consistent"])
		}
		issues, _ := body["issues"].([]any)
		if len(issues) == 0 {
			t.Fatalf("issues empty, want at least BALANCE_MISMATCH (%s)", rec.Body.String())
		}
		found := false
		for _, raw := range issues {
			if issue, _ := raw.(map[string]any); issue["type"] == "BALANCE_MISMATCH" {
				found = true
			}
		}
		if !found {
			t.Errorf("no BALANCE_MISMATCH in %s", rec.Body.String())
		}
	})

	t.Run("unknown wallet returns 404 without leaking internals", func(t *testing.T) {
		rec := f.doReconcile(internalToken, f.uuid(t))
		assertCode(t, rec, http.StatusNotFound, "WALLET_NOT_FOUND")
	})

	t.Run("missing credentials returns 401", func(t *testing.T) {
		rec := f.doReconcile("", healthyWallet)
		assertCode(t, rec, http.StatusUnauthorized, "MISSING_CREDENTIALS")
	})

	t.Run("invalid token returns 401", func(t *testing.T) {
		rec := f.doReconcile("garbage", healthyWallet)
		assertCode(t, rec, http.StatusUnauthorized, "INVALID_TOKEN")
	})

	t.Run("external provider cannot reconcile and learns nothing", func(t *testing.T) {
		existing := f.doReconcile(externalToken, healthyWallet)
		assertCode(t, existing, http.StatusForbidden, "INTERNAL_SERVICE_REQUIRED")

		missing := f.doReconcile(externalToken, f.uuid(t))
		assertCode(t, missing, http.StatusForbidden, "INTERNAL_SERVICE_REQUIRED")

		if existing.Body.String() != missing.Body.String() {
			t.Errorf("responses differ, existence leaked:\n%s\n%s", existing.Body.String(), missing.Body.String())
		}
	})

	t.Run("reconciliation outcomes are counted in metrics", func(t *testing.T) {
		beforeConsistent := f.metrics.Value("wallet_reconciliations_total", map[string]string{"result": "consistent"})
		beforeInconsistent := f.metrics.Value("wallet_reconciliations_total", map[string]string{"result": "inconsistent"})

		if rec := f.doReconcile(internalToken, healthyWallet); rec.Code != http.StatusOK {
			t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
		}
		if rec := f.doReconcile(internalToken, divergentWallet); rec.Code != http.StatusOK {
			t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
		}

		if got := f.metrics.Value("wallet_reconciliations_total", map[string]string{"result": "consistent"}); got != beforeConsistent+1 {
			t.Errorf("consistent count = %d, want %d", got, beforeConsistent+1)
		}
		if got := f.metrics.Value("wallet_reconciliations_total", map[string]string{"result": "inconsistent"}); got != beforeInconsistent+1 {
			t.Errorf("inconsistent count = %d, want %d", got, beforeInconsistent+1)
		}
	})

	t.Run("wrong method returns 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/wallets/"+healthyWallet+"/reconciliation", nil)
		req.SetPathValue("walletId", healthyWallet)
		req.Header.Set("Authorization", "Bearer "+internalToken)
		rec := httptest.NewRecorder()
		f.chain.ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	})
}
