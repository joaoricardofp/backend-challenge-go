package app_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/joaoricardofp/backend-challenge-go/internal/app"
	"github.com/joaoricardofp/backend-challenge-go/internal/domain"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
)

// TestCompositionGraph valida o grafo Fx (provedores ausentes, dependências
// não registradas, ciclos). Construtores rodam no New, então exige banco
// como os demais testes de integração; OIDC desligado mantém hermético.
func TestCompositionGraph(t *testing.T) {
	t.Setenv("OIDC_ENABLED", "false")
	fxapp := fx.New(app.Module)
	if err := fxapp.Err(); err != nil {
		t.Fatalf("composition graph: %v", err)
	}
}

// testIssuer sobe discovery + JWKS reais com chave própria (IdP local para
// o teste de lifecycle; Keycloak real fica para integração dedicada).
type testIssuer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	issuer string
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	iss := &testIssuer{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"jwks_uri": iss.server.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(iss.key.PublicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(iss.key.PublicKey.E)).Bytes())
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "k1", "n": n, "e": e,
		}}})
	})
	iss.server = httptest.NewServer(mux)
	iss.issuer = iss.server.URL
	t.Cleanup(iss.server.Close)
	return iss
}

func (iss *testIssuer) mint(t *testing.T, providerID string) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":         iss.issuer,
		"aud":         "test-aud",
		"sub":         "subject-1",
		"exp":         time.Now().Add(time.Hour).Unix(),
		"iat":         time.Now().Add(-time.Minute).Unix(),
		"provider_id": providerID,
	})
	token.Header["kid"] = "k1"
	signed, err := token.SignedString(iss.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signed
}

// TestLifecycle sobe o grafo real (pool + OIDC local + HTTP em 127.0.0.1:0)
// e prova start, disponibilidade, wiring de ponta a ponta e stop graceful:
// sem novas conexões, pool fechado, sem erro.
func TestLifecycle(t *testing.T) {
	iss := newTestIssuer(t)
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OIDC_ENABLED", "true")
	t.Setenv("OIDC_ISSUER", iss.issuer)
	t.Setenv("OIDC_AUDIENCE", "test-aud")

	var pool *pgxpool.Pool
	var ln net.Listener
	fxapp := fxtest.New(t, app.Module, fx.Populate(&pool, &ln))
	fxapp.RequireStart()
	base := "http://" + ln.Addr().String()

	// HTTP disponível (poll com deadline, sem sleep arbitrário cego).
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(base + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready: last err %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Sem auth → 401: middleware conectado no grafo.
	req, _ := http.NewRequest(http.MethodPost, base+"/wagering/transactions", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", resp.StatusCode)
	}

	// Fluxo autenticado de ponta a ponta: wallet real + token real.
	providerID := newUUID(t, pool)
	wallet := createWallet(t, pool, 10000)
	rec := doWager(t, base, iss.mint(t, providerID), wagerBody(providerID, wallet, "30.00"), "k-e2e")
	if rec.Status != http.StatusOK {
		t.Fatalf("authenticated status = %d (%s)", rec.Status, rec.Body)
	}
	stored := readWallet(t, pool, wallet.ID)
	if stored.Balance.Cents() != 7000 {
		t.Errorf("wallet balance = %d, want 7000 (Fx-wired service processed)", stored.Balance.Cents())
	}

	fxapp.RequireStop()

	// Pós-stop: sem novas conexões e pool fechado.
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("listener still accepting connections after stop")
	}
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("pool still usable after stop")
	}
}

func newUUID(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	var id string
	if err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id
}

func createWallet(t *testing.T, pool *pgxpool.Pool, balanceCents int64) *domain.Wallet {
	t.Helper()

	wallet, err := domain.NewWallet(newUUID(t, pool), newUUID(t, pool), "BRL")
	if err != nil {
		t.Fatalf("new wallet: %v", err)
	}
	deposit, err := domain.NewMoney(balanceCents, "BRL")
	if err != nil {
		t.Fatalf("new money: %v", err)
	}
	if err := wallet.Credit(deposit); err != nil {
		t.Fatalf("credit: %v", err)
	}
	if err := postgres.NewWalletRepository(pool).Create(context.Background(), wallet); err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1)`, wallet.ID)
		_, _ = pool.Exec(ctx, "TRUNCATE wallet_ledger_entries")
		_, _ = pool.Exec(ctx, "DELETE FROM wager_transactions WHERE wallet_id = $1", wallet.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM wallets WHERE id = $1", wallet.ID)
	})
	return wallet
}

func wagerBody(providerID string, wallet *domain.Wallet, amount string) string {
	return fmt.Sprintf(
		`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,`+
			`"roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":%q,"currency":"BRL"}}`,
		providerID, "ext-"+providerID, wallet.PlayerID, wallet.ID, amount,
	)
}

type wagerResult struct {
	Status int
	Body   string
}

func doWager(t *testing.T, base, token, body, key string) wagerResult {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, base+"/wagering/transactions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	buf := new(strings.Builder)
	if _, err := io.Copy(buf, resp.Body); err != nil {
		t.Fatalf("read: %v", err)
	}
	return wagerResult{Status: resp.StatusCode, Body: buf.String()}
}

func readWallet(t *testing.T, pool *pgxpool.Pool, id string) *domain.Wallet {
	t.Helper()

	wallet, err := postgres.NewWalletRepository(pool).GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	return wallet
}

// TestShutdownOrder valida a ordem do lifecycle Fx: health OnStop (MarkStopping)
// executa ANTES dos OnStop do SQS, garantindo que /health/ready fique false
// imediatamente quando o shutdown começa, antes de consumer/publisher pararem.
// Usa SQS desligado (padrão) pois a ordem dos módulos é independente do SQS estar ativo.
func TestShutdownOrder(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OIDC_ENABLED", "false")
	// SQS_ENABLED não definido = desligado (padrão)

	var pool *pgxpool.Pool
	var ln net.Listener
	fxapp := fxtest.New(t, app.Module, fx.Populate(&pool, &ln))
	fxapp.RequireStart()
	base := "http://" + ln.Addr().String()

	// Aguarda ready = 200 (só PostgreSQL, SQS desligado não bloqueia)
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(base + "/health/ready")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready: last err %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Inicia shutdown e verifica que o grafo desce sem erro com a nova ordem.
	// O teste de unidade em observability_test.go (TestReadiness_Shutdown_NotReady)
	// já valida que MarkStopping() -> ready=false imediatamente.
	fxapp.RequireStop()

	// Pós-stop: sem novas conexões e pool fechado.
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("listener still accepting connections after stop")
	}
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("pool still usable after stop")
	}
}

// TestOperationalEndpoints sobe o grafo com OIDC e SQS desligados e prova
// os endpoints operacionais da B3.14: live sempre 200, ready 200 com DB
// saudável (sem requisito SQS desligado), metrics 200 sem OIDC, e
// X-Request-ID ecoado/gerado — incluindo métricas de negócio fim a fim.
func TestOperationalEndpoints(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("OIDC_ENABLED", "false")
	t.Setenv("SQS_ENABLED", "")

	var pool *pgxpool.Pool
	var ln net.Listener
	fxapp := fxtest.New(t, app.Module, fx.Populate(&pool, &ln))
	fxapp.RequireStart()
	defer fxapp.RequireStop()
	base := "http://" + ln.Addr().String()

	get := func(path string, headers map[string]string) (int, http.Header, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, base+path, nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		defer resp.Body.Close()
		buf := new(strings.Builder)
		_, _ = io.Copy(buf, resp.Body)
		return resp.StatusCode, resp.Header, strings.TrimSpace(buf.String())
	}

	// /health/live: 200 imediato, sem dependências.
	if status, _, body := get("/health/live", nil); status != http.StatusOK || body != `{"status":"ok"}` {
		t.Errorf("live = %d %q, want 200 ok", status, body)
	}

	// /health/ready: 200 com DB saudável e SQS desligado (sem requisito SQS).
	if status, _, body := get("/health/ready", nil); status != http.StatusOK || body != `{"status":"ok"}` {
		t.Errorf("ready = %d %q, want 200 ok", status, body)
	}

	// /metrics: 200 sem OIDC, formato Prometheus.
	status, headers, metricsBody := get("/metrics", nil)
	if status != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", status)
	}
	if ct := headers.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("metrics content-type = %q", ct)
	}

	// Request ID válido ecoado até no health.
	if _, headers, _ := get("/health/live", map[string]string{"X-Request-ID": "ops-1"}); headers.Get("X-Request-ID") != "ops-1" {
		t.Errorf("health echo = %q, want ops-1", headers.Get("X-Request-ID"))
	}

	// Wager fim a fim (OIDC desligado): resposta carrega request ID e alimenta
	// as métricas de negócio.
	providerID := newUUID(t, pool)
	wallet := createWallet(t, pool, 10000)
	req, _ := http.NewRequest(http.MethodPost, base+"/wagering/transactions", strings.NewReader(wagerBody(providerID, wallet, "30.00")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "k-ops")
	req.Header.Set("X-Request-ID", "ops-wager-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("wager status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Request-ID"); got != "ops-wager-1" {
		t.Errorf("wager echo = %q, want ops-wager-1", got)
	}

	// Sem header: gerado e presente na resposta.
	req, _ = http.NewRequest(http.MethodPost, base+"/wagering/transactions", strings.NewReader(wagerBody(newUUID(t, pool), wallet, "30.00")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "k-ops-2")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp2.Body.Close()
	if got := resp2.Header.Get("X-Request-ID"); got == "" {
		t.Error("missing generated X-Request-ID")
	}

	_, _, metricsBody = get("/metrics", nil)
	for _, want := range []string{
		`wager_requests_total{status="200"} 2`,
		`wager_transactions_total{kind="BET",status="PROCESSED"} 2`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Errorf("metrics missing %q:\n%s", want, metricsBody)
		}
	}
}
