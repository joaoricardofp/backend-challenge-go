package http_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/auth"
	wagerhttp "github.com/joaoricardofp/backend-challenge-go/internal/transport/http"
)

// testIssuer sobe discovery + JWKS reais com chave própria. A validação é a
// de produção; o IdP é local (Keycloak real fica para integração dedicada).
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

func (iss *testIssuer) mint(t *testing.T, providerID string, expires time.Time) string {
	t.Helper()

	claims := jwt.MapClaims{
		"iss": iss.issuer,
		"aud": "wager-api",
		"sub": "subject-1",
		"exp": expires.Unix(),
		"iat": time.Now().Add(-time.Minute).Unix(),
	}
	if providerID != "" {
		claims["provider_id"] = providerID
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "k1"
	signed, err := token.SignedString(iss.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signed
}

func newTestVerifier(t *testing.T, iss *testIssuer) *auth.Verifier {
	t.Helper()

	v, err := auth.NewVerifier(context.Background(), auth.Config{Enabled: true, Issuer: iss.issuer, Audience: "wager-api"})
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	return v
}

type stubNext struct {
	called    bool
	principal auth.Principal
	hasAuth   bool
}

func (s *stubNext) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.called = true
	s.principal, s.hasAuth = auth.PrincipalFrom(r.Context())
	w.WriteHeader(http.StatusTeapot)
}

func authedRequest(t *testing.T, token, body string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "k")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

const authTestBody = `{"providerId":"provider-a","externalTransactionId":"e1","playerId":"p1",` +
	`"walletId":"w1","roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}`

func TestRequireProviderAuth(t *testing.T) {
	iss := newTestIssuer(t)
	v := newTestVerifier(t, iss)
	future := time.Now().Add(time.Hour)

	t.Run("missing header", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(authTestBody))
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusUnauthorized, "MISSING_CREDENTIALS")
		if next.called {
			t.Error("next called without credentials")
		}
	})

	t.Run("wrong scheme", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := authedRequest(t, "", authTestBody)
		req.Header.Set("Authorization", "Basic abc")
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusUnauthorized, "MISSING_CREDENTIALS")
		if next.called {
			t.Error("next called")
		}
	})

	t.Run("empty bearer", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := authedRequest(t, "", authTestBody)
		req.Header.Set("Authorization", "Bearer ")
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusUnauthorized, "MISSING_CREDENTIALS")
		if next.called {
			t.Error("next called")
		}
	})

	t.Run("garbage token", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := authedRequest(t, "garbage", authTestBody)
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusUnauthorized, "INVALID_TOKEN")
		if next.called {
			t.Error("next called")
		}
	})

	t.Run("expired token", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := authedRequest(t, iss.mint(t, "provider-a", time.Now().Add(-time.Hour)), authTestBody)
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusUnauthorized, "INVALID_TOKEN")
		if next.called {
			t.Error("next called")
		}
	})

	t.Run("valid token matching provider", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := authedRequest(t, iss.mint(t, "provider-a", future), authTestBody)
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		if !next.called {
			t.Fatal("next not called for valid token")
		}
		if !next.hasAuth || next.principal.ProviderID != "provider-a" {
			t.Errorf("principal = %+v", next.principal)
		}
	})

	t.Run("impersonation rejected", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		body := strings.Replace(authTestBody, `"providerId":"provider-a"`, `"providerId":"provider-b"`, 1)
		req := authedRequest(t, iss.mint(t, "provider-a", future), body)
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusForbidden, "PROVIDER_MISMATCH")
		if next.called {
			t.Error("next called on impersonation")
		}
	})

	t.Run("missing provider claim", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := authedRequest(t, iss.mint(t, "", future), authTestBody)
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		assertCode(t, rec, http.StatusForbidden, "PROVIDER_UNKNOWN")
		if next.called {
			t.Error("next called")
		}
	})

	t.Run("malformed body passes through to handler", func(t *testing.T) {
		next := &stubNext{}
		rec := httptest.NewRecorder()
		req := authedRequest(t, iss.mint(t, "provider-a", future), `{broken`)
		wagerhttp.RequireProviderAuth(v, next).ServeHTTP(rec, req)
		if !next.called {
			t.Error("next not called; handler owns body validation")
		}
	})
}

func assertCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf("status = %d, want %d (%s)", rec.Code, status, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	if body["code"] != code {
		t.Errorf("code = %v, want %s", body["code"], code)
	}
}

func TestWagerHTTP_AuthenticatedEndToEnd(t *testing.T) {
	iss := newTestIssuer(t)
	v := newTestVerifier(t, iss)
	f := newHTTPFixture(t)
	wallet := f.createWallet(t, 10000)
	future := time.Now().Add(time.Hour)

	chain := wagerhttp.RequireProviderAuth(v, http.HandlerFunc(f.handler.ProcessWager))

	doPost := func(t *testing.T, token, body, key string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		chain.ServeHTTP(rec, req)
		return rec
	}
	bodyFor := func(provider string) string {
		return `{"providerId":"` + provider + `","externalTransactionId":"ext-1",` +
			`"playerId":"` + wallet.PlayerID + `","walletId":"` + wallet.ID + `",` +
			`"roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}`
	}
	providerA := f.uuid(t)
	providerB := f.uuid(t)

	t.Run("no token rejected before service", func(t *testing.T) {
		rec := doPost(t, "", bodyFor(providerA), "k1")
		assertCode(t, rec, http.StatusUnauthorized, "MISSING_CREDENTIALS")
	})

	t.Run("matching provider processes", func(t *testing.T) {
		rec := doPost(t, iss.mint(t, providerA, future), bodyFor(providerA), "k2")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("impersonation rejected without side effects", func(t *testing.T) {
		rec := doPost(t, iss.mint(t, providerA, future), bodyFor(providerB), "k3")
		assertCode(t, rec, http.StatusForbidden, "PROVIDER_MISMATCH")

		stored, err := f.wallets.GetByID(context.Background(), wallet.ID)
		if err != nil {
			t.Fatalf("get wallet: %v", err)
		}
		if stored.Balance.Cents() != 9000 || stored.Version != 2 {
			t.Errorf("wallet = %d/v%d, want 9000/v2 (only the legitimate BET)", stored.Balance.Cents(), stored.Version)
		}
		var n int
		if err := f.pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1`, wallet.ID,
		).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n != 1 {
			t.Errorf("transactions = %d, want 1", n)
		}
	})
}

func TestApplicationStaysTransportAgnostic(t *testing.T) {
	// Prova estrutural: WagerService não carrega auth/JWT/HTTP nos tipos dos
	// seus campos. Infra de transporte termina no adapter.
	rt := reflect.TypeOf((*application.WagerService)(nil)).Elem()
	for i := 0; i < rt.NumField(); i++ {
		pkg := rt.Field(i).Type.PkgPath()
		for _, banned := range []string{"auth", "jwt", "net/http", "transport"} {
			if strings.Contains(pkg, banned) {
				t.Errorf("WagerService field %s has transport/auth type %v", rt.Field(i).Name, rt.Field(i).Type)
			}
		}
	}
}
