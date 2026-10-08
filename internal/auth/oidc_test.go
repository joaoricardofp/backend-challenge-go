package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/joaoricardofp/backend-challenge-go/internal/auth"
)

// testIssuer é um OIDC issuer controlado: discovery + JWKS reais com chave
// RSA gerada no teste. A validação exercitada é a de produção (assinatura,
// expiração, issuer, audience via JWKS); só o IdP é local, não o Keycloak.
type testIssuer struct {
	server  *httptest.Server
	key     *rsa.PrivateKey
	keyID   string
	issuer  string
	otherKP *rsa.PrivateKey
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	iss := &testIssuer{key: key, keyID: "test-kid-1", otherKP: other}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"jwks_uri": iss.server.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(iss.key.PublicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(iss.key.PublicKey.E)).Bytes())
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": iss.keyID, "n": n, "e": e,
		}}})
	})
	iss.server = httptest.NewServer(mux)
	iss.issuer = iss.server.URL
	t.Cleanup(iss.server.Close)
	return iss
}

func (iss *testIssuer) mint(t *testing.T, key *rsa.PrivateKey, kid, issuer, audience, subject, providerID string, expires time.Time, method jwt.SigningMethod) string {
	t.Helper()

	claims := jwt.MapClaims{
		"iss": issuer,
		"aud": audience,
		"sub": subject,
		"exp": expires.Unix(),
		"iat": time.Now().Add(-time.Minute).Unix(),
	}
	if providerID != "" {
		claims["provider_id"] = providerID
	}
	token := jwt.NewWithClaims(method, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signed
}

func (iss *testIssuer) validToken(t *testing.T, audience, providerID string) string {
	t.Helper()
	return iss.mint(t, iss.key, iss.keyID, iss.issuer, audience, "subject-1", providerID, time.Now().Add(time.Hour), jwt.SigningMethodRS256)
}

func newVerifier(t *testing.T, iss *testIssuer, audience string) *auth.Verifier {
	t.Helper()

	v, err := auth.NewVerifier(context.Background(), auth.Config{Enabled: true, Issuer: iss.issuer, Audience: audience})
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	return v
}

func TestVerifier_ValidToken(t *testing.T) {
	iss := newTestIssuer(t)
	v := newVerifier(t, iss, "wager-api")

	p, err := v.Validate(context.Background(), iss.validToken(t, "wager-api", "provider-a"))
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if p.ProviderID != "provider-a" {
		t.Errorf("ProviderID = %q, want provider-a", p.ProviderID)
	}
	if p.Subject != "subject-1" {
		t.Errorf("Subject = %q, want subject-1", p.Subject)
	}
}

func TestVerifier_Rejects(t *testing.T) {
	iss := newTestIssuer(t)
	v := newVerifier(t, iss, "wager-api")
	future := time.Now().Add(time.Hour)

	for _, tc := range []struct {
		name  string
		token func(t *testing.T) string
	}{
		{"empty", func(t *testing.T) string { return "" }},
		{"malformed", func(t *testing.T) string { return "not.a.token" }},
		{"wrong signature", func(t *testing.T) string {
			return iss.mint(t, iss.otherKP, iss.keyID, iss.issuer, "wager-api", "s", "p", future, jwt.SigningMethodRS256)
		}},
		{"unknown kid", func(t *testing.T) string {
			return iss.mint(t, iss.key, "unknown-kid", iss.issuer, "wager-api", "s", "p", future, jwt.SigningMethodRS256)
		}},
		{"expired", func(t *testing.T) string {
			return iss.mint(t, iss.key, iss.keyID, iss.issuer, "wager-api", "s", "p", time.Now().Add(-time.Hour), jwt.SigningMethodRS256)
		}},
		{"wrong issuer", func(t *testing.T) string {
			return iss.mint(t, iss.key, iss.keyID, "https://other-issuer", "wager-api", "s", "p", future, jwt.SigningMethodRS256)
		}},
		{"wrong audience", func(t *testing.T) string {
			return iss.mint(t, iss.key, iss.keyID, iss.issuer, "other-aud", "s", "p", future, jwt.SigningMethodRS256)
		}},
		{"hs256 confusion", func(t *testing.T) string {
			tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": iss.issuer})
			s, err := tok.SignedString([]byte("secret"))
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			return s
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.Validate(context.Background(), tc.token(t))
			if !errors.Is(err, auth.ErrInvalidToken) {
				t.Fatalf("error = %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestVerifier_MissingProviderClaim(t *testing.T) {
	iss := newTestIssuer(t)
	v := newVerifier(t, iss, "wager-api")

	token := iss.mint(t, iss.key, iss.keyID, iss.issuer, "wager-api", "subject-1", "", time.Now().Add(time.Hour), jwt.SigningMethodRS256)
	_, err := v.Validate(context.Background(), token)
	if !errors.Is(err, auth.ErrUnknownProvider) {
		t.Fatalf("error = %v, want ErrUnknownProvider", err)
	}
}

func TestVerifier_EmptyAudienceSkipsCheck(t *testing.T) {
	iss := newTestIssuer(t)
	v := newVerifier(t, iss, "")

	p, err := v.Validate(context.Background(), iss.validToken(t, "any-audience", "provider-a"))
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if p.ProviderID != "provider-a" {
		t.Errorf("ProviderID = %q", p.ProviderID)
	}
}

func TestNewVerifier_Config(t *testing.T) {
	t.Run("disabled refuses", func(t *testing.T) {
		if _, err := auth.NewVerifier(context.Background(), auth.Config{Enabled: false}); err == nil {
			t.Fatal("expected error for disabled config")
		}
	})

	t.Run("enabled without issuer refuses", func(t *testing.T) {
		if _, err := auth.NewVerifier(context.Background(), auth.Config{Enabled: true}); err == nil {
			t.Fatal("expected error for missing issuer")
		}
	})

	t.Run("unreachable issuer refuses", func(t *testing.T) {
		if _, err := auth.NewVerifier(context.Background(), auth.Config{Enabled: true, Issuer: "http://127.0.0.1:1"}); err == nil {
			t.Fatal("expected error for unreachable issuer")
		}
	})
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("OIDC_ENABLED", "")
	t.Setenv("OIDC_ISSUER", "https://issuer")
	t.Setenv("OIDC_AUDIENCE", "aud")

	cfg := auth.ConfigFromEnv()
	if !cfg.Enabled {
		t.Error("Enabled = false, want true by default (fail-closed)")
	}
	if cfg.Issuer != "https://issuer" || cfg.Audience != "aud" {
		t.Errorf("config = %+v", cfg)
	}

	t.Setenv("OIDC_ENABLED", "false")
	if auth.ConfigFromEnv().Enabled {
		t.Error("Enabled = true, want false")
	}
}

func TestPrincipalContext(t *testing.T) {
	ctx := auth.WithPrincipal(context.Background(), auth.Principal{ProviderID: "p", Subject: "s"})
	got, ok := auth.PrincipalFrom(ctx)
	if !ok {
		t.Fatal("principal missing")
	}
	if got.ProviderID != "p" || got.Subject != "s" {
		t.Errorf("principal = %+v", got)
	}

	if _, ok := auth.PrincipalFrom(context.Background()); ok {
		t.Error("unexpected principal in empty context")
	}
}
