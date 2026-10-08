// Package auth valida Bearer tokens OIDC (Keycloak como IdP de referência)
// e extrai a identidade autenticada da requisição.
//
// Convenção deste projeto (o README não define claim exato): o claim
// `provider_id` determina o provider efetivo. `sub` é transportado como
// identificação do subject. Nenhum outro claim é interpretado.
//
// Somente RS256 é aceito (padrão do Keycloak). Chaves vêm do JWKS do issuer
// via OIDC Discovery, com cache em memória e refresh em kid desconhecido.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ErrInvalidToken indica token malformado, com assinatura desconhecida,
// expirado, ou com issuer/audience fora do configurado.
var ErrInvalidToken = errors.New("invalid token")

// ErrUnknownProvider indica token criptograficamente válido mas sem claim
// `provider_id`: não identifica nenhum provider para este endpoint.
var ErrUnknownProvider = errors.New("unknown provider")

// Config concentra a configuração OIDC. Nada é hardcoded: issuer e audience
// vêm de ambiente.
type Config struct {
	Enabled  bool
	Issuer   string
	Audience string
}

// ConfigFromEnv lê OIDC_ENABLED (default true, fail-closed), OIDC_ISSUER e
// OIDC_AUDIENCE. Com enabled e sem issuer, NewVerifier recusa iniciar.
func ConfigFromEnv() Config {
	enabled := true
	if v := strings.TrimSpace(os.Getenv("OIDC_ENABLED")); v != "" {
		enabled = v != "false" && v != "0"
	}
	return Config{
		Enabled:  enabled,
		Issuer:   strings.TrimSpace(os.Getenv("OIDC_ISSUER")),
		Audience: strings.TrimSpace(os.Getenv("OIDC_AUDIENCE")),
	}
}

// Principal é a identidade autenticada mínima transportada no contexto.
// Apenas strings: nenhum claim bruto, JWT ou chave é copiado.
type Principal struct {
	ProviderID string
	Subject    string
}

type principalKey struct{}

// WithPrincipal anexa a identidade autenticada ao contexto.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom recupera a identidade do contexto, se presente.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// oidcClaims são os únicos claims interpretados: os registrados do JWT
// mais `provider_id` (convenção do projeto).
type oidcClaims struct {
	jwt.RegisteredClaims
	ProviderID string `json:"provider_id"`
}

type discoveryDocument struct {
	JWKSURI string `json:"jwks_uri"`
}

type jwksDocument struct {
	Keys []jwksKey `json:"keys"`
}

type jwksKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

const jwksCacheTTL = 5 * time.Minute

// Verifier valida tokens contra o JWKS do issuer.
type Verifier struct {
	client   *http.Client
	issuer   string
	audience string

	mu      sync.Mutex
	keys    map[string]*rsa.PublicKey
	expires time.Time
}

// NewVerifier monta o verificador e valida a configuração, buscando
// discovery + JWKS uma vez (fail fast). Com Enabled e Issuer vazio, recusa.
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	if !cfg.Enabled {
		return nil, errors.New("oidc disabled")
	}
	if cfg.Issuer == "" {
		return nil, errors.New("oidc issuer is required")
	}
	v := &Verifier{
		client:   &http.Client{Timeout: 10 * time.Second},
		issuer:   strings.TrimRight(cfg.Issuer, "/"),
		audience: cfg.Audience,
		keys:     map[string]*rsa.PublicKey{},
	}
	if err := v.refreshKeys(ctx); err != nil {
		return nil, fmt.Errorf("load jwks: %w", err)
	}
	return v, nil
}

// Validate verifica assinatura (RS256), expiração, issuer, audience (quando
// configurada) e extrai provider_id + sub. Nunca aceita token sem validar
// a assinatura contra o JWKS.
func (v *Verifier) Validate(ctx context.Context, rawToken string) (Principal, error) {
	if strings.TrimSpace(rawToken) == "" {
		return Principal{}, ErrInvalidToken
	}

	keyFunc := func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			return nil, fmt.Errorf("%w: unexpected signing method", ErrInvalidToken)
		}
		kid, _ := token.Header["kid"].(string)
		key, err := v.keyFor(ctx, kid)
		if err != nil {
			return nil, err
		}
		return key, nil
	}

	opts := []jwt.ParserOption{
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30 * time.Second),
		jwt.WithIssuer(v.issuer),
	}
	if v.audience != "" {
		opts = append(opts, jwt.WithAudience(v.audience))
	}

	var claims oidcClaims
	token, err := jwt.ParseWithClaims(rawToken, &claims, keyFunc, opts...)
	if err != nil || !token.Valid {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if claims.ProviderID == "" {
		return Principal{}, ErrUnknownProvider
	}
	return Principal{ProviderID: claims.ProviderID, Subject: claims.Subject}, nil
}

func (v *Verifier) keyFor(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if key, ok := v.keys[kid]; ok && time.Now().Before(v.expires) && kid != "" {
		return key, nil
	}
	if err := v.refreshKeysLocked(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	key, ok := v.keys[kid]
	if !ok {
		return nil, fmt.Errorf("%w: unknown key", ErrInvalidToken)
	}
	return key, nil
}

func (v *Verifier) refreshKeys(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.refreshKeysLocked(ctx)
}

func (v *Verifier) refreshKeysLocked(ctx context.Context) error {
	var discovery discoveryDocument
	if err := getJSON(ctx, v.client, v.issuer+"/.well-known/openid-configuration", &discovery); err != nil {
		return err
	}
	if discovery.JWKSURI == "" {
		return errors.New("discovery without jwks_uri")
	}
	var doc jwksDocument
	if err := getJSON(ctx, v.client, discovery.JWKSURI, &doc); err != nil {
		return err
	}

	keys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, k := range doc.Keys {
		if k.Kty != "RSA" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		pub, err := rsaPublicKey(k.N, k.E)
		if err != nil {
			continue
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return errors.New("no usable RSA keys")
	}
	v.keys = keys
	v.expires = time.Now().Add(jwksCacheTTL)
	return nil
}

func getJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("metadata status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func rsaPublicKey(nB64, eB64 string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, err
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}
	if e == 0 {
		return nil, errors.New("empty exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}, nil
}
