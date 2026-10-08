package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/joaoricardofp/backend-challenge-go/internal/auth"
)

// RequireProviderAuth é o middleware de autenticação/autorização da fronteira
// HTTP: exige Bearer OIDC válido, extrai a identidade e vincula o provider
// do corpo ao provider autenticado. Não contém regra financeira; apenas
// decide se a requisição pode alcançar o use case.
func RequireProviderAuth(verifier *auth.Verifier, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "MISSING_CREDENTIALS", "authorization required")
			return
		}

		principal, err := verifier.Validate(r.Context(), token)
		if err != nil {
			if errors.Is(err, auth.ErrUnknownProvider) {
				writeError(w, http.StatusForbidden, "PROVIDER_UNKNOWN", "unknown provider")
				return
			}
			writeError(w, http.StatusUnauthorized, "INVALID_TOKEN", "invalid token")
			return
		}

		// Sonda mínima do corpo para vincular o provider declarado ao
		// autenticado. O corpo segue intacto para o handler; se ilegível,
		// o handler responde 400 pelo contrato já congelado.
		if body, readErr := io.ReadAll(r.Body); readErr == nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
			var probe struct {
				ProviderID string `json:"providerId"`
			}
			if json.Unmarshal(body, &probe) == nil &&
				probe.ProviderID != "" &&
				probe.ProviderID != principal.ProviderID {
				writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "provider mismatch")
				return
			}
		}

		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
	})
}

// RequireInternalServiceAuth é o middleware para operações internas restritas
// (ex.: abertura de carteira). Exige Bearer OIDC válido e verifica se o
// provider_id autenticado é o UUID reservado para serviços internos.
// Não enfraquece o middleware OIDC existente; apenas adiciona uma verificação
// de autorização específica para o uso interno.
const InternalProviderID = "00000000-0000-0000-0000-000000000001"

func RequireInternalServiceAuth(verifier *auth.Verifier, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "MISSING_CREDENTIALS", "authorization required")
			return
		}

		principal, err := verifier.Validate(r.Context(), token)
		if err != nil {
			if errors.Is(err, auth.ErrUnknownProvider) {
				writeError(w, http.StatusForbidden, "PROVIDER_UNKNOWN", "unknown provider")
				return
			}
			writeError(w, http.StatusUnauthorized, "INVALID_TOKEN", "invalid token")
			return
		}

		// Apenas o provider_id reservado pode acessar endpoints internos.
		// Isso evita que providers externos criem carteiras arbitrariamente.
		if principal.ProviderID != InternalProviderID {
			writeError(w, http.StatusForbidden, "INTERNAL_SERVICE_REQUIRED", "internal service required")
			return
		}

		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
	})
}

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	parts := strings.SplitN(h, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", false
	}
	return token, true
}
