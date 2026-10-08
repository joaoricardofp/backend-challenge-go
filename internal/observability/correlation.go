package observability

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
)

// RequestIDHeader é o único header de correlação aceito e ecoado. Não é
// distributed tracing: apenas um ID operacional por request/execução,
// presente na resposta e nos logs.
const RequestIDHeader = "X-Request-ID"

// requestIDKey carrega o ID no contexto da requisição/execução.
type requestIDKey struct{}

// NewRequestID gera um UUID v4 (RFC 4122) pseudo-aleatório seguro de 36 caracteres.
// Fonte: crypto/rand; em falha catastrófica do gerador, retorna string vazia e o
// chamador trata como ausente.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ValidRequestID aceita IDs curtos e seguros para ecoar em header e logs:
// 1–128 chars de [A-Za-z0-9_-]. Qualquer outra coisa (vazio, longo demais,
// com espaços, quebras de linha ou escapes) é rejeitada e substituída por
// um ID gerado — nunca ecoamos bytes arbitrários do cliente.
func ValidRequestID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' ||
			c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' ||
			c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

// WithRequestID anexa o ID ao contexto.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom recupera o ID do contexto, se presente e não vazio.
func RequestIDFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(requestIDKey{}).(string)
	if !ok || id == "" {
		return "", false
	}
	return id, true
}

// EnsureRequestID resolve o ID para a requisição: usa o do contexto (posto
// pelo middleware), senão o header validado, senão gera um novo. Também
// garante o header na resposta, sem sobrescrever valor já definido.
func EnsureRequestID(w http.ResponseWriter, r *http.Request) (string, *http.Request) {
	if id, ok := RequestIDFrom(r.Context()); ok {
		if w.Header().Get(RequestIDHeader) == "" {
			w.Header().Set(RequestIDHeader, id)
		}
		return id, r
	}
	id := r.Header.Get(RequestIDHeader)
	if !ValidRequestID(id) {
		id = NewRequestID()
	}
	if w.Header().Get(RequestIDHeader) == "" {
		w.Header().Set(RequestIDHeader, id)
	}
	return id, r.WithContext(WithRequestID(r.Context(), id))
}

// RequestIDMiddleware garante correlation ID em todas as rotas: aceita o
// header do cliente após validação (ou gera), injeta no contexto e ecoa na
// resposta. Handlers toleram ausência (usam EnsureRequestID), então chamadas
// diretas sem middleware continuam funcionando.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !ValidRequestID(id) {
			id = NewRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
	})
}
