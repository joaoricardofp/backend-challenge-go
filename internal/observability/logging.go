package observability

import (
	"context"
	"io"
	"log/slog"
)

type contextHandler struct {
	slog.Handler
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if id, ok := RequestIDFrom(ctx); ok && id != "" {
			hasReqID := false
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "request_id" {
					hasReqID = true
					return false
				}
				return true
			})
			if !hasReqID {
				r.AddAttrs(slog.String("request_id", id))
			}
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}

// NewLogger monta um logger JSON estruturado com o componente fixo e injeção
// automática de request/correlation ID do contexto. JSON em uma linha por
// evento (timestamp, level, msg + attrs); sem dependências externas.
// O destino é injetável para testes (buffer); produção usa os.Stdout.
func NewLogger(component string, w io.Writer) *slog.Logger {
	h := &contextHandler{Handler: slog.NewJSONHandler(w, nil)}
	return slog.New(h).With("component", component)
}

// AttrsFromContext extrai atributos operacionais do contexto para os logs:
// apenas o request/correlation ID. NUNCA tokens, Authorization, secrets ou
// payloads financeiros — chamadores adicionam IDs de negócio (provider,
// transação, evento) explicitamente, sem valores monetários.
func AttrsFromContext(ctx context.Context) []any {
	if id, ok := RequestIDFrom(ctx); ok {
		return []any{"request_id", id}
	}
	return nil
}
