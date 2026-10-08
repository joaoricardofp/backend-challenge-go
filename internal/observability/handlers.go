package observability

import (
	"encoding/json"
	"net/http"
)

// statusBody é o corpo estável e mínimo dos health endpoints.
type statusBody struct {
	Status string `json:"status"`
}

func writeStatus(w http.ResponseWriter, status int, ready bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := "not_ready"
	if ready {
		body = "ok"
	}
	_ = json.NewEncoder(w).Encode(statusBody{Status: body})
}

// LiveHandler serve GET /health/live: 200 enquanto a aplicação está em funcionamento,
// 503 após início do shutdown. Não executa consulta ao PostgreSQL nem chamadas
// de rede a cada request. Resposta simples e determinística em JSON.
func LiveHandler(h *Health) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if h != nil && !h.Live() {
			writeStatus(w, http.StatusServiceUnavailable, false)
			return
		}
		writeStatus(w, http.StatusOK, true)
	})
}

// ReadyHandler serve GET /health/ready: 200 pronto, 503 não pronto
// (startup incompleto, shutdown iniciado ou dependência falha).
// Sem chamadas externas redundantes por request.
func ReadyHandler(h *Health) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if h == nil {
			writeStatus(w, http.StatusServiceUnavailable, false)
			return
		}
		ready, _ := h.Ready()
		if !ready {
			writeStatus(w, http.StatusServiceUnavailable, false)
			return
		}
		writeStatus(w, http.StatusOK, true)
	})
}

// MetricsHandler serve GET /metrics em formato texto Prometheus.
// Somente leitura, sem OIDC.
func MetricsHandler(m *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		if m != nil {
			m.WritePrometheus(w)
		}
	})
}
