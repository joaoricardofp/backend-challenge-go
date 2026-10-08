package observability

import (
	"net/http"
	"strconv"
	"time"
)

// statusRecorder captura o status efetivamente escrito na resposta.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if !r.wrote {
		r.status = status
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(status)
}

// MetricsMiddleware conta respostas por status code na rota envolvida
// (wager_requests_total) e registra histograma de duração de requests.
// Aplica-se ao endpoint de wagering: health/metrics ficam de fora das métricas
// de negócio. Respostas nunca escritas (contexto cancelado antes do primeiro Write)
// não são contadas.
func MetricsMiddleware(m *Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if rec.wrote {
			m.IncWagerRequest(strconv.Itoa(rec.status))
			m.RecordRequestDuration(time.Since(start))
		}
	})
}
