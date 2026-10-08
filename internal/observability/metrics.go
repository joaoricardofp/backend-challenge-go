package observability

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Métricas operacionais mínimas (B3.14) de baixa cardinalidade por construção:
// os únicos labels possíveis estão fixados nos métodos abaixo (status HTTP,
// kind/status/code de conjuntos fechados do domínio; queue de {wager, events, dlq};
// check de {live, ready, postgres, sqs-*}). IDs de negócio (transactionId,
// externalTransactionId, idempotencyKey, playerId, walletId) NUNCA são labels.
// Métodos são nil-safe: Metrics nil descarta operações com segurança.
type Metrics struct {
	mu              sync.Mutex
	counters        map[string]*metricCounter
	gauges          map[string]*metricGauge
	durationCount   uint64
	durationSum     float64
	durationBuckets map[string]uint64
}

type metricCounter struct {
	name   string
	labels map[string]string
	value  uint64
}

type metricGauge struct {
	name   string
	labels map[string]string
	value  float64
}

var defaultDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0}

// NewMetrics monta o registro vazio (um por processo, via Fx).
func NewMetrics() *Metrics {
	return &Metrics{
		counters:        map[string]*metricCounter{},
		gauges:          map[string]*metricGauge{},
		durationBuckets: map[string]uint64{},
	}
}

func counterKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteString("\x00")
		b.WriteString(k)
		b.WriteString("\x00")
		b.WriteString(labels[k])
	}
	return b.String()
}

func (m *Metrics) inc(name string, labels map[string]string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counters == nil {
		m.counters = map[string]*metricCounter{}
	}
	key := counterKey(name, labels)
	c, ok := m.counters[key]
	if !ok {
		cp := map[string]string{}
		for k, v := range labels {
			cp[k] = v
		}
		c = &metricCounter{name: name, labels: cp}
		m.counters[key] = c
	}
	c.value++
}

// IncWagerRequest conta respostas do endpoint de wagering por status HTTP.
func (m *Metrics) IncWagerRequest(status string) {
	m.inc("wager_requests_total", map[string]string{"status": status})
}

// IncWagerTransaction conta desfechos de transação servidos (inclui replay).
func (m *Metrics) IncWagerTransaction(kind, status string) {
	m.inc("wager_transactions_total", map[string]string{"kind": kind, "status": status})
}

// IncWagerRejection conta rejeições de negócio persistidas (REJECTED).
func (m *Metrics) IncWagerRejection(code string) {
	m.inc("wager_transaction_rejections_total", map[string]string{"failure_code": code})
}

// IncWagerFailure conta falhas permanentes (FAILED, ex. OVERFLOW).
func (m *Metrics) IncWagerFailure(code string) {
	m.inc("wager_transaction_failures_total", map[string]string{"failure_code": code})
}

// IncSQSReceived conta mensagens SQS recebidas para processamento.
func (m *Metrics) IncSQSReceived(queue string) {
	m.inc("sqs_messages_received_total", map[string]string{"queue": queue})
}

// IncSQSProcessed conta mensagens concluídas: acked, duplicate ou pending
// (aguardando referência; será redelivered).
func (m *Metrics) IncSQSProcessed(queue, outcome string) {
	m.inc("sqs_messages_processed_total", map[string]string{"queue": queue, "outcome": outcome})
}

// IncSQSFailed conta mensagens não concluídas, pela etapa da falha.
func (m *Metrics) IncSQSFailed(queue, stage string) {
	m.inc("sqs_messages_failed_total", map[string]string{"queue": queue, "stage": stage})
}

// IncOutboxPublished conta eventos marcados como publicados.
func (m *Metrics) IncOutboxPublished(queue string) {
	m.inc("outbox_events_published_total", map[string]string{"queue": queue})
}

// IncOutboxPublishFailure conta falhas de envio/marcação da outbox.
func (m *Metrics) IncOutboxPublishFailure(queue string) {
	m.inc("outbox_publish_failures_total", map[string]string{"queue": queue})
}

// IncOutboxDeadLettered conta eventos descartados para a DLQ.
func (m *Metrics) IncOutboxDeadLettered() { m.inc("outbox_events_dead_lettered_total", nil) }

// IncWalletReconciliation conta reconciliações de carteira por resultado
// ("consistent" ou "inconsistent"). Baixa cardinalidade por construção:
// nenhum ID de negócio é usado como label.
func (m *Metrics) IncWalletReconciliation(result string) {
	m.inc("wallet_reconciliations_total", map[string]string{"result": result})
}

// RecordRequestDuration registra a duração de requisições HTTP em segundos
// (histograma com buckets de baixa cardinalidade, soma e contagem).
func (m *Metrics) RecordRequestDuration(duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.durationBuckets == nil {
		m.durationBuckets = map[string]uint64{}
	}
	sec := duration.Seconds()
	m.durationCount++
	m.durationSum += sec
	for _, b := range defaultDurationBuckets {
		if sec <= b {
			k := fmt.Sprintf("%.3f", b)
			m.durationBuckets[k]++
		}
	}
}

// DurationCount retorna o total de requisições cuja duração foi observada.
func (m *Metrics) DurationCount() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.durationCount
}

// DurationSum retorna a soma das durações observadas em segundos.
func (m *Metrics) DurationSum() float64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.durationSum
}

// SetHealthStatus registra o estado de saúde de um componente como gauge
// (1 para saudável, 0 para não saudável).
func (m *Metrics) SetHealthStatus(check string, healthy bool) {
	if m == nil {
		return
	}
	val := 0.0
	if healthy {
		val = 1.0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gauges == nil {
		m.gauges = map[string]*metricGauge{}
	}
	key := counterKey("health_status", map[string]string{"check": check})
	m.gauges[key] = &metricGauge{
		name:   "health_status",
		labels: map[string]string{"check": check},
		value:  val,
	}
}

// HealthStatus retorna o valor do gauge de saúde (1 para saudável, 0 para falha, -1 se inexistente).
func (m *Metrics) HealthStatus(check string) float64 {
	if m == nil {
		return -1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := counterKey("health_status", map[string]string{"check": check})
	if g, ok := m.gauges[key]; ok {
		return g.value
	}
	return -1
}

// Value retorna o valor atual de um contador (para testes).
func (m *Metrics) Value(name string, labels map[string]string) uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[counterKey(name, labels)]; ok {
		return c.value
	}
	return 0
}

// WritePrometheus expõe os contadores, histogramas e gauges no formato de texto
// do Prometheus (linhas ordenadas para determinismo). Somente leitura; sem OIDC.
func (m *Metrics) WritePrometheus(w io.Writer) {
	if m == nil {
		return
	}
	m.mu.Lock()
	counters := make([]*metricCounter, 0, len(m.counters))
	for _, c := range m.counters {
		counters = append(counters, c)
	}
	gauges := make([]*metricGauge, 0, len(m.gauges))
	for _, g := range m.gauges {
		gauges = append(gauges, g)
	}
	dCount := m.durationCount
	dSum := m.durationSum
	dBuckets := make(map[string]uint64, len(m.durationBuckets))
	for k, v := range m.durationBuckets {
		dBuckets[k] = v
	}
	m.mu.Unlock()

	sort.Slice(counters, func(i, j int) bool {
		if counters[i].name != counters[j].name {
			return counters[i].name < counters[j].name
		}
		return formatLabels(counters[i].labels) < formatLabels(counters[j].labels)
	})
	lastType := ""
	for _, c := range counters {
		if c.name != lastType {
			fmt.Fprintf(w, "# TYPE %s counter\n", c.name)
			lastType = c.name
		}
		fmt.Fprintf(w, "%s%s %d\n", c.name, formatLabels(c.labels), c.value)
	}

	if dCount > 0 {
		fmt.Fprintf(w, "# TYPE http_request_duration_seconds histogram\n")
		for _, b := range defaultDurationBuckets {
			k := fmt.Sprintf("%.3f", b)
			fmt.Fprintf(w, "http_request_duration_seconds_bucket{le=\"%s\"} %d\n", k, dBuckets[k])
		}
		fmt.Fprintf(w, "http_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", dCount)
		fmt.Fprintf(w, "http_request_duration_seconds_sum %.6f\n", dSum)
		fmt.Fprintf(w, "http_request_duration_seconds_count %d\n", dCount)
	}

	sort.Slice(gauges, func(i, j int) bool {
		if gauges[i].name != gauges[j].name {
			return gauges[i].name < gauges[j].name
		}
		return formatLabels(gauges[i].labels) < formatLabels(gauges[j].labels)
	})
	lastGauge := ""
	for _, g := range gauges {
		if g.name != lastGauge {
			fmt.Fprintf(w, "# TYPE %s gauge\n", g.name)
			lastGauge = g.name
		}
		fmt.Fprintf(w, "%s%s %g\n", g.name, formatLabels(g.labels), g.value)
	}
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+`="`+labels[k]+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}
