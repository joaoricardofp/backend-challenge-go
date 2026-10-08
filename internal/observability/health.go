package observability

import (
	"context"
	"sync"
	"time"
)

// checkTimeout limita cada verificação de dependência: readiness nunca fica
// presa numa dependência lenta. Chamadas externas por request são evitadas —
// o estado é atualizado pelo loop Run/checks do lifecycle e servido do cache.
const checkTimeout = 3 * time.Second

// Pinger é uma interface pequena para verificação de conectividade (ex.: *pgxpool.Pool).
type Pinger interface {
	Ping(ctx context.Context) error
}

// QueueChecker é uma interface pequena para verificação de filas (ex.: *sqs.SQSAdapter).
type QueueChecker interface {
	CheckQueue(ctx context.Context) error
}

// Check é uma verificação nomeada de dependência (postgres, filas SQS).
// Função pura injetável: produção usa Ping/GetQueueAttributes reais, testes
// usam funcs sintéticas. OIDC não tem check: é validado no startup
// (construção do verifier falha o boot) e nunca chamado por request.
type Check struct {
	Name  string
	Check func(ctx context.Context) error
}

// NewPingCheck monta um Check nomeado a partir de um Pinger.
func NewPingCheck(name string, p Pinger) Check {
	return Check{
		Name: name,
		Check: func(ctx context.Context) error {
			return p.Ping(ctx)
		},
	}
}

// NewQueueCheck monta um Check nomeado a partir de um QueueChecker.
func NewQueueCheck(name string, q QueueChecker) Check {
	return Check{
		Name: name,
		Check: func(ctx context.Context) error {
			return q.CheckQueue(ctx)
		},
	}
}

// Health guarda o estado de readiness com cache atualizado pelo lifecycle:
// startup (dependências ok) → ready=true; shutdown iniciado → ready=false.
// Live é saudável (true) durante o funcionamento normal e não-saudável (false)
// durante o shutdown.
type Health struct {
	mu       sync.Mutex
	checks   []Check
	interval time.Duration
	started  bool
	stopping bool
	ready    bool
	details  map[string]string
	metrics  *Metrics
}

// NewHealth monta o monitor desligado (not ready) até MarkStarted após
// checks iniciais. Intervalo <= 0 usa 10s.
func NewHealth(checks []Check, interval time.Duration) *Health {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &Health{checks: checks, interval: interval, details: map[string]string{}}
}

// CheckOnce executa todas as verificações (cada uma com timeout próprio) e
// guarda o resultado. Retorna nil somente se todas passaram. Chamadores:
// loop Run, OnStart inicial e testes.
func (h *Health) CheckOnce(ctx context.Context) error {
	type result struct {
		name string
		err  error
	}
	results := make([]result, 0, len(h.checks))
	var firstErr error
	for _, c := range h.checks {
		checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
		err := c.Check(checkCtx)
		cancel()
		results = append(results, result{name: c.Name, err: err})
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if ctx.Err() != nil && firstErr == nil {
			firstErr = ctx.Err()
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	details := make(map[string]string, len(results))
	ready := true
	for _, r := range results {
		if r.err != nil {
			ready = false
			details[r.name] = r.err.Error()
		} else {
			details[r.name] = "ok"
		}
	}
	h.ready = ready
	h.details = details
	h.syncMetricsLocked()
	return firstErr
}

// SetMetrics vincula um registro de métricas para atualização automática
// do estado de saúde (live, ready e checks individuais).
func (h *Health) SetMetrics(m *Metrics) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.metrics = m
	h.syncMetricsLocked()
}

func (h *Health) syncMetricsLocked() {
	if h.metrics == nil {
		return
	}
	h.metrics.SetHealthStatus("live", !h.stopping)
	h.metrics.SetHealthStatus("ready", h.started && !h.stopping && h.ready)
	for name, res := range h.details {
		h.metrics.SetHealthStatus(name, res == "ok")
	}
}

// Run atualiza o estado periodicamente até o contexto cancelar. Sem
// goroutines internas: o lifecycle Fx detém a única goroutine.
func (h *Health) Run(ctx context.Context) error {
	for {
		_ = h.CheckOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(h.interval):
		}
	}
}

// MarkStarted libera o anúncio de readiness (chamado no OnStart após checks
// iniciais passarem). Antes disso, Ready é sempre false: startup incompleto
// nunca anuncia pronto.
func (h *Health) MarkStarted() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.started = true
	h.syncMetricsLocked()
}

// MarkStopping desliga o anúncio de liveness e readiness (primeiro passo do
// shutdown: a aplicação para de receber tráfego antes de encerrar componentes).
func (h *Health) MarkStopping() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopping = true
	h.syncMetricsLocked()
}

// Live responde se o processo está vivo: saudável (true) em funcionamento
// normal, não-saudável (false) após o início do shutdown (MarkStopping).
// Não executa chamadas externas nem consultas ao banco a cada request.
func (h *Health) Live() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.stopping
}

// Ready responde se a aplicação pode receber tráfego: lifecycle iniciado,
// shutdown ainda não iniciado e última verificação com todas as dependências
// ok. Retorna cópia dos detalhes por check. Não faz chamadas de rede por request.
func (h *Health) Ready() (bool, map[string]string) {
	if h == nil {
		return false, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	details := make(map[string]string, len(h.details))
	for k, v := range h.details {
		details[k] = v
	}
	return h.started && !h.stopping && h.ready, details
}
