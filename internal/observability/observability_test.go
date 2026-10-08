package observability_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
)

func TestRequestIDValidation(t *testing.T) {
	valid := []string{"abc", "A1-_z", strings.Repeat("a", 128), "sqs-msg-123"}
	for _, id := range valid {
		if !observability.ValidRequestID(id) {
			t.Errorf("ValidRequestID(%q) = false, want true", id)
		}
	}
	invalid := []string{"", strings.Repeat("a", 129), "with space", "line\nbreak", "semi;colon", "quote\"", "back\\slash", "uniçode"}
	for _, id := range invalid {
		if observability.ValidRequestID(id) {
			t.Errorf("ValidRequestID(%q) = true, want false", id)
		}
	}
}

func TestNewRequestID(t *testing.T) {
	a, b := observability.NewRequestID(), observability.NewRequestID()
	if len(a) != 36 || len(b) != 36 || a == b {
		t.Errorf("ids = %q, %q, want two distinct 36-char UUIDs", a, b)
	}
	if !observability.ValidRequestID(a) || !observability.ValidRequestID(b) {
		t.Errorf("generated ids fail own validation: %q, %q", a, b)
	}
	// Valida padrão UUID 8-4-4-4-12
	parts := strings.Split(a, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
		t.Errorf("id %q is not valid UUID format", a)
	}
}

func TestRequestIDMiddleware(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := observability.RequestIDFrom(r.Context())
		if !ok || id == "" {
			t.Error("no request ID in context")
		}
		w.WriteHeader(http.StatusOK)
	})

	// Header válido é propagado e ecoado.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "client-123")
	rec := httptest.NewRecorder()
	observability.RequestIDMiddleware(next).ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-ID") != "client-123" {
		t.Errorf("echo = %q, want client-123", rec.Header().Get("X-Request-ID"))
	}

	// Ausente: gerado e ecoado.
	rec = httptest.NewRecorder()
	observability.RequestIDMiddleware(next).ServeHTTP(rec, httptest.NewRequest("x", "/", nil))
	got := rec.Header().Get("X-Request-ID")
	if !observability.ValidRequestID(got) || got == "" {
		t.Errorf("generated echo = %q, want valid generated ID", got)
	}

	// Inválido: substituído, nunca ecoado cru.
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "bad\nid")
	rec = httptest.NewRecorder()
	observability.RequestIDMiddleware(next).ServeHTTP(rec, req)
	if echo := rec.Header().Get("X-Request-ID"); echo == "bad\nid" || !observability.ValidRequestID(echo) {
		t.Errorf("echo = %q, want generated replacement", echo)
	}
}

func TestEnsureRequestID(t *testing.T) {
	// Sem middleware: gera e injeta no contexto + header.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	id, req2 := observability.EnsureRequestID(rec, req)
	if id == "" {
		t.Fatal("empty ID")
	}
	if got, ok := observability.RequestIDFrom(req2.Context()); !ok || got != id {
		t.Error("context not updated")
	}
	if rec.Header().Get("X-Request-ID") != id {
		t.Error("response header not set")
	}

	// Com contexto já populado: preserva, sem sobrescrever header existente.
	rec = httptest.NewRecorder()
	rec.Header().Set("X-Request-ID", "kept")
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	req = req.WithContext(observability.WithRequestID(req.Context(), "ctx-id"))
	id, _ = observability.EnsureRequestID(rec, req)
	if id != "ctx-id" || rec.Header().Get("X-Request-ID") != "kept" {
		t.Errorf("id = %q, header = %q, want preserved", id, rec.Header().Get("X-Request-ID"))
	}
}

func TestMetricsExposition(t *testing.T) {
	m := observability.NewMetrics()
	m.IncWagerRequest("200")
	m.IncWagerRequest("200")
	m.IncWagerRequest("422")
	m.IncWagerTransaction("BET", "PROCESSED")
	m.IncWagerRejection("INSUFFICIENT_FUNDS")
	m.IncWagerFailure("OVERFLOW")
	m.IncSQSReceived("wager")
	m.IncSQSProcessed("wager", "acked")
	m.IncSQSFailed("wager", "process")
	m.IncOutboxPublished("events")
	m.IncOutboxPublishFailure("events")
	m.IncOutboxDeadLettered()

	if got := m.Value("wager_requests_total", map[string]string{"status": "200"}); got != 2 {
		t.Errorf("requests 200 = %d, want 2", got)
	}
	if got := m.Value("wager_requests_total", map[string]string{"status": "999"}); got != 0 {
		t.Errorf("missing counter = %d, want 0", got)
	}

	var b strings.Builder
	m.WritePrometheus(&b)
	out := b.String()
	for _, want := range []string{
		"# TYPE wager_requests_total counter",
		`wager_requests_total{status="200"} 2`,
		`wager_requests_total{status="422"} 1`,
		`wager_transactions_total{kind="BET",status="PROCESSED"} 1`,
		`wager_transaction_rejections_total{failure_code="INSUFFICIENT_FUNDS"} 1`,
		`wager_transaction_failures_total{failure_code="OVERFLOW"} 1`,
		`sqs_messages_received_total{queue="wager"} 1`,
		`sqs_messages_processed_total{outcome="acked",queue="wager"} 1`,
		`sqs_messages_failed_total{queue="wager",stage="process"} 1`,
		`outbox_events_published_total{queue="events"} 1`,
		`outbox_publish_failures_total{queue="events"} 1`,
		`outbox_events_dead_lettered_total 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q\n%s", want, out)
		}
	}
	// Nenhum ID de negócio como label (baixa cardinalidade por construção).
	for _, forbidden := range []string{"transactionId=", "externalTransactionId=", "idempotencyKey=", "eventId="} {
		if strings.Contains(out, forbidden) {
			t.Errorf("exposition leaks high-cardinality label %q", forbidden)
		}
	}
}

func TestMetricsNilSafe(t *testing.T) {
	var m *observability.Metrics
	m.IncWagerRequest("200") // sem panic
	m.IncSQSProcessed("wager", "acked")
	if got := m.Value("x", nil); got != 0 {
		t.Errorf("nil Value = %d, want 0", got)
	}
	m.WritePrometheus(io.Discard) // sem panic
}

func TestMetricsMiddleware(t *testing.T) {
	m := observability.NewMetrics()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	rec := httptest.NewRecorder()
	observability.MetricsMiddleware(m, next).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := m.Value("wager_requests_total", map[string]string{"status": "418"}); got != 1 {
		t.Errorf("requests 418 = %d, want 1", got)
	}
}

func TestHealthLifecycle(t *testing.T) {
	ok := observability.Check{Name: "db", Check: func(context.Context) error { return nil }}
	bad := observability.Check{Name: "db", Check: func(context.Context) error { return errors.New("down") }}

	h := observability.NewHealth([]observability.Check{ok}, time.Millisecond)
	if ready, _ := h.Ready(); ready {
		t.Error("ready before start, want false (startup incompleto nunca anuncia pronto)")
	}

	// startup: checks ok → started → ready.
	if err := h.CheckOnce(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}
	h.MarkStarted()
	if ready, details := h.Ready(); !ready || details["db"] != "ok" {
		t.Errorf("ready = %v, %v, want true/ok", ready, details)
	}

	// Dependência cai: volta a não pronto.
	h2 := observability.NewHealth([]observability.Check{bad}, time.Millisecond)
	_ = h2.CheckOnce(context.Background())
	h2.MarkStarted()
	if ready, details := h2.Ready(); ready || details["db"] != "down" {
		t.Errorf("ready = %v, %v, want false/down", ready, details)
	}

	// Shutdown: para de anunciar mesmo com checks ok.
	h.MarkStopping()
	if ready, _ := h.Ready(); ready {
		t.Error("ready during shutdown, want false")
	}
}

func TestHealthContextCanceled(t *testing.T) {
	block := observability.Check{Name: "slow", Check: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	h := observability.NewHealth([]observability.Check{block}, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.CheckOnce(ctx); err == nil {
		t.Error("expected context error")
	}
	h.MarkStarted()
	if ready, _ := h.Ready(); ready {
		t.Error("ready with failed check, want false")
	}
}

func TestHealthRunStopsOnCancel(t *testing.T) {
	calls := 0
	h := observability.NewHealth([]observability.Check{{
		Name:  "db",
		Check: func(context.Context) error { calls++; return nil },
	}}, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := h.Run(ctx); err == nil {
		t.Error("expected context error from Run")
	}
	if calls == 0 {
		t.Error("Run never checked")
	}
}

func TestLiveAlwaysReady(t *testing.T) {
	h := observability.NewHealth(nil, time.Second)
	if !h.Live() {
		t.Error("Live = false, want true (sem dependências)")
	}
	if ready, _ := h.Ready(); ready {
		t.Error("Ready with no checks and not started, want false")
	}
}

func TestHealthHandlers(t *testing.T) {
	h := observability.NewHealth([]observability.Check{{Name: "db", Check: func(context.Context) error { return nil }}}, time.Second)

	// live: 200 normal
	rec := httptest.NewRecorder()
	observability.LiveHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("live = %d, want 200", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Errorf("live body = %q", rec.Body.String())
	}

	// live: 503 pós-shutdown
	hStopping := observability.NewHealth(nil, time.Second)
	hStopping.MarkStopping()
	rec = httptest.NewRecorder()
	observability.LiveHandler(hStopping).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("live shutdown = %d, want 503", rec.Code)
	}

	// ready: 503 antes do start, 200 após checks+start.
	rec = httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready pre-start = %d, want 503", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != `{"status":"not_ready"}` {
		t.Errorf("ready body = %q", rec.Body.String())
	}
	_ = h.CheckOnce(context.Background())
	h.MarkStarted()
	rec = httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready = %d, want 200", rec.Code)
	}

	// metrics: 200 com contadores; nil-safe.
	m := observability.NewMetrics()
	m.IncWagerRequest("200")
	rec = httptest.NewRecorder()
	observability.MetricsHandler(m).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q", ct)
	}
	if !strings.Contains(rec.Body.String(), `wager_requests_total{status="200"} 1`) {
		t.Errorf("body missing counter:\n%s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	observability.MetricsHandler(nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("nil metrics = %d, want 200", rec.Code)
	}
}

func TestNewLoggerJSON(t *testing.T) {
	var b strings.Builder
	logger := observability.NewLogger("test-comp", &b)
	ctx := observability.WithRequestID(context.Background(), "req-1")
	logger.InfoContext(
		ctx,
		"processed",
		append([]any{"transaction_id", "t1"}, observability.AttrsFromContext(ctx)...)...,
	)
	out := b.String()
	for _, want := range []string{`"component":"test-comp"`, `"msg":"processed"`, `"transaction_id":"t1"`, `"request_id":"req-1"`, `"level":"INFO"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
	if observability.AttrsFromContext(context.Background()) != nil {
		t.Error("attrs without request ID should be nil")
	}
}

type fakePinger struct {
	err error
}

func (f *fakePinger) Ping(ctx context.Context) error {
	return f.err
}

type fakeQueueChecker struct {
	err error
}

func (f *fakeQueueChecker) CheckQueue(ctx context.Context) error {
	return f.err
}

// B3.14 - Testes obrigatórios de Liveness:
// - live retorna saudável normalmente;
// - live fica não saudável após início do shutdown.
func TestLiveness_HealthyNormally(t *testing.T) {
	h := observability.NewHealth(nil, time.Second)
	if !h.Live() {
		t.Fatal("h.Live() = false, want true during normal operation")
	}
	rec := httptest.NewRecorder()
	observability.LiveHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("live status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Errorf("live body = %q, want status:ok", rec.Body.String())
	}
}

func TestLiveness_UnhealthyAfterShutdown(t *testing.T) {
	h := observability.NewHealth(nil, time.Second)
	h.MarkStopping()
	if h.Live() {
		t.Fatal("h.Live() = true, want false after shutdown started")
	}
	rec := httptest.NewRecorder()
	observability.LiveHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("live status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"not_ready"`) {
		t.Errorf("live body = %q, want status:not_ready", rec.Body.String())
	}
}

// B3.14 - Testes obrigatórios de Readiness:
// - PostgreSQL saudável -> ready;
// - PostgreSQL indisponível -> not ready;
// - SQS habilitado e saudável -> ready;
// - SQS habilitado e indisponível -> not ready;
// - SQS desabilitado -> não deve bloquear readiness;
// - shutdown -> not ready.
func TestReadiness_PostgresHealthy_Ready(t *testing.T) {
	pg := &fakePinger{err: nil}
	h := observability.NewHealth([]observability.Check{observability.NewPingCheck("postgres", pg)}, time.Second)
	if err := h.CheckOnce(context.Background()); err != nil {
		t.Fatalf("check failed: %v", err)
	}
	h.MarkStarted()

	ready, details := h.Ready()
	if !ready || details["postgres"] != "ok" {
		t.Fatalf("ready = %v, details = %v, want true / postgres: ok", ready, details)
	}

	rec := httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("ready status = %d, want 200", rec.Code)
	}
}

func TestReadiness_PostgresUnavailable_NotReady(t *testing.T) {
	pg := &fakePinger{err: errors.New("connection refused")}
	h := observability.NewHealth([]observability.Check{observability.NewPingCheck("postgres", pg)}, time.Second)
	_ = h.CheckOnce(context.Background())
	h.MarkStarted()

	ready, details := h.Ready()
	if ready || details["postgres"] != "connection refused" {
		t.Fatalf("ready = %v, details = %v, want false / postgres: connection refused", ready, details)
	}

	rec := httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("ready status = %d, want 503", rec.Code)
	}
}

func TestReadiness_SQSEnabledAndHealthy_Ready(t *testing.T) {
	pg := &fakePinger{err: nil}
	qWager := &fakeQueueChecker{err: nil}
	qEvents := &fakeQueueChecker{err: nil}
	h := observability.NewHealth([]observability.Check{
		observability.NewPingCheck("postgres", pg),
		observability.NewQueueCheck("sqs-wager", qWager),
		observability.NewQueueCheck("sqs-events", qEvents),
	}, time.Second)
	if err := h.CheckOnce(context.Background()); err != nil {
		t.Fatalf("check failed: %v", err)
	}
	h.MarkStarted()

	ready, details := h.Ready()
	if !ready || details["postgres"] != "ok" || details["sqs-wager"] != "ok" || details["sqs-events"] != "ok" {
		t.Fatalf("ready = %v, details = %v, want true with all ok", ready, details)
	}

	rec := httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("ready status = %d, want 200", rec.Code)
	}
}

func TestReadiness_SQSEnabledAndUnavailable_NotReady(t *testing.T) {
	pg := &fakePinger{err: nil}
	qWager := &fakeQueueChecker{err: errors.New("sqs queue not accessible")}
	h := observability.NewHealth([]observability.Check{
		observability.NewPingCheck("postgres", pg),
		observability.NewQueueCheck("sqs-wager", qWager),
	}, time.Second)
	_ = h.CheckOnce(context.Background())
	h.MarkStarted()

	ready, details := h.Ready()
	if ready || details["sqs-wager"] != "sqs queue not accessible" {
		t.Fatalf("ready = %v, details = %v, want false with sqs error", ready, details)
	}

	rec := httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("ready status = %d, want 503", rec.Code)
	}
}

func TestReadiness_SQSDisabled_DoesNotBlockReadiness(t *testing.T) {
	// Quando SQS está desabilitado, nenhum check SQS é adicionado
	pg := &fakePinger{err: nil}
	h := observability.NewHealth([]observability.Check{
		observability.NewPingCheck("postgres", pg),
	}, time.Second)
	if err := h.CheckOnce(context.Background()); err != nil {
		t.Fatalf("check failed: %v", err)
	}
	h.MarkStarted()

	ready, _ := h.Ready()
	if !ready {
		t.Fatal("ready = false, want true when SQS is disabled and postgres is healthy")
	}

	rec := httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("ready status = %d, want 200", rec.Code)
	}
}

func TestReadiness_Shutdown_NotReady(t *testing.T) {
	pg := &fakePinger{err: nil}
	h := observability.NewHealth([]observability.Check{
		observability.NewPingCheck("postgres", pg),
	}, time.Second)
	_ = h.CheckOnce(context.Background())
	h.MarkStarted()
	h.MarkStopping() // início do shutdown

	ready, _ := h.Ready()
	if ready {
		t.Fatal("ready = true, want false during shutdown")
	}

	rec := httptest.NewRecorder()
	observability.ReadyHandler(h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("ready status = %d, want 503", rec.Code)
	}
}

// B3.14 - Testes obrigatórios de Request ID:
// - request sem ID recebe um ID;
// - request com ID válido preserva o ID;
// - response devolve o mesmo ID;
// - ID chega ao contexto/logger conforme a implementação.
func TestRequestIDScenarios(t *testing.T) {
	var capturedID string
	var logBuf strings.Builder
	logger := observability.NewLogger("test", &logBuf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := observability.RequestIDFrom(r.Context())
		if !ok || id == "" {
			t.Error("request ID missing from context")
		}
		capturedID = id
		logger.InfoContext(r.Context(), "request handled")
		w.WriteHeader(http.StatusOK)
	})

	// 1. Sem ID -> gera ID (UUID), devolve no response header, propaga ao contexto e log
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	observability.RequestIDMiddleware(handler).ServeHTTP(rec, req)

	respID := rec.Header().Get("X-Request-ID")
	if respID == "" {
		t.Fatal("response header missing X-Request-ID")
	}
	if len(respID) != 36 {
		t.Errorf("generated ID length = %d, want 36 (UUID v4)", len(respID))
	}
	if capturedID != respID {
		t.Errorf("captured ID in context %q != response ID %q", capturedID, respID)
	}
	if !strings.Contains(logBuf.String(), `"request_id":"`+respID+`"`) {
		t.Errorf("logger missing request_id %q: %s", respID, logBuf.String())
	}

	// 2. Com ID válido recebido pelo cliente -> preserva o ID
	logBuf.Reset()
	clientUUID := "12345678-1234-4234-8234-123456789abc"
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Request-ID", clientUUID)
	observability.RequestIDMiddleware(handler).ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got != clientUUID {
		t.Errorf("echo response ID = %q, want %q", got, clientUUID)
	}
	if capturedID != clientUUID {
		t.Errorf("captured ID = %q, want %q", capturedID, clientUUID)
	}
	if !strings.Contains(logBuf.String(), `"request_id":"`+clientUUID+`"`) {
		t.Errorf("logger missing client UUID: %s", logBuf.String())
	}
}

// B3.14 - Testes obrigatórios de Logging:
// Testar apenas o comportamento essencial, evitando testes frágeis baseados em formatação textual excessivamente específica.
func TestStructuredLogging_EssentialBehavior(t *testing.T) {
	var b strings.Builder
	logger := observability.NewLogger("audit", &b)
	ctx := observability.WithRequestID(context.Background(), "test-corr-id")

	logger.InfoContext(ctx, "financial operation completed",
		slog.String("provider_id", "prov-1"),
		slog.String("status", "COMPLETED"),
	)
	out := b.String()

	// Deve conter campos essenciais estruturados: timestamp ("time"), nível ("level"), msg, component, request_id
	for _, essential := range []string{`"level":"INFO"`, `"msg":"financial operation completed"`, `"component":"audit"`, `"request_id":"test-corr-id"`, `"provider_id":"prov-1"`} {
		if !strings.Contains(out, essential) {
			t.Errorf("log missing essential field %q:\n%s", essential, out)
		}
	}

	// NÃO deve conter Authorization, tokens ou senhas
	for _, forbidden := range []string{"Bearer ", "Authorization", "secret", "password"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("log unexpectedly leaked secret %q:\n%s", forbidden, out)
		}
	}
}

// B3.14 - Testes obrigatórios de Métricas:
// Testar que as métricas principais são registradas/expostas e que não existem labels de alta cardinalidade.
func TestMetrics_DetailedAndLowCardinality(t *testing.T) {
	m := observability.NewMetrics()

	// HTTP requests e duração
	m.IncWagerRequest("200")
	m.IncWagerRequest("400")
	m.RecordRequestDuration(25 * time.Millisecond)

	// Status / resultados de wagering
	m.IncWagerTransaction("BET", "COMPLETED")
	m.IncWagerRejection("INSUFFICIENT_FUNDS")
	m.IncWagerFailure("OVERFLOW")

	// SQS processamento
	m.IncSQSReceived("wager")
	m.IncSQSProcessed("wager", "acked")
	m.IncSQSFailed("wager", "reserve")

	// Outbox publisher
	m.IncOutboxPublished("events")
	m.IncOutboxPublishFailure("events")
	m.IncOutboxDeadLettered()

	// Health status gauge
	m.SetHealthStatus("live", true)
	m.SetHealthStatus("ready", true)
	m.SetHealthStatus("postgres", true)

	var b strings.Builder
	m.WritePrometheus(&b)
	out := b.String()

	for _, want := range []string{
		`wager_requests_total{status="200"} 1`,
		`wager_requests_total{status="400"} 1`,
		`# TYPE http_request_duration_seconds histogram`,
		`http_request_duration_seconds_count 1`,
		`wager_transactions_total{kind="BET",status="COMPLETED"} 1`,
		`wager_transaction_rejections_total{failure_code="INSUFFICIENT_FUNDS"} 1`,
		`wager_transaction_failures_total{failure_code="OVERFLOW"} 1`,
		`sqs_messages_received_total{queue="wager"} 1`,
		`sqs_messages_processed_total{outcome="acked",queue="wager"} 1`,
		`sqs_messages_failed_total{queue="wager",stage="reserve"} 1`,
		`outbox_events_published_total{queue="events"} 1`,
		`outbox_publish_failures_total{queue="events"} 1`,
		`outbox_events_dead_lettered_total 1`,
		`# TYPE health_status gauge`,
		`health_status{check="live"} 1`,
		`health_status{check="ready"} 1`,
		`health_status{check="postgres"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing metric %q:\n%s", want, out)
		}
	}

	// Não usar como labels: playerId, walletId, externalTransactionId, idempotencyKey, messageId, UUIDs, payloads, valores monetários
	forbiddenLabels := []string{
		"playerId=", "walletId=", "externalTransactionId=", "idempotencyKey=",
		"messageId=", "payload=", "amount=", "balance=",
	}
	for _, forbidden := range forbiddenLabels {
		if strings.Contains(out, forbidden) {
			t.Errorf("metrics exposition contains forbidden high-cardinality label %q:\n%s", forbidden, out)
		}
	}
}
