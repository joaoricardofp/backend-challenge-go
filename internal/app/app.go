// Package app compõe a aplicação com Uber Fx: config → postgres pool →
// repositories → application service → auth → HTTP handler/router/server.
// Nenhuma regra de negócio vive aqui, só wiring com lifecycle explícito.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/joaoricardofp/backend-challenge-go/internal/application"
	"github.com/joaoricardofp/backend-challenge-go/internal/auth"
	"github.com/joaoricardofp/backend-challenge-go/internal/config"
	"github.com/joaoricardofp/backend-challenge-go/internal/messaging/sqs"
	"github.com/joaoricardofp/backend-challenge-go/internal/observability"
	"github.com/joaoricardofp/backend-challenge-go/internal/repository/postgres"
	wagerhttp "github.com/joaoricardofp/backend-challenge-go/internal/transport/http"
	wallethttp "github.com/joaoricardofp/backend-challenge-go/internal/transport/http"
)

const shutdownTimeout = 10 * time.Second

// Module declara o grafo completo da aplicação em submódulos por área.
var Module = fx.Module("app",
	fx.Module("config",
		fx.Provide(config.Load),
	),
	fx.Module("postgres",
		fx.Provide(newPool),
	),
	fx.Module("repositories",
		fx.Provide(
			postgres.NewWalletRepository,
			postgres.NewWagerTransactionRepository,
			postgres.NewLedgerRepository,
			postgres.NewInboxRepository,
			postgres.NewOutboxRepository,
		),
	),
	fx.Module("application",
		fx.Provide(
			application.NewWagerService,
			application.NewWalletService,
		),
	),
	fx.Module("auth",
		fx.Provide(newVerifier),
	),
	fx.Module("http",
		fx.Provide(
			newWagerHandler,
			newWalletHandler,
			newMux,
			newListener,
			newHTTPServer,
		),
	),
	fx.Module("sqs",
		fx.Provide(
			newSQSAdapter,
			newConsumer,
			newPublisher,
		),
		fx.Invoke(registerConsumerLifecycle),
		fx.Invoke(registerPublisherLifecycle),
	),
	fx.Module("observability",
		fx.Provide(
			observability.NewMetrics,
			newHealth,
		),
		fx.Invoke(configureLogging),
		fx.Invoke(registerHealthLifecycle),
	),
	// Puxa o grafo a partir do servidor: garante pool validado no startup.
	fx.Invoke(func(*http.Server) {}),
)

// newPool cria o pool (com ping de validação) e o fecha no shutdown, depois
// dos componentes que o utilizam (ordem reversa do Fx).
func newPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}

// newVerifier monta o verificador OIDC. Com OIDC desabilitado, fornece nil
// explícito e o mux serve a rota sem middleware (modo dev documentado).
func newVerifier(cfg config.Config) (*auth.Verifier, error) {
	if !cfg.OIDC.Enabled {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return auth.NewVerifier(ctx, cfg.OIDC)
}

// newWagerHandler monta o handler de wagering com métricas de negócio.
func newWagerHandler(service *application.WagerService, metrics *observability.Metrics) *wagerhttp.WagerHandler {
	return wagerhttp.NewWagerHandler(service, metrics)
}

// newWalletHandler monta o handler de wallet com métricas de negócio.
func newWalletHandler(
	walletService *application.WalletService,
	wagerService *application.WagerService,
	metrics *observability.Metrics,
) *wallethttp.WalletHandler {
	return wallethttp.NewWalletHandler(walletService, wagerService, metrics)
}

// configureLogging define o logger estruturado padrão do processo (JSON em
// stdout, uma linha por evento). Componentes usam slog.Default com attrs;
// nenhuma stack externa. Roda antes de qualquer OnStart (invokes primeiro).
func configureLogging() {
	slog.SetDefault(observability.NewLogger("app", os.Stdout))
}

// newHealth monta o monitor de readiness: PostgreSQL sempre; filas SQS
// somente as configuradas (desligado = sem requisito SQS). OIDC não tem
// check por request: é validado no startup (verifier construído ou boot
// falha) e o JWKS com cache é reutilizado nas validações.
func newHealth(
	pool *pgxpool.Pool,
	adapters *sqs.QueueAdapters,
	metrics *observability.Metrics,
) *observability.Health {
	checks := []observability.Check{
		observability.NewPingCheck("postgres", pool),
	}
	if adapters != nil {
		if adapters.Wager != nil {
			checks = append(checks, observability.NewQueueCheck("sqs-wager", adapters.Wager))
		}
		if adapters.Outbox != nil {
			checks = append(checks, observability.NewQueueCheck("sqs-events", adapters.Outbox))
		}
		if adapters.OutboxDLQ != nil {
			checks = append(checks, observability.NewQueueCheck("sqs-events-dlq", adapters.OutboxDLQ))
		}
	}
	h := observability.NewHealth(checks, 10*time.Second)
	h.SetMetrics(metrics)
	return h
}

// registerHealthLifecycle integra readiness ao lifecycle: OnStart popula o
// estado com checks reais e libera o anúncio; o loop mantém o cache
// atualizado; OnStop desliga o anúncio ANTES dos demais componentes
// (registrado por último no Module, executa primeiro no shutdown reverso do Fx).
// readiness = started && !stopping && checks ok.
func registerHealthLifecycle(lc fx.Lifecycle, h *observability.Health) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if err := h.CheckOnce(ctx); err != nil {
				slog.Warn("health initial check failed; starting not-ready",
					slog.String("component", "app"),
					slog.String("error", err.Error()))
			}
			h.MarkStarted()
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = h.Run(ctx)
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			h.MarkStopping()
			cancel()
			wg.Wait()
			return nil
		},
	})
}

// newMux monta as rotas com o comportamento já aprovado em B3.6/B3.7, mais
// os endpoints operacionais da B3.14. O contrato de wagering não muda; só
// entram /health/live, /health/ready e /metrics (sem OIDC, somente leitura).
func newMux(
	handler *wagerhttp.WagerHandler,
	walletHandler *wallethttp.WalletHandler,
	verifier *auth.Verifier,
	health *observability.Health,
	metrics *observability.Metrics,
) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("GET /health/live", observability.LiveHandler(health))
	mux.Handle("GET /health/ready", observability.ReadyHandler(health))
	mux.Handle("GET /metrics", observability.MetricsHandler(metrics))

	var wagering http.Handler = http.HandlerFunc(handler.ProcessWager)
	wagering = observability.MetricsMiddleware(metrics, wagering)
	if verifier == nil {
		slog.Warn("oidc disabled; wagering endpoint unauthenticated", slog.String("component", "app"))
	} else {
		wagering = wagerhttp.RequireProviderAuth(verifier, wagering)
	}
	wagering = observability.RequestIDMiddleware(wagering)
	mux.Handle("POST /wagering/transactions", wagering)

	// Wallet endpoints
	var walletCreate http.Handler = http.HandlerFunc(walletHandler.OpenWallet)
	walletCreate = observability.MetricsMiddleware(metrics, walletCreate)
	if verifier == nil {
		slog.Warn("oidc disabled; wallet creation endpoint unauthenticated", slog.String("component", "app"))
	} else {
		walletCreate = wallethttp.RequireInternalServiceAuth(verifier, walletCreate)
	}
	walletCreate = observability.RequestIDMiddleware(walletCreate)
	mux.Handle("POST /wallets", walletCreate)

	var walletRead http.Handler = http.HandlerFunc(walletHandler.GetWallet)
	walletRead = observability.MetricsMiddleware(metrics, walletRead)
	if verifier != nil {
		walletRead = wagerhttp.RequireProviderAuth(verifier, walletRead)
	}
	walletRead = observability.RequestIDMiddleware(walletRead)
	mux.Handle("GET /wallets/{walletId}", walletRead)

	var walletLedger http.Handler = http.HandlerFunc(walletHandler.GetWalletLedger)
	walletLedger = observability.MetricsMiddleware(metrics, walletLedger)
	if verifier != nil {
		walletLedger = wagerhttp.RequireProviderAuth(verifier, walletLedger)
	}
	walletLedger = observability.RequestIDMiddleware(walletLedger)
	mux.Handle("GET /wallets/{walletId}/ledger", walletLedger)

	var wagerTransactionRead http.Handler = http.HandlerFunc(walletHandler.GetTransaction)
	wagerTransactionRead = observability.MetricsMiddleware(metrics, wagerTransactionRead)
	if verifier != nil {
		wagerTransactionRead = wagerhttp.RequireProviderAuth(verifier, wagerTransactionRead)
	}
	wagerTransactionRead = observability.RequestIDMiddleware(wagerTransactionRead)
	mux.Handle("GET /wagering/transactions/{transactionId}", wagerTransactionRead)

	var providerTransactionRead http.Handler = http.HandlerFunc(walletHandler.GetTransactionByProviderExternalID)
	providerTransactionRead = observability.MetricsMiddleware(metrics, providerTransactionRead)
	if verifier != nil {
		providerTransactionRead = wagerhttp.RequireProviderAuth(verifier, providerTransactionRead)
	}
	providerTransactionRead = observability.RequestIDMiddleware(providerTransactionRead)
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", providerTransactionRead)

	// Reconciliação de carteira (apenas serviço interno)
	var walletReconcile http.Handler = http.HandlerFunc(walletHandler.ReconcileWallet)
	walletReconcile = observability.MetricsMiddleware(metrics, walletReconcile)
	if verifier == nil {
		slog.Warn("oidc disabled; wallet reconciliation endpoint unauthenticated", slog.String("component", "app"))
	} else {
		walletReconcile = wallethttp.RequireInternalServiceAuth(verifier, walletReconcile)
	}
	walletReconcile = observability.RequestIDMiddleware(walletReconcile)
	mux.Handle("POST /wallets/{walletId}/reconciliation", walletReconcile)

	return mux
}

// newListener vincula o endereço (suporta 127.0.0.1:0 em teste) e o libera
// no shutdown. O http.Server fecha o listener no Shutdown; o Close aqui é
// redundante e seguro (erro ignorado).
func newListener(lc fx.Lifecycle, cfg config.Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			_ = ln.Close()
			return nil
		},
	})
	return ln, nil
}

// newHTTPServer registra o servidor no lifecycle: OnStart serve no listener
// (goroutine com ownership do Fx, encerrada pelo Shutdown), OnStop faz
// graceful shutdown com timeout — conexões existentes têm oportunidade de
// terminar em vez de corte imediato. Todo request passa pelo middleware de
// correlation ID (ecoado em X-Request-ID, inclusive health/metrics/404).
func newHTTPServer(lc fx.Lifecycle, mux *http.ServeMux, ln net.Listener) *http.Server {
	srv := &http.Server{
		Handler:           observability.RequestIDMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				_ = srv.Serve(ln)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
			defer cancel()
			return srv.Shutdown(shutdownCtx)
		},
	})
	return srv
}

// newSQSAdapter monta os adapters SQS da aplicação (B3.10 + B3.12/B3.13):
// consumer (entrada), publisher (eventos) e DLQ compartilham UM único
// cliente AWS. Desligado por padrão (sem SQS_ENABLED=true): retorna vazio
// sem tocar em credencial AWS, e consumer/publisher nascem desabilitados.
func newSQSAdapter(cfg config.Config) (*sqs.QueueAdapters, error) {
	if !cfg.SQS.Enabled {
		return &sqs.QueueAdapters{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return sqs.NewQueueAdapters(ctx, cfg.SQS)
}

// newConsumer monta o consumer SQS (B3.10) a partir do pool, da Inbox, do
// WagerService, do adapter da FILA DE ENTRADA e da config centralizada. Sem
// adapter, retorna um consumer desabilitado e o lifecycle não inicia polling.
func newConsumer(
	pool *pgxpool.Pool,
	inbox *postgres.InboxRepository,
	service *application.WagerService,
	adapters *sqs.QueueAdapters,
	cfg config.Config,
	metrics *observability.Metrics,
) (*sqs.Consumer, error) {
	sqsCfg := cfg.SQS
	// Atenção ao nil: *SQSAdapter nil dentro de interface não é == nil.
	var receiver sqs.Receiver
	if adapters != nil && adapters.Wager != nil {
		receiver = adapters.Wager
	}
	if receiver == nil {
		return sqs.NewConsumer(
			pool, inbox, service, nil,
			sqsCfg.ConsumerName,
			sqsCfg.MaxMessages, sqsCfg.WaitTimeSeconds, sqsCfg.VisibilityTimeoutSecond,
			false,
			metrics,
		)
	}
	return sqs.NewConsumer(
		pool, inbox, service, receiver,
		sqsCfg.ConsumerName,
		sqsCfg.MaxMessages, sqsCfg.WaitTimeSeconds, sqsCfg.VisibilityTimeoutSecond,
		true,
		metrics,
	)
}

// registerConsumerLifecycle inicia o polling sem bloquear o startup do Fx e
// o encerra de forma limpa no shutdown (cancel + wait, sem goroutine
// vazando). Desabilitado, não lança goroutine. Não cria conexões próprias:
// reusa pool, Inbox e service já existentes no grafo.
func registerConsumerLifecycle(lc fx.Lifecycle, c *sqs.Consumer) {
	if !c.Enabled() {
		slog.Info("sqs consumer disabled (set SQS_ENABLED=true and SQS_WAGER_QUEUE_URL to enable)",
			slog.String("component", "app"))
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = c.Run(ctx)
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			wg.Wait()
			return nil
		},
	})
}

// newPublisher monta o publisher da outbox (B3.12/B3.13) a partir do pool,
// do OutboxRepository, dos adapters de eventos e DLQ e da config
// centralizada. O sender publica SOMENTE na fila de eventos; a DLQ (quando
// configurada) recebe SOMENTE corpos esgotados. Sem adapter de eventos,
// nasce desabilitado e o lifecycle não inicia polling. Sem goroutine própria
// aqui: o loop vive no Run do publisher.
func newPublisher(
	pool *pgxpool.Pool,
	outbox *postgres.OutboxRepository,
	adapters *sqs.QueueAdapters,
	cfg config.Config,
	metrics *observability.Metrics,
) (*sqs.Publisher, error) {
	sqsCfg := cfg.SQS
	interval := time.Duration(sqsCfg.OutboxPollIntervalSeconds) * time.Second
	maxBackoff := time.Duration(sqsCfg.OutboxMaxBackoffSeconds) * time.Second
	// Atenção ao nil: *SQSAdapter nil dentro de interface não é == nil.
	var sender, dlq sqs.Sender
	if adapters != nil {
		if adapters.Outbox != nil {
			sender = adapters.Outbox
		}
		if adapters.OutboxDLQ != nil {
			dlq = adapters.OutboxDLQ
		}
	}
	if sender == nil {
		return sqs.NewPublisher(pool, outbox, nil, nil, int(sqsCfg.OutboxBatchSize), interval, sqsCfg.OutboxMaxAttempts, maxBackoff, false, metrics)
	}
	return sqs.NewPublisher(pool, outbox, sender, dlq, int(sqsCfg.OutboxBatchSize), interval, sqsCfg.OutboxMaxAttempts, maxBackoff, true, metrics)
}

// registerPublisherLifecycle inicia o polling da outbox sem bloquear o
// startup do Fx e o encerra de forma limpa no shutdown (cancel + wait, sem
// goroutine vazando). Desabilitado, não lança goroutine.
func registerPublisherLifecycle(lc fx.Lifecycle, p *sqs.Publisher) {
	if !p.Enabled() {
		slog.Info("sqs outbox publisher disabled (set SQS_ENABLED=true and SQS_OUTBOX_QUEUE_URL to enable)",
			slog.String("component", "app"))
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = p.Run(ctx)
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			wg.Wait()
			return nil
		},
	})
}
