// Package config centraliza a configuração via ambiente. É a única origem
// de env para a composição Fx (sem leitura espalhada).
package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joaoricardofp/backend-challenge-go/internal/auth"
)

// Config agrega database, HTTP, OIDC, SQS e resolvedor de referências com
// os defaults locais já existentes.
type Config struct {
	DatabaseURL     string
	HTTPAddr        string
	OIDC            auth.Config
	SQS             SQSConfig
	PendingResolver PendingResolverConfig
}

// PendingResolverConfig concentra a configuração do worker de
// PENDING_REFERENCE (README §7). É a única origem de env do resolvedor.
// Habilitado por padrão: o polling é uma consulta indexada barata e o
// worker é exigido para concluir reversões com referência tardia.
type PendingResolverConfig struct {
	Enabled          bool
	IntervalSeconds  int32
	BatchSize        int32
	MaxAttempts      int32
	MaxBackoffSecond int32
}

// PendingResolverEnabled retorna se o worker deve iniciar.
func (c PendingResolverConfig) PendingResolverEnabled() bool {
	return c.Enabled
}

// SQSConfig concentra a configuração SQS (consumer B3.10 + publisher
// B3.12/B3.13). É a única origem de env do SQS: nenhum os.Getenv espalhado
// no polling/ack/publicação. SQS_ENABLED é o gate geral; cada componente
// exige ainda a sua fila: o consumer de wagering consome SOMENTE
// WagerQueueURL e o publisher publica SOMENTE em OutboxQueueURL. Sem a fila
// correspondente, o componente nasce desligado mesmo com a flag.
type SQSConfig struct {
	Enabled                 bool
	WagerQueueURL           string
	OutboxQueueURL          string
	OutboxDLQURL            string
	ConsumerName            string
	Region                  string
	Endpoint                string
	MaxMessages             int32
	WaitTimeSeconds         int32
	VisibilityTimeoutSecond int32
	// OutboxPollIntervalSeconds é o intervalo fixo do polling do publisher.
	// OutboxBatchSize limita as linhas elegíveis buscadas por ciclo.
	// OutboxMaxAttempts é o número de envios (fila de eventos) após o qual
	// o evento é encaminhado à DLQ. OutboxMaxBackoffSeconds é o teto do
	// backoff exponencial entre tentativas.
	OutboxPollIntervalSeconds int32
	OutboxBatchSize           int32
	OutboxMaxAttempts         int32
	OutboxMaxBackoffSeconds   int32
}

// ConsumerEnabled retorna se o consumer de wagering deve iniciar: gate geral
// mais fila de entrada presente.
func (c SQSConfig) ConsumerEnabled() bool {
	return c.Enabled && strings.TrimSpace(c.WagerQueueURL) != ""
}

// PublisherEnabled retorna se o publisher da outbox deve iniciar: gate geral
// mais fila de eventos presente.
func (c SQSConfig) PublisherEnabled() bool {
	return c.Enabled && strings.TrimSpace(c.OutboxQueueURL) != ""
}

// Load lê o ambiente com os defaults locais preservados.
func Load() Config {
	db := os.Getenv("DATABASE_URL")
	if db == "" {
		db = "postgres://postgres:postgres@localhost:5432/backend-challenge-go"
	}
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	return Config{
		DatabaseURL:     db,
		HTTPAddr:        addr,
		OIDC:            auth.ConfigFromEnv(),
		SQS:             SQSConfigFromEnv(),
		PendingResolver: PendingResolverConfigFromEnv(),
	}
}

// PendingResolverConfigFromEnv lê o ambiente do worker de PENDING_REFERENCE.
// Defaults: habilitado, varredura de 5s, 10 linhas por ciclo, 10 tentativas,
// teto de backoff de 5 minutos. Valores fora do intervalo são limitados em
// vez de falhar o boot.
func PendingResolverConfigFromEnv() PendingResolverConfig {
	enabled := true
	if v := strings.TrimSpace(os.Getenv("PENDING_RESOLVER_ENABLED")); v != "" {
		enabled = v == "true" || v == "1"
	}
	return PendingResolverConfig{
		Enabled:          enabled,
		IntervalSeconds:  clampInt32(envInt32("PENDING_RESOLVER_INTERVAL_SECONDS", 5), 1, 3600),
		BatchSize:        clampInt32(envInt32("PENDING_RESOLVER_BATCH_SIZE", 10), 1, 100),
		MaxAttempts:      clampInt32(envInt32("PENDING_RESOLVER_MAX_ATTEMPTS", 10), 1, 1000),
		MaxBackoffSecond: clampInt32(envInt32("PENDING_RESOLVER_MAX_BACKOFF_SECONDS", 300), 1, 3600),
	}
}

// SQSConfigFromEnv lê o ambiente do SQS (consumer B3.10 + publisher
// B3.12/B3.13). Defaults: desligado, consumer "wager-consumer" (mesma
// identidade da Inbox), long polling de 20s, visibility de 30s, batch de 10
// (máximo do SQS), polling da outbox de 1s com 10 linhas por ciclo, 5
// tentativas de envio com teto de backoff de 5 minutos. Valores fora do
// intervalo são limitados em vez de falhar o boot.
func SQSConfigFromEnv() SQSConfig {
	enabled := false
	if v := strings.TrimSpace(os.Getenv("SQS_ENABLED")); v != "" {
		enabled = v == "true" || v == "1"
	}
	consumer := strings.TrimSpace(os.Getenv("SQS_CONSUMER_NAME"))
	if consumer == "" {
		consumer = "wager-consumer"
	}
	region := strings.TrimSpace(os.Getenv("SQS_REGION"))
	if region == "" {
		region = strings.TrimSpace(os.Getenv("AWS_REGION"))
	}
	if region == "" {
		region = "us-east-1"
	}
	return SQSConfig{
		Enabled:                 enabled,
		WagerQueueURL:           strings.TrimSpace(os.Getenv("SQS_WAGER_QUEUE_URL")),
		OutboxQueueURL:          strings.TrimSpace(os.Getenv("SQS_OUTBOX_QUEUE_URL")),
		OutboxDLQURL:            strings.TrimSpace(os.Getenv("SQS_OUTBOX_DLQ_URL")),
		ConsumerName:            consumer,
		Region:                  region,
		Endpoint:                strings.TrimSpace(os.Getenv("SQS_ENDPOINT")),
		MaxMessages:             clampInt32(envInt32("SQS_MAX_MESSAGES", 10), 1, 10),
		WaitTimeSeconds:         clampInt32(envInt32("SQS_WAIT_TIME_SECONDS", 20), 0, 20),
		VisibilityTimeoutSecond: clampInt32(envInt32("SQS_VISIBILITY_TIMEOUT_SECONDS", 30), 0, 43200),
		// Polling simples do publisher: 1s fixo, 10 linhas por ciclo.
		OutboxPollIntervalSeconds: clampInt32(envInt32("SQS_OUTBOX_POLL_INTERVAL_SECONDS", 1), 1, 60),
		OutboxBatchSize:           clampInt32(envInt32("SQS_OUTBOX_BATCH_SIZE", 10), 1, 100),
		OutboxMaxAttempts:         clampInt32(envInt32("SQS_OUTBOX_MAX_ATTEMPTS", 5), 1, 100),
		OutboxMaxBackoffSeconds:   clampInt32(envInt32("SQS_OUTBOX_MAX_BACKOFF_SECONDS", 300), 1, 3600),
	}
}

func envInt32(key string, def int32) int32 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return int32(n)
}

func clampInt32(v, min, max int32) int32 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
